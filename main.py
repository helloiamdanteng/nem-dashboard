"""
NEM Dashboard - FastAPI backend
Serves scraped NEMWeb data and the mobile-friendly dashboard frontend.
"""

import asyncio
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from scraper import scrape_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory cache
cache = {
    "data": None,
    "last_updated": None,
    "error": None,
}

REFRESH_INTERVAL = 300  # 5 minutes


async def refresh_data():
    """Background task to periodically refresh NEMWeb data."""
    while True:
        try:
            logger.info("Refreshing NEMWeb data...")
            data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
            cache["data"] = data
            cache["last_updated"] = datetime.now(timezone.utc).isoformat()
            cache["error"] = None
            logger.info("Data refresh complete.")
        except Exception as e:
            logger.error(f"Error refreshing data: {e}")
            cache["error"] = str(e)
        await asyncio.sleep(REFRESH_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initial fetch
    try:
        logger.info("Initial NEMWeb data fetch...")
        data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
        cache["data"] = data
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.error(f"Initial fetch failed: {e}")
        cache["error"] = str(e)

    # Start background refresh
    task = asyncio.create_task(refresh_data())
    yield
    task.cancel()


app = FastAPI(title="NEM Dashboard", lifespan=lifespan)

# Serve static files
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/api/data")
async def get_data():
    """Return latest NEMWeb data."""
    if cache["data"] is None:
        return JSONResponse(
            content={"error": cache.get("error", "Data not yet available"), "loading": True},
            status_code=503 if cache["error"] else 202,
        )
    return JSONResponse(content={
        **cache["data"],
        "last_updated": cache["last_updated"],
        "cache_error": cache.get("error"),
    })


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "last_updated": cache["last_updated"],
        "has_data": cache["data"] is not None,
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard HTML."""
    html_path = Path(__file__).parent / "static" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>Dashboard loading...</h1>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
