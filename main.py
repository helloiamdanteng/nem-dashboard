"""
NEM Dashboard - FastAPI backend
"""

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from scraper import scrape_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

cache = {"data": None, "last_updated": None, "error": None}
REFRESH_INTERVAL = 300


async def refresh_data():
    while True:
        try:
            data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
            cache["data"] = data
            cache["last_updated"] = datetime.now(timezone.utc).isoformat()
            cache["error"] = None
        except Exception as e:
            logger.error(f"Refresh error: {e}")
            cache["error"] = str(e)
        await asyncio.sleep(REFRESH_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
        cache["data"] = data
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.error(f"Initial fetch failed: {e}")
        cache["error"] = str(e)
    task = asyncio.create_task(refresh_data())
    yield
    task.cancel()


app = FastAPI(title="NEM Dashboard", lifespan=lifespan)

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/api/data")
async def get_data():
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
    return {"status": "ok", "last_updated": cache["last_updated"], "has_data": cache["data"] is not None}


@app.get("/api/debug")
async def debug():
    from scraper import (get_all_file_urls, _list_hrefs, _read_zip_csv, _parse_aemo,
                         DISPATCH_IS_URL, PREDISPATCH_URL, TRADING_ARCHIVE_BASE)
    from zoneinfo import ZoneInfo

    result = {}
    aest = ZoneInfo("Australia/Sydney")
    now = datetime.now(aest)
    today_str = now.strftime("%Y%m%d")
    result["now_aest"] = now.isoformat()
    result["today_str"] = today_str

    # Check trading archive
    try:
        archive_url = f"{TRADING_ARCHIVE_BASE}/"
        all_zips = _list_hrefs(archive_url)
        today_zips = [u for u in all_zips if today_str in u]
        result["trading_archive_total"] = len(all_zips)
        result["trading_archive_today"] = len(today_zips)
        result["trading_archive_sample"] = all_zips[-3:] if all_zips else []

        # Peek at latest trading file
        test_url = today_zips[-1] if today_zips else (all_zips[-1] if all_zips else None)
        if test_url:
            result["trading_test_url"] = test_url
            text = _read_zip_csv(test_url)
            # Find all table keys
            import csv, io, re
            tables = {}
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row and row[0].strip().upper() == "I" and len(row) >= 5:
                    key = f"{row[1].strip()}_{row[2].strip()}".upper()
                    headers = [c.strip().upper() for c in row[4:] if c.strip()]
                    tables[key] = headers
            result["trading_tables"] = tables

            # Sample TRADING_PRICE rows
            sample = _parse_aemo(text, "TRADING_PRICE")
            result["trading_price_rows"] = len(sample)
            result["trading_price_sample"] = sample[:2] if sample else []
    except Exception:
        result["trading_error"] = traceback.format_exc()

    # Check predispatch
    try:
        pd_urls = _list_hrefs(PREDISPATCH_URL)
        result["predispatch_total"] = len(pd_urls)
        if pd_urls:
            result["predispatch_latest"] = pd_urls[-1]
            text = _read_zip_csv(pd_urls[-1])
            import csv, io
            tables = {}
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row and row[0].strip().upper() == "I" and len(row) >= 5:
                    key = f"{row[1].strip()}_{row[2].strip()}".upper()
                    headers = [c.strip().upper() for c in row[4:] if c.strip()]
                    tables[key] = headers
            result["predispatch_tables"] = tables
            for tk in ["PREDISPATCH_REGION_PRICES", "PREDISPATCH_PRICE", "PREDISPATCH_REGIONPRICE", "PREDISPATCH_REGION_SOLUTION"]:
                rows = _parse_aemo(text, tk)
                result[f"pd_{tk}_count"] = len(rows)
                if rows:
                    result[f"pd_{tk}_sample"] = rows[0]
    except Exception:
        result["predispatch_error"] = traceback.format_exc()

    # Cache summary
    if cache["data"]:
        d = cache["data"]
        result["cache"] = {
            "prices": d.get("prices", {}),
            "hist_price":  {r: len(v) for r, v in d.get("historical_prices", {}).items()},
            "pd_price":    {r: len(v) for r, v in d.get("predispatch_prices", {}).items()},
            "demand_hist": {r: len(v) for r, v in d.get("demand_history", {}).items()},
            "pd_demand":   {r: len(v) for r, v in d.get("predispatch_demand", {}).items()},
        }

    return JSONResponse(content=result)


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = Path(__file__).parent / "static" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>Loading...</h1>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
