"""
NEM Dashboard - FastAPI backend
"""

import asyncio
import logging
import time
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
            t0 = time.time()
            data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
            cache["data"] = data
            cache["last_updated"] = datetime.now(timezone.utc).isoformat()
            cache["error"] = None
            logger.info(f"Scrape completed in {time.time()-t0:.1f}s")
        except Exception as e:
            logger.error(f"Refresh error: {e}\n{traceback.format_exc()}")
            cache["error"] = str(e)
        await asyncio.sleep(REFRESH_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        t0 = time.time()
        data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
        cache["data"] = data
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Initial scrape completed in {time.time()-t0:.1f}s")
    except Exception as e:
        logger.error(f"Initial fetch failed: {e}\n{traceback.format_exc()}")
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
    return {
        "status": "ok" if cache["data"] else "loading",
        "last_updated": cache["last_updated"],
        "has_data": cache["data"] is not None,
        "error": cache.get("error"),
    }


@app.get("/api/debug")
async def debug():
    import csv, io
    from scraper import (
        _list_hrefs, _read_zip, _parse_aemo,
        DISPATCH_IS_URL, PREDISPATCH_URL, TRADING_ARCHIVE
    )
    from zoneinfo import ZoneInfo

    result = {}
    aest = ZoneInfo("Australia/Sydney")
    now = datetime.now(aest)
    today_str = now.strftime("%Y%m%d")
    result["now_aest"] = now.isoformat()

    # Trading archive
    try:
        t0 = time.time()
        all_zips = _list_hrefs(TRADING_ARCHIVE)
        today_zips = [u for u in all_zips if today_str in u]
        result["trading_total_zips"] = len(all_zips)
        result["trading_today_zips"] = len(today_zips)
        result["trading_list_ms"] = round((time.time()-t0)*1000)
        result["trading_sample"] = all_zips[-3:] if all_zips else []

        test_url = today_zips[-1] if today_zips else (all_zips[-1] if all_zips else None)
        if test_url:
            t1 = time.time()
            text = _read_zip(test_url)
            result["trading_fetch_ms"] = round((time.time()-t1)*1000)
            tables = {}
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row and row[0].strip().upper() == "I" and len(row) >= 5:
                    key = f"{row[1].strip()}_{row[2].strip()}".upper()
                    tables[key] = [c.strip().upper() for c in row[4:] if c.strip()]
            result["trading_tables"] = list(tables.keys())
            price_rows = _parse_aemo(text, "TRADING_PRICE")
            demand_rows = _parse_aemo(text, "TRADING_REGIONSUM")
            result["trading_price_rows"] = len(price_rows)
            result["trading_demand_rows"] = len(demand_rows)
            result["trading_price_sample"] = price_rows[:1] if price_rows else []
            result["trading_demand_sample"] = demand_rows[:1] if demand_rows else []
    except Exception:
        result["trading_error"] = traceback.format_exc()

    # Predispatch
    try:
        t0 = time.time()
        pd_urls = _list_hrefs(PREDISPATCH_URL)
        result["predispatch_total"] = len(pd_urls)
        result["predispatch_list_ms"] = round((time.time()-t0)*1000)
        if pd_urls:
            t1 = time.time()
            text = _read_zip(pd_urls[-1])
            result["predispatch_fetch_ms"] = round((time.time()-t1)*1000)
            tables = {}
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row and row[0].strip().upper() == "I" and len(row) >= 5:
                    key = f"{row[1].strip()}_{row[2].strip()}".upper()
                    tables[key] = True
            result["predispatch_tables"] = list(tables.keys())
    except Exception:
        result["predispatch_error"] = traceback.format_exc()

    # Cache summary
    if cache["data"]:
        d = cache["data"]
        result["cache"] = {
            "prices": d.get("prices", {}),
            "hist_prices":   {r: len(v) for r, v in d.get("historical_prices", {}).items()},
            "pd_prices":     {r: len(v) for r, v in d.get("predispatch_prices", {}).items()},
            "demand_hist":   {r: len(v) for r, v in d.get("demand_history", {}).items()},
            "dispatch_hist": {r: len(v) for r, v in d.get("dispatch_history", {}).items()},
            "pd_demand":     {r: len(v) for r, v in d.get("predispatch_demand", {}).items()},
            "fuel_mix":      {r: len(v) for r, v in d.get("fuel_mix_history", {}).items()},
            "ic_history":    {k: len(v) for k, v in d.get("ic_history", {}).items()},
            "origin_found":  sum(1 for v in d.get("origin_assets", {}).values() if v.get("mw") is not None),
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
