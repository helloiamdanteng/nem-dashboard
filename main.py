"""
NEM Dashboard - FastAPI backend
Fast cache: prices, demand, gen, IC, Origin — refreshed every 5 min
Slow cache: all generators, ST PASA week-ahead — refreshed every 30 min
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

from scraper import scrape_all, scrape_slow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fast_cache = {"data": None, "last_updated": None, "error": None}
slow_cache = {"data": None, "last_updated": None, "error": None}

FAST_INTERVAL = 300   # 5 min
SLOW_INTERVAL = 1800  # 30 min


async def _run_fast():
    t0 = time.time()
    data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
    fast_cache["data"] = data
    fast_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    fast_cache["error"] = None
    logger.info(f"Fast scrape done in {time.time()-t0:.1f}s")


async def _run_slow():
    t0 = time.time()
    data = await asyncio.get_event_loop().run_in_executor(None, scrape_slow)
    slow_cache["data"] = data
    slow_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    slow_cache["error"] = None
    logger.info(f"Slow scrape done in {time.time()-t0:.1f}s")


async def fast_loop():
    while True:
        try:
            await _run_fast()
        except Exception as e:
            logger.error(f"Fast scrape error: {e}\n{traceback.format_exc()}")
            fast_cache["error"] = str(e)
        await asyncio.sleep(FAST_INTERVAL)


async def slow_loop():
    while True:
        try:
            await _run_slow()
        except Exception as e:
            logger.error(f"Slow scrape error: {e}\n{traceback.format_exc()}")
            slow_cache["error"] = str(e)
        await asyncio.sleep(SLOW_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fast scrape first — gets prices/demand up immediately
    try:
        await _run_fast()
    except Exception as e:
        logger.error(f"Initial fast scrape failed: {e}\n{traceback.format_exc()}")
        fast_cache["error"] = str(e)

    # Slow scrape kicks off in background — doesn't block startup
    asyncio.create_task(_run_slow())

    fast_task = asyncio.create_task(fast_loop())
    slow_task = asyncio.create_task(slow_loop())
    yield
    fast_task.cancel()
    slow_task.cancel()


app = FastAPI(title="NEM Dashboard", lifespan=lifespan)

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/api/data")
async def get_data():
    if fast_cache["data"] is None:
        return JSONResponse(
            content={"error": fast_cache.get("error", "Data not yet available"), "loading": True},
            status_code=503 if fast_cache["error"] else 202,
        )
    return JSONResponse(content={
        **fast_cache["data"],
        "last_updated": fast_cache["last_updated"],
        "cache_error":  fast_cache.get("error"),
    })


@app.get("/api/slow")
async def get_slow():
    if slow_cache["data"] is None:
        return JSONResponse(
            content={"loading": True, "error": slow_cache.get("error")},
            status_code=202,
        )
    return JSONResponse(content={
        **slow_cache["data"],
        "last_updated": slow_cache["last_updated"],
        "cache_error":  slow_cache.get("error"),
    })


@app.get("/api/health")
async def health():
    return {
        "status":           "ok" if fast_cache["data"] else "loading",
        "fast_updated":     fast_cache["last_updated"],
        "slow_updated":     slow_cache["last_updated"],
        "fast_has_data":    fast_cache["data"] is not None,
        "slow_has_data":    slow_cache["data"] is not None,
        "fast_error":       fast_cache.get("error"),
        "slow_error":       slow_cache.get("error"),
    }


@app.get("/api/debug")
async def debug():
    import csv, io
    from scraper import _list_hrefs, _read_zip, _parse_aemo, DISPATCH_IS_URL, PREDISPATCH_URL, TRADING_ARCHIVE, ST_PASA_URL
    from zoneinfo import ZoneInfo

    result = {}
    aest = ZoneInfo("Australia/Sydney")
    now = datetime.now(aest)
    today_str = now.strftime("%Y%m%d")
    result["now_aest"] = now.isoformat()

    # Trading archive timing
    try:
        t0 = time.time()
        all_zips = _list_hrefs(TRADING_ARCHIVE)
        today_zips = [u for u in all_zips if today_str in u]
        result["trading_total_zips"] = len(all_zips)
        result["trading_today_zips"] = len(today_zips)
        result["trading_list_ms"] = round((time.time()-t0)*1000)
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
                    tables[key] = True
            result["trading_tables"] = list(tables.keys())
    except Exception:
        result["trading_error"] = traceback.format_exc()

    # ST PASA listing
    try:
        t0 = time.time()
        pasa_urls = _list_hrefs(ST_PASA_URL)
        result["stpasa_total"] = len(pasa_urls)
        result["stpasa_list_ms"] = round((time.time()-t0)*1000)
        result["stpasa_sample"] = pasa_urls[-2:] if pasa_urls else []
    except Exception:
        result["stpasa_error"] = traceback.format_exc()

    # Cache summary
    if fast_cache["data"]:
        d = fast_cache["data"]
        result["fast_cache"] = {
            "prices":        d.get("prices", {}),
            "hist_prices":   {r: len(v) for r, v in d.get("historical_prices", {}).items()},
            "pd_prices":     {r: len(v) for r, v in d.get("predispatch_prices", {}).items()},
            "demand_hist":   {r: len(v) for r, v in d.get("demand_history", {}).items()},
            "dispatch_hist": {r: len(v) for r, v in d.get("dispatch_history", {}).items()},
            "fuel_mix":      {r: len(v) for r, v in d.get("fuel_mix_history", {}).items()},
            "ic_history":    {k: len(v) for k, v in d.get("ic_history", {}).items()},
        }
    if slow_cache["data"]:
        d = slow_cache["data"]
        gens = d.get("generators", {}).get("grouped", {})
        result["slow_cache"] = {
            "gen_regions":   list(gens.keys()),
            "gen_units":     sum(len(u) for r in gens.values() for u in r.values()),
            "stpasa_pts":    {r: len(v) for r, v in d.get("stpasa_demand", {}).items()},
            "fuel_mix_pts":  {r: len(v) for r, v in d.get("fuel_mix_today", {}).items()},
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
