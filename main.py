"""
NEM Dashboard - FastAPI backend
Fast cache: prices, demand, gen, IC, Origin — refreshed every 5 min
Slow cache: all generators, ST PASA week-ahead — refreshed every 30 min
"""

import asyncio
import io
import logging
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from scraper import scrape_all, scrape_gen, scrape_slow, scrape_scada_history

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fast_cache = {"data": None, "last_updated": None, "error": None}
gen_cache  = {"data": None, "last_updated": None, "error": None}
slow_cache = {"data": None, "last_updated": None, "error": None}

FAST_INTERVAL = 300    # 5 min
GEN_INTERVAL  = 900    # 15 min
SLOW_INTERVAL = 1800   # 30 min


_FAST_EMPTY = {
    "prices": {}, "demand": {}, "op_demand": {}, "region_summary": {},
    "historical_prices": {}, "predispatch_prices": {}, "predispatch_demand": {},
    "predispatch_gen": {}, "predispatch_ic": {}, "ic_flows": {}, "ic_history": {},
    "origin_assets": {}, "tomorrow_prices": {}, "tomorrow_demand": {},
    "dispatch_history": {}, "fuel_mix": {}, "predispatch_units": {},
    "dispatch_prices_5min": {}, "demand_history": {}, "op_demand_history": {},
    "interconnectors": {}, "generation": {}, "fuel_colors": {}, "all_fuels": [],
    "bdu_history": {},
}

async def _run_fast():
    t0 = time.time()
    try:
        data = await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(None, scrape_all),
            timeout=60,
        )
        fast_cache["data"] = data
        fast_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        fast_cache["error"] = None
        logger.info(f"Fast scrape done in {time.time()-t0:.1f}s")
    except asyncio.TimeoutError:
        logger.error("Fast scrape timed out after 60s")
        fast_cache["error"] = "timeout"
        if fast_cache["data"] is None:
            fast_cache["data"] = dict(_FAST_EMPTY)
            fast_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.error(f"Fast scrape error: {e}\n{traceback.format_exc()}")
        fast_cache["error"] = str(e)
        if fast_cache["data"] is None:
            fast_cache["data"] = dict(_FAST_EMPTY)
            fast_cache["last_updated"] = datetime.now(timezone.utc).isoformat()


async def _run_slow():
    t0 = time.time()
    try:
        loop = asyncio.get_running_loop()
        data = await asyncio.wait_for(
            loop.run_in_executor(None, scrape_slow),
            timeout=120  # hard ceiling — if AEMO XLS hangs, don't block forever
        )
        slow_cache["data"] = data
        slow_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        slow_cache["error"] = None
        logger.info(f"Slow scrape done in {time.time()-t0:.1f}s")
    except asyncio.TimeoutError:
        logger.error("Slow scrape timed out after 120s")
        slow_cache["error"] = "timeout"
        # Populate with whatever partial data we have rather than staying empty
        if slow_cache["data"] is None:
            slow_cache["data"] = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "generators": {"grouped": {}, "summary": {}, "fuel_colors": {}, "reg_list_count": 0, "scada_count": 0},
                "stpasa_demand": {},
                "fuel_mix_today": {},
                "fuel_colors": {},
                "all_fuels": [],
            }
            slow_cache["last_updated"] = datetime.now(timezone.utc).isoformat()


async def fast_loop():
    while True:
        try:
            await _run_fast()
        except Exception as e:
            logger.error(f"Fast scrape error: {e}\n{traceback.format_exc()}")
            fast_cache["error"] = str(e)
        await asyncio.sleep(FAST_INTERVAL)


async def _run_gen():
    t0 = time.time()
    try:
        loop = asyncio.get_running_loop()
        data = await asyncio.wait_for(loop.run_in_executor(None, scrape_gen), timeout=60)
        gen_cache["data"] = data
        gen_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        gen_cache["error"] = None
        logger.info(f"Gen scrape done in {time.time()-t0:.1f}s — "
                    f"scada={data.get('scada_count',0)} reg={data.get('reg_count',0)}")
    except asyncio.TimeoutError:
        logger.error("Gen scrape timed out")
        gen_cache["error"] = "timeout"
        if gen_cache["data"] is None:
            gen_cache["data"] = {"fuel_mix": {}, "nem_totals": {}, "grouped": {},
                                 "fuel_colors": {}, "all_fuels": [], "scada_count": 0, "reg_count": 0}
    except Exception as e:
        logger.error(f"Gen scrape error: {e}")
        gen_cache["error"] = str(e)


async def gen_loop():
    await asyncio.sleep(5)   # let fast scrape finish first
    # Backfill 24hr SCADA history once at startup so chart is immediately populated
    try:
        loop = asyncio.get_running_loop()
        logger.info("Starting SCADA history backfill…")
        await asyncio.wait_for(
            loop.run_in_executor(None, scrape_scada_history), timeout=120
        )
        logger.info("SCADA history backfill complete")
    except Exception as e:
        logger.warning(f"SCADA history backfill failed: {e}")
    while True:
        try:
            await _run_gen()
        except Exception as e:
            logger.error(f"Gen loop error: {e}")
            gen_cache["error"] = str(e)
        await asyncio.sleep(GEN_INTERVAL)


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
    # Fast scrape first — prices/demand up immediately
    # _run_fast() now handles its own exceptions and always sets cache
    await _run_fast()
    if fast_cache["data"] is None:
        fast_cache["data"] = dict(_FAST_EMPTY)
        fast_cache["last_updated"] = datetime.now(timezone.utc).isoformat()

    # Gen and slow kick off in background
    asyncio.create_task(_run_slow())

    fast_task = asyncio.create_task(fast_loop())
    gen_task  = asyncio.create_task(gen_loop())
    slow_task = asyncio.create_task(slow_loop())
    yield
    fast_task.cancel()
    gen_task.cancel()
    slow_task.cancel()


app = FastAPI(title="NEM Dashboard", lifespan=lifespan)

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
(static_dir / "data").mkdir(exist_ok=True)  # ensure views.json directory exists
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


@app.get("/api/gen")
async def get_gen():
    if gen_cache["data"] is None:
        return JSONResponse(
            content={"loading": True, "error": gen_cache.get("error")},
            status_code=202,
        )
    return JSONResponse(content={
        **gen_cache["data"],
        "last_updated": gen_cache["last_updated"],
        "cache_error":  gen_cache.get("error"),
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
    from scraper import _list_hrefs, _read_zip, _parse_aemo, DISPATCH_IS_URL, PREDISPATCH_URL, TRADING_CURRENT, ST_PASA_URL
    from zoneinfo import ZoneInfo

    result = {}
    aest = ZoneInfo("Australia/Brisbane")  # UTC+10 fixed, matches AEMO
    now = datetime.now(aest)
    today_str = now.strftime("%Y%m%d")
    result["now_aest"] = now.isoformat()

    # Direct test of scrape_trading_history
    try:
        from scraper import scrape_trading_history
        th = scrape_trading_history()
        result["trading_history_test"] = {
            "price_regions": list(th["prices"].keys()),
            "price_counts": {r: len(v) for r, v in th["prices"].items()},
            "price_sample": {r: v[:2] for r, v in th["prices"].items()},
            "fetch_stats": th.get("fetch_stats", {}),
        }
    except Exception:
        result["trading_history_error"] = traceback.format_exc()

    # Trading archive timing
    try:
        t0 = time.time()
        all_zips = _list_hrefs(TRADING_CURRENT)
        today_zips = [u for u in all_zips if today_str in u]
        result["trading_total_zips"] = len(all_zips)
        result["trading_today_zips"] = len(today_zips)
        result["trading_list_ms"] = round((time.time()-t0)*1000)
        test_url = today_zips[-1] if today_zips else (all_zips[-1] if all_zips else None)
        if test_url:
            result["trading_test_url"] = test_url
            t1 = time.time()
            text = _read_zip(test_url)
            result["trading_fetch_ms"] = round((time.time()-t1)*1000)
            # Show all table names AND their columns
            tables = {}
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row and row[0].strip().upper() == "I" and len(row) >= 5:
                    key = f"{row[1].strip()}_{row[2].strip()}".upper()
                    tables[key] = [c.strip().upper() for c in row[4:] if c.strip()]
            result["trading_tables_with_cols"] = tables
            # Sample actual rows
            for tbl in ["TRADING_PRICE", "TRADING_REGIONSUM"]:
                rows = _parse_aemo(text, tbl)
                result[f"{tbl}_count"] = len(rows)
                result[f"{tbl}_sample"] = rows[:2] if rows else []
    except Exception:
        result["trading_error"] = traceback.format_exc()

    # Also check DispatchIS (the live file)
    try:
        dispatch_urls = _list_hrefs(DISPATCH_IS_URL)
        if dispatch_urls:
            result["dispatch_latest_url"] = dispatch_urls[-1]
            t1 = time.time()
            text = _read_zip(dispatch_urls[-1])
            result["dispatch_fetch_ms"] = round((time.time()-t1)*1000)
            tables = {}
            reader = csv.reader(io.StringIO(text))
            for row in reader:
                if row and row[0].strip().upper() == "I" and len(row) >= 5:
                    key = f"{row[1].strip()}_{row[2].strip()}".upper()
                    tables[key] = [c.strip().upper() for c in row[4:] if c.strip()]
            result["dispatch_tables_with_cols"] = tables
            for tbl in ["DISPATCH_PRICE", "DISPATCH_REGIONSUM"]:
                rows = _parse_aemo(text, tbl)
                result[f"{tbl}_count"] = len(rows)
                result[f"{tbl}_sample"] = rows[:2] if rows else []
    except Exception:
        result["dispatch_error"] = traceback.format_exc()

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
            "demand":        d.get("demand", {}),
            "hist_prices":   {r: len(v) for r, v in d.get("historical_prices", {}).items()},
            "hist_prices_sample": {r: v[:2] for r, v in d.get("historical_prices", {}).items()},
            "hist_fetch_stats": d.get("price_fetch_stats", {}),
            "pd_prices":     {r: len(v) for r, v in d.get("predispatch_prices", {}).items()},
            "dispatch_hist": {r: len(v) for r, v in d.get("dispatch_history", {}).items()},
            "dispatch_hist_sample": {r: v[:2] for r, v in d.get("dispatch_history", {}).items()},
            "demand_hist":   {r: len(v) for r, v in d.get("demand_history", {}).items()},
            "demand_fetch_stats": d.get("demand_fetch_stats", {}),
            "fuel_mix":      {r: len(v) for r, v in d.get("fuel_mix_history", {}).items()},
            "ic_history":    {k: len(v) for k, v in d.get("ic_history", {}).items()},
        }
    if gen_cache["data"]:
        gd = gen_cache["data"]
        result["gen_cache"] = {
            "scada_count":   gd.get("scada_count", 0),
            "reg_count":     gd.get("reg_count", 0),
            "fuel_mix_regions": list(gd.get("fuel_mix", {}).keys()),
            "fuel_mix_sample": {r: dict(list(v.items())[:3]) for r, v in gd.get("fuel_mix", {}).items()},
            "fuel_history_pts": {r: len(v) for r, v in gd.get("fuel_history", {}).items()},
            "gen_error":     gen_cache.get("error"),
        }
    else:
        result["gen_cache"] = {"status": "no data yet", "error": gen_cache.get("error")}

    if slow_cache["data"]:
        d = slow_cache["data"]
        result["slow_cache"] = {
            "stpasa_pts":    {r: len(v) for r, v in d.get("stpasa_demand", {}).items()},
        }

    return JSONResponse(content=result)


@app.get("/api/reg-test")
async def reg_test():
    """Directly test AEMO registration list download and parse."""
    import time as _time
    result = {}
    try:
        from scraper import AEMO_REG_LIST_URL, SESSION
        result["url"] = AEMO_REG_LIST_URL
        t0 = _time.time()
        r = SESSION.get(AEMO_REG_LIST_URL, timeout=30, allow_redirects=True)
        result["status_code"] = r.status_code
        result["content_type"] = r.headers.get("Content-Type", "")
        result["content_length"] = len(r.content)
        result["fetch_ms"] = round((_time.time() - t0) * 1000)
        result["final_url"] = r.url

        if r.status_code != 200:
            result["error"] = f"HTTP {r.status_code}"
            return JSONResponse(content=result)

        # Try xlrd
        try:
            import xlrd
            wb = xlrd.open_workbook(file_contents=r.content)
            result["xlrd_sheets"] = wb.sheet_names()
            # Try each sheet
            sheet_info = {}
            for name in wb.sheet_names():
                sh = wb.sheet_by_name(name)
                # First 5 rows, first 8 cols
                rows_sample = []
                for i in range(min(5, sh.nrows)):
                    rows_sample.append([str(sh.cell_value(i, j)) for j in range(min(8, sh.ncols))])
                sheet_info[name] = {"nrows": sh.nrows, "ncols": sh.ncols, "sample": rows_sample}
            result["sheets"] = sheet_info
        except Exception as e:
            result["xlrd_error"] = str(e)
            # Try openpyxl as fallback diagnostic
            try:
                import openpyxl
                wb2 = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True)
                result["openpyxl_sheets"] = wb2.sheetnames
            except Exception as e2:
                result["openpyxl_error"] = str(e2)

    except Exception as e:
        result["exception"] = str(e)
        import traceback as tb
        result["traceback"] = tb.format_exc()

    return JSONResponse(content=result)



@app.get("/api/scada-debug")
async def scada_debug():
    """Show raw SCADA DUIDs and which ones match/miss the registry."""
    from scraper import _fetch_full_scada, NEM_UNITS
    loop = asyncio.get_running_loop()
    scada = await loop.run_in_executor(None, _fetch_full_scada)
    matched, unmatched = {}, {}
    for duid, mw in scada.items():
        info = NEM_UNITS.get(duid.upper())
        if info:
            matched[duid] = {**info, "mw": mw}
        else:
            unmatched[duid] = mw
    # Sort unmatched by MW desc to see biggest missing generators
    top_unmatched = dict(sorted(unmatched.items(), key=lambda x: abs(x[1] or 0), reverse=True)[:100])
    return JSONResponse(content={
        "total_scada": len(scada),
        "matched": len(matched),
        "unmatched": len(unmatched),
        "top_unmatched_by_mw": top_unmatched,
        "matched_fuel_summary": {
            fuel: round(sum(v["mw"] or 0 for v in matched.values() if v["fuel"] == fuel), 1)
            for fuel in set(v["fuel"] for v in matched.values())
        }
    })


@app.get("/api/gen-debug")
async def gen_debug():
    """Show what DUIDs are classified as 'Other' in the current gen cache, plus top unmatched."""
    from scraper import _fetch_full_scada, NEM_UNITS, _infer_fuel_from_duid
    data = gen_cache.get("data") or {}
    grouped = data.get("grouped", {})
    other_by_region = {}
    for region, fuels in grouped.items():
        others = fuels.get("Other", [])
        if others:
            other_by_region[region] = sorted(others, key=lambda x: x.get("mw") or 0, reverse=True)[:20]

    # Live check: fetch SCADA and show top unmatched DUIDs
    loop = asyncio.get_running_loop()
    scada = await loop.run_in_executor(None, _fetch_full_scada)
    unmatched = {}
    for duid, mw in scada.items():
        if not NEM_UNITS.get(duid.upper()):
            inferred = _infer_fuel_from_duid(duid)
            unmatched[duid] = {"mw": mw, "inferred_fuel": inferred}
    top_unmatched = dict(sorted(unmatched.items(), key=lambda x: abs(x[1]["mw"] or 0), reverse=True)[:30])

    return JSONResponse(content={
        "other_by_region": other_by_region,
        "top_unmatched_scada_duids": top_unmatched,
        "nem_units_count": len(NEM_UNITS),
        "sample_nem_units_keys": list(NEM_UNITS.keys())[:20],
    })


@app.get("/api/station/{duid}")
async def station_detail(duid: str):
    from scraper import _duid_history, NEM_UNITS
    duid = duid.strip().upper()
    info = NEM_UNITS.get(duid, {})
    history = _duid_history.get(duid, {})
    history_series = [{"interval": k, "mw": v} for k, v in sorted(history.items())]
    return JSONResponse(content={
        "duid": duid, "station": info.get("station", duid),
        "fuel": info.get("fuel", "Other"), "region": info.get("region", ""),
        "capacity": info.get("capacity"), "history": history_series, "predispatch": [],
    })


@app.get("/api/stations/batch")
async def station_batch(duids: str):
    """Return history for multiple DUIDs in one request. duids=DUID1,DUID2,..."""
    from scraper import _duid_history, NEM_UNITS
    result = []
    for duid in duids.upper().split(","):
        duid = duid.strip()
        if not duid:
            continue
        info = NEM_UNITS.get(duid, {})
        history = _duid_history.get(duid, {})
        result.append({
            "duid": duid, "station": info.get("station", duid),
            "fuel": info.get("fuel", "Other"), "region": info.get("region", ""),
            "capacity": info.get("capacity"),
            "history": [{"interval": k, "mw": v} for k, v in sorted(history.items())],
            "predispatch": [],
        })
    return JSONResponse(content=result)


@app.get("/api/origin")
async def origin_history():
    """Return Origin asset history — separate from fast cache to keep /api/data lean."""
    from scraper import _duid_history, ORIGIN_DUIDS
    result = {}
    for duid in ORIGIN_DUIDS:
        history = _duid_history.get(duid, {})
        if history:
            result[duid] = [{"interval": k, "mw": v} for k, v in sorted(history.items())]
    return JSONResponse(content=result)


@app.get("/api/pd-debug")
async def pd_debug():
    """Find where AEMO publishes unit-level dispatch forecasts."""
    from scraper import _list_hrefs, _read_zip, NEMWEB_BASE
    import csv, io, re
    loop = asyncio.get_running_loop()

    dirs_to_probe = [
        f"{NEMWEB_BASE}/Reports/CURRENT/P5_Reports/",
        f"{NEMWEB_BASE}/Reports/CURRENT/PredispatchIS_Reports/",
        f"{NEMWEB_BASE}/Reports/CURRENT/DispatchIS_Reports/",
    ]

    results = {}
    for dir_url in dirs_to_probe:
        label = dir_url.split("/CURRENT/")[-1].rstrip("/")
        hrefs = await loop.run_in_executor(None, _list_hrefs, dir_url)
        # Group by prefix
        by_prefix = {}
        for h in hrefs:
            p = re.sub(r'_\d{8,}.*', '', h.split('/')[-1])
            by_prefix.setdefault(p, []).append(h)

        prefix_tables = {}
        for prefix, files in by_prefix.items():
            try:
                text = await asyncio.wait_for(
                    loop.run_in_executor(None, _read_zip, files[-1]), timeout=15)
                keys = []
                for row in csv.reader(io.StringIO(text or "")):
                    if row and row[0].strip().upper() == "I" and len(row) >= 3:
                        key = f"{row[1].strip()}_{row[2].strip()}".upper()
                        if key not in keys:
                            keys.append(key)
                prefix_tables[prefix] = {"tables": keys, "files": len(files)}
            except Exception as e:
                prefix_tables[prefix] = {"error": str(e), "files": len(files)}

        results[label] = prefix_tables

    return results


@app.get("/api/station-debug")
async def station_debug():
    from scraper import _duid_history
    total_duids = len(_duid_history)
    sample = {}
    for duid in list(_duid_history.keys())[:10]:
        pts = len(_duid_history[duid])
        last = sorted(_duid_history[duid].keys())[-1] if _duid_history[duid] else None
        sample[duid] = {"pts": pts, "last": last}
    return JSONResponse(content={
        "total_duids": total_duids,
        "sample": sample,
    })


@app.post("/api/views")
async def record_view(request: Request):
    import json, hashlib
    from datetime import datetime, timezone, timedelta
    AEST = timezone(timedelta(hours=10))
    now_aest = datetime.now(AEST)
    today   = now_aest.strftime("%Y-%m-%d")
    month   = now_aest.strftime("%Y-%m")
    # Persist in static/data/ — survives Render dyno restarts (unlike /tmp)
    data_dir = Path(__file__).parent / "static" / "data"
    data_dir.mkdir(exist_ok=True)
    path = data_dir / "views.json"
    forwarded = request.headers.get("x-forwarded-for")
    raw_ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "unknown")
    ip_hash = hashlib.sha256(raw_ip.encode()).hexdigest()[:16]
    try:
        data = json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        data = {}
    # Schema: { total, by_day, by_month, unique_ips, unique_by_day, unique_by_month }
    data.setdefault("total", 0)
    data.setdefault("by_day", {})
    data.setdefault("by_month", {})
    data.setdefault("unique_ips", [])
    data.setdefault("unique_by_day", {})
    data.setdefault("unique_by_month", {})

    data["total"] += 1
    data["by_day"][today]   = data["by_day"].get(today, 0) + 1
    data["by_month"][month] = data["by_month"].get(month, 0) + 1

    is_new_global = ip_hash not in data["unique_ips"]
    if is_new_global:
        data["unique_ips"].append(ip_hash)

    today_ips = data["unique_by_day"].setdefault(today, [])
    if ip_hash not in today_ips:
        today_ips.append(ip_hash)

    month_ips = data["unique_by_month"].setdefault(month, [])
    if ip_hash not in month_ips:
        month_ips.append(ip_hash)

    # Prune daily buckets older than 60 days (keep monthly forever)
    if len(data["by_day"]) > 60:
        for old in sorted(data["by_day"].keys())[:-60]:
            data["by_day"].pop(old, None)
            data["unique_by_day"].pop(old, None)

    try:
        path.write_text(json.dumps(data))
    except Exception:
        pass
    return {
        "total":          data["total"],
        "today":          data["by_day"].get(today, 0),
        "this_month":     data["by_month"].get(month, 0),
        "unique_total":   len(data["unique_ips"]),
        "unique_today":   len(data["unique_by_day"].get(today, [])),
        "unique_month":   len(data["unique_by_month"].get(month, [])),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = Path(__file__).parent / "static" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>Loading...</h1>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
