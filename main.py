# -*- coding: utf-8 -*-

“””
NEM Dashboard - FastAPI backend
Fast cache: prices, demand, gen, IC, Origin - refreshed every 5 min
Slow cache: all generators, ST PASA week-ahead - refreshed every 30 min
“””

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

from scraper import scrape_all, scrape_gen, scrape_slow, scrape_scada_history, scrape_mtpasa_outages

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

fast_cache   = {“data”: None, “last_updated”: None, “error”: None}
mtpasa_cache = {“data”: [],   “last_updated”: None, “error”: None}
gen_cache    = {“data”: None, “last_updated”: None, “error”: None}
slow_cache   = {“data”: None, “last_updated”: None, “error”: None}

FAST_INTERVAL = 300    # 5 min
GEN_INTERVAL  = 900    # 15 min
SLOW_INTERVAL = 1800   # 30 min

_FAST_EMPTY = {
“prices”: {}, “demand”: {}, “op_demand”: {}, “region_summary”: {},
“historical_prices”: {}, “predispatch_prices”: {}, “predispatch_demand”: {},
“predispatch_gen”: {}, “predispatch_ic”: {}, “ic_flows”: {}, “ic_history”: {},
“origin_assets”: {}, “tomorrow_prices”: {}, “tomorrow_demand”: {},
“dispatch_history”: {}, “fuel_mix”: {}, “predispatch_units”: {},
“dispatch_prices_5min”: {}, “demand_history”: {}, “op_demand_history”: {},
“interconnectors”: {}, “generation”: {}, “fuel_colors”: {}, “all_fuels”: [],
“bdu_history”: {},
}

async def _run_fast():
t0 = time.time()
try:
data = await asyncio.wait_for(
asyncio.get_running_loop().run_in_executor(None, scrape_all),
timeout=60,
)
fast_cache[“data”] = data
fast_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()
fast_cache[“error”] = None
logger.info(“Fast scrape done in %.1fs”, time.time() - t0)
except asyncio.TimeoutError:
logger.error(“Fast scrape timed out after 60s”)
fast_cache[“error”] = “timeout”
if fast_cache[“data”] is None:
fast_cache[“data”] = dict(_FAST_EMPTY)
fast_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()
except Exception as e:
logger.error(“Fast scrape error: %s”, traceback.format_exc())
fast_cache[“error”] = str(e)
if fast_cache[“data”] is None:
fast_cache[“data”] = dict(_FAST_EMPTY)
fast_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()

async def _run_slow():
t0 = time.time()
try:
loop = asyncio.get_running_loop()
data = await asyncio.wait_for(
loop.run_in_executor(None, scrape_slow),
timeout=180
)
slow_cache[“data”] = data
slow_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()
slow_cache[“error”] = None
logger.info(“Slow scrape done in %.1fs”, time.time() - t0)
except asyncio.TimeoutError:
logger.error(“Slow scrape timed out after 180s”)
slow_cache[“error”] = “timeout”
if slow_cache[“data”] is None:
slow_cache[“data”] = {
“timestamp”: datetime.now(timezone.utc).isoformat(),
“stpasa_demand”: {}, “fuel_colors”: {}, “all_fuels”: [],
“weather”: {}, “tomorrow_demand_stpasa”: {}, “mtpasa_outages”: [],
}
slow_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()
except Exception as e:
logger.error(“Slow scrape error: %s”, traceback.format_exc())
slow_cache[“error”] = str(e)
if slow_cache[“data”] is None:
slow_cache[“data”] = {
“timestamp”: datetime.now(timezone.utc).isoformat(),
“stpasa_demand”: {}, “fuel_colors”: {}, “all_fuels”: [],
“weather”: {}, “tomorrow_demand_stpasa”: {}, “mtpasa_outages”: [],
}
slow_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()

async def fast_loop():
await _run_fast()
while True:
await asyncio.sleep(FAST_INTERVAL)
try:
await _run_fast()
except Exception as e:
logger.error(“Fast loop error: %s”, e)
fast_cache[“error”] = str(e)

async def _run_gen():
t0 = time.time()
try:
loop = asyncio.get_running_loop()
data = await asyncio.wait_for(loop.run_in_executor(None, scrape_gen), timeout=60)
gen_cache[“data”] = data
gen_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()
gen_cache[“error”] = None
logger.info(“Gen scrape done in %.1fs - scada=%d reg=%d”,
time.time() - t0, data.get(“scada_count”, 0), data.get(“reg_count”, 0))
except asyncio.TimeoutError:
logger.error(“Gen scrape timed out”)
gen_cache[“error”] = “timeout”
if gen_cache[“data”] is None:
gen_cache[“data”] = {“fuel_mix”: {}, “nem_totals”: {}, “grouped”: {},
“fuel_colors”: {}, “all_fuels”: [], “scada_count”: 0, “reg_count”: 0}
except Exception as e:
logger.error(“Gen scrape error: %s”, e)
gen_cache[“error”] = str(e)

async def gen_loop():
await asyncio.sleep(5)
try:
loop = asyncio.get_running_loop()
logger.info(“Starting SCADA history backfill…”)
await asyncio.wait_for(
loop.run_in_executor(None, scrape_scada_history), timeout=120
)
logger.info(“SCADA history backfill complete”)
except Exception as e:
logger.warning(“SCADA history backfill failed: %s”, e)
await _run_gen()
while True:
await asyncio.sleep(GEN_INTERVAL)
try:
await _run_gen()
except Exception as e:
logger.error(“Gen loop error: %s”, e)
gen_cache[“error”] = str(e)

async def mtpasa_loop():
await asyncio.sleep(10)
while True:
try:
loop = asyncio.get_running_loop()
data = await asyncio.wait_for(
loop.run_in_executor(None, scrape_mtpasa_outages),
timeout=120
)
mtpasa_cache[“data”] = data
mtpasa_cache[“last_updated”] = datetime.now(timezone.utc).isoformat()
mtpasa_cache[“error”] = None
logger.info(“MTPASA scrape done: %d units”, len(data))
except asyncio.TimeoutError:
logger.warning(“MTPASA scrape timed out”)
mtpasa_cache[“error”] = “timeout”
except Exception as e:
logger.warning(“MTPASA scrape error: %s”, e)
mtpasa_cache[“error”] = str(e)
await asyncio.sleep(1800)

async def slow_loop():
await asyncio.sleep(15)
await _run_slow()
while True:
await asyncio.sleep(SLOW_INTERVAL)
try:
await _run_slow()
except Exception as e:
logger.error(“Slow loop error: %s”, e)
slow_cache[“error”] = str(e)

@asynccontextmanager
async def lifespan(app: FastAPI):
# Yield immediately - health check passes at once, scraping runs in background
fast_task   = asyncio.create_task(fast_loop())
gen_task    = asyncio.create_task(gen_loop())
slow_task   = asyncio.create_task(slow_loop())
mtpasa_task = asyncio.create_task(mtpasa_loop())
yield
fast_task.cancel()
gen_task.cancel()
slow_task.cancel()
mtpasa_task.cancel()

app = FastAPI(title=“NEM Dashboard”, lifespan=lifespan)

static_dir = Path(**file**).parent / “static”
static_dir.mkdir(exist_ok=True)
(static_dir / “data”).mkdir(exist_ok=True)
app.mount(”/static”, StaticFiles(directory=str(static_dir)), name=“static”)

@app.get(”/api/data”)
async def get_data():
if fast_cache[“data”] is None:
return JSONResponse(
content={“error”: fast_cache.get(“error”, “Loading…”), “loading”: True},
status_code=202,
)
return JSONResponse(content={
**fast_cache[“data”],
“last_updated”: fast_cache[“last_updated”],
“cache_error”:  fast_cache.get(“error”),
})

@app.get(”/api/gen”)
async def get_gen():
if gen_cache[“data”] is None:
return JSONResponse(
content={“loading”: True, “error”: gen_cache.get(“error”)},
status_code=202,
)
return JSONResponse(content={
**gen_cache[“data”],
“last_updated”: gen_cache[“last_updated”],
“cache_error”:  gen_cache.get(“error”),
})

@app.get(”/api/slow”)
async def get_slow():
if slow_cache[“data”] is None:
return JSONResponse(
content={“loading”: True, “error”: slow_cache.get(“error”)},
status_code=202,
)
return JSONResponse(content={
**slow_cache[“data”],
“mtpasa_outages”: mtpasa_cache[“data”],
“last_updated”: slow_cache[“last_updated”],
“cache_error”:  slow_cache.get(“error”),
})

@app.get(”/api/health”)
async def health():
return {
“status”:        “ok”,
“fast_updated”:  fast_cache[“last_updated”],
“slow_updated”:  slow_cache[“last_updated”],
“fast_has_data”: fast_cache[“data”] is not None,
“slow_has_data”: slow_cache[“data”] is not None,
“fast_error”:    fast_cache.get(“error”),
“slow_error”:    slow_cache.get(“error”),
}

@app.get(”/api/debug”)
async def debug():
from scraper import _list_hrefs, _read_zip, _parse_aemo
from zoneinfo import ZoneInfo
result = {}
aest = ZoneInfo(“Australia/Brisbane”)
result[“now_aest”] = datetime.now(aest).isoformat()
if fast_cache[“data”]:
d = fast_cache[“data”]
result[“fast_cache”] = {
“prices”:      d.get(“prices”, {}),
“hist_prices”: {r: len(v) for r, v in d.get(“historical_prices”, {}).items()},
“pd_prices”:   {r: len(v) for r, v in d.get(“predispatch_prices”, {}).items()},
}
if slow_cache[“data”]:
d = slow_cache[“data”]
result[“slow_cache”] = {
“stpasa_pts”: {r: len(v) for r, v in d.get(“stpasa_demand”, {}).items()},
}
return JSONResponse(content=result)

@app.get(”/api/reg-test”)
async def reg_test():
import time as _time
result = {}
try:
from scraper import AEMO_REG_LIST_URL, SESSION
result[“url”] = AEMO_REG_LIST_URL
t0 = _time.time()
r = SESSION.get(AEMO_REG_LIST_URL, timeout=30, allow_redirects=True)
result[“status_code”] = r.status_code
result[“content_length”] = len(r.content)
result[“fetch_ms”] = round((_time.time() - t0) * 1000)
except Exception as e:
result[“exception”] = str(e)
return JSONResponse(content=result)

@app.get(”/api/scada-debug”)
async def scada_debug():
from scraper import _fetch_full_scada, NEM_UNITS
loop = asyncio.get_running_loop()
scada = await loop.run_in_executor(None, _fetch_full_scada)
matched, unmatched = {}, {}
for duid, mw in scada.items():
info = NEM_UNITS.get(duid.upper())
if info:
matched[duid] = {**info, “mw”: mw}
else:
unmatched[duid] = mw
top_unmatched = dict(sorted(unmatched.items(), key=lambda x: abs(x[1] or 0), reverse=True)[:50])
return JSONResponse(content={
“total_scada”: len(scada), “matched”: len(matched), “unmatched”: len(unmatched),
“top_unmatched_by_mw”: top_unmatched,
})

@app.get(”/api/gen-debug”)
async def gen_debug():
from scraper import _fetch_full_scada, NEM_UNITS, _infer_fuel_from_duid
data = gen_cache.get(“data”) or {}
grouped = data.get(“grouped”, {})
other_by_region = {
region: sorted(fuels.get(“Other”, []), key=lambda x: x.get(“mw”) or 0, reverse=True)[:20]
for region, fuels in grouped.items() if fuels.get(“Other”)
}
loop = asyncio.get_running_loop()
scada = await loop.run_in_executor(None, _fetch_full_scada)
unmatched = {
duid: {“mw”: mw, “inferred_fuel”: _infer_fuel_from_duid(duid)}
for duid, mw in scada.items() if not NEM_UNITS.get(duid.upper())
}
top = dict(sorted(unmatched.items(), key=lambda x: abs(x[1][“mw”] or 0), reverse=True)[:30])
return JSONResponse(content={“other_by_region”: other_by_region, “top_unmatched”: top})

@app.get(”/api/station/{duid}”)
async def station_detail(duid: str):
from scraper import _duid_history, NEM_UNITS
duid = duid.strip().upper()
info = NEM_UNITS.get(duid, {})
history = _duid_history.get(duid, {})
return JSONResponse(content={
“duid”: duid, “station”: info.get(“station”, duid),
“fuel”: info.get(“fuel”, “Other”), “region”: info.get(“region”, “”),
“capacity”: info.get(“capacity”),
“history”: [{“interval”: k, “mw”: v} for k, v in sorted(history.items())],
“predispatch”: [],
})

@app.get(”/api/stations/batch”)
async def station_batch(duids: str):
from scraper import _duid_history, NEM_UNITS
result = []
for duid in duids.upper().split(”,”):
duid = duid.strip()
if not duid:
continue
info = NEM_UNITS.get(duid, {})
history = _duid_history.get(duid, {})
result.append({
“duid”: duid, “station”: info.get(“station”, duid),
“fuel”: info.get(“fuel”, “Other”), “region”: info.get(“region”, “”),
“capacity”: info.get(“capacity”),
“history”: [{“interval”: k, “mw”: v} for k, v in sorted(history.items())],
“predispatch”: [],
})
return JSONResponse(content=result)

@app.get(”/api/historical_prices”)
async def historical_prices(date: str):
import re
if not re.match(r”^\d{8}$”, date):
return JSONResponse(status_code=400, content={“error”: “date must be YYYYMMDD”})
from scraper import scrape_historical_prices
loop = asyncio.get_running_loop()
try:
data = await asyncio.wait_for(
loop.run_in_executor(None, scrape_historical_prices, date), timeout=60)
return data
except asyncio.TimeoutError:
return JSONResponse(status_code=504, content={“error”: “timeout”})
except Exception as e:
return JSONResponse(status_code=500, content={“error”: str(e)})

@app.get(”/api/origin”)
async def origin_history():
from scraper import _duid_history, ORIGIN_DUIDS
result = {}
for duid in ORIGIN_DUIDS:
history = _duid_history.get(duid, {})
if history:
result[duid] = [{“interval”: k, “mw”: v} for k, v in sorted(history.items())]
return JSONResponse(content=result)

@app.get(”/api/station-debug”)
async def station_debug():
from scraper import _duid_history
sample = {}
for duid in list(_duid_history.keys())[:10]:
pts = len(_duid_history[duid])
last = sorted(_duid_history[duid].keys())[-1] if _duid_history[duid] else None
sample[duid] = {“pts”: pts, “last”: last}
return JSONResponse(content={“total_duids”: len(_duid_history), “sample”: sample})

@app.post(”/api/views”)
async def record_view(request: Request):
import json, hashlib, os
from datetime import timedelta
import requests as _requests

```
AEST     = timezone(timedelta(hours=10))
now_aest = datetime.now(AEST)
today    = now_aest.strftime("%Y-%m-%d")
month    = now_aest.strftime("%Y-%m")

forwarded = request.headers.get("x-forwarded-for")
raw_ip    = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "unknown")
ip_hash   = hashlib.sha256(raw_ip.encode()).hexdigest()[:16]

GIST_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GIST_ID    = os.environ.get("VIEWS_GIST_ID", "")

data: dict = {}
if GIST_TOKEN and GIST_ID:
    try:
        loop = asyncio.get_running_loop()
        r = await loop.run_in_executor(None, lambda: _requests.get(
            "https://api.github.com/gists/" + GIST_ID,
            headers={"Authorization": "token " + GIST_TOKEN,
                     "Accept": "application/vnd.github.v3+json"},
            timeout=8
        ))
        if r.status_code == 200:
            files = r.json().get("files", {})
            raw   = next(iter(files.values()), {}).get("content", "{}")
            data  = json.loads(raw)
    except Exception:
        pass

data.setdefault("total", 0)
data.setdefault("by_day", {})
data.setdefault("by_month", {})
data.setdefault("unique_ips", [])
data.setdefault("unique_by_day", {})
data.setdefault("unique_by_month", {})

data["total"] += 1
data["by_day"][today]   = data["by_day"].get(today, 0) + 1
data["by_month"][month] = data["by_month"].get(month, 0) + 1

if ip_hash not in data["unique_ips"]:
    data["unique_ips"].append(ip_hash)
if ip_hash not in data["unique_by_day"].setdefault(today, []):
    data["unique_by_day"][today].append(ip_hash)
if ip_hash not in data["unique_by_month"].setdefault(month, []):
    data["unique_by_month"][month].append(ip_hash)

if len(data["by_day"]) > 60:
    for old in sorted(data["by_day"].keys())[:-60]:
        data["by_day"].pop(old, None)
        data["unique_by_day"].pop(old, None)

if GIST_TOKEN and GIST_ID:
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: _requests.patch(
            "https://api.github.com/gists/" + GIST_ID,
            headers={"Authorization": "token " + GIST_TOKEN,
                     "Accept": "application/vnd.github.v3+json"},
            json={"files": {"views.json": {"content": json.dumps(data)}}},
            timeout=8
        ))
    except Exception:
        pass

return {
    "total":        data["total"],
    "today":        data["by_day"].get(today, 0),
    "this_month":   data["by_month"].get(month, 0),
    "unique_total": len(data["unique_ips"]),
    "unique_today": len(data["unique_by_day"].get(today, [])),
    "unique_month": len(data["unique_by_month"].get(month, [])),
}
```

@app.get(”/”, response_class=HTMLResponse)
async def dashboard():
html_path = Path(**file**).parent / “static” / “index.html”
if html_path.exists():
return HTMLResponse(content=html_path.read_text())
return HTMLResponse(content=”<h1>Loading…</h1>”)

if **name** == “**main**”:
import uvicorn
uvicorn.run(“main:app”, host=“0.0.0.0”, port=8000, reload=True)
