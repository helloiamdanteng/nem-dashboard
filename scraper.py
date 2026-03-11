"""
NEMWeb scraper — concurrent fetches, Origin assets, fuel mix via OpenNEM.

AEMO CSV format:
  C rows = comments
  I rows = headers: [I, TABLE, SUBTABLE, VERSION, col1, col2, ...]
  D rows = data:    [D, TABLE, SUBTABLE, VERSION, val1, val2, ...]
  Trailing comma means last field is always empty — we handle by slicing from index 4.
"""

import re
import csv
import io
import logging
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

AEST = ZoneInfo("Australia/Sydney")
NEMWEB_BASE       = "https://www.nemweb.com.au"
DISPATCH_IS_URL   = f"{NEMWEB_BASE}/Reports/CURRENT/DispatchIS_Reports/"
PREDISPATCH_URL   = f"{NEMWEB_BASE}/Reports/CURRENT/PredispatchIS_Reports/"
SCADA_URL         = f"{NEMWEB_BASE}/Reports/CURRENT/Dispatch_SCADA/"
TRADING_CURRENT   = f"{NEMWEB_BASE}/Reports/CURRENT/TradingIS_Reports/"
ST_PASA_URL       = f"{NEMWEB_BASE}/Reports/CURRENT/Short_Term_PASA_Reports/"
OPENNEM_API       = "https://api.opennem.org.au"
AEMO_REG_LIST_URL = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/Current-Participants/NEM-Registration-and-Exemption-List.xls"

NEM_REGIONS = ["QLD1", "NSW1", "VIC1", "SA1", "TAS1"]

FUEL_COLORS = {
    "Black Coal": "#4a4a6a",
    "Brown Coal": "#8B4513",
    "Gas":        "#ff9f40",
    "Hydro":      "#36a2eb",
    "Wind":       "#4bc0c0",
    "Solar":      "#ffd700",
    "Battery":    "#9b59b6",
    "Liquid":     "#e74c3c",
    "Other":      "#95a5a6",
}
ALL_FUELS = list(FUEL_COLORS.keys())

# ---------------------------------------------------------------------------
# Origin Energy assets — DUID -> display info
# These are Origin's registered generating units in the NEM
# ---------------------------------------------------------------------------
ORIGIN_ASSETS = {
    # Eraring Power Station (Coal, NSW) — largest power station in Australia
    "ERARING1": {"name": "Eraring 1", "station": "Eraring",     "fuel": "Black Coal", "region": "NSW1", "capacity": 720},
    "ERARING2": {"name": "Eraring 2", "station": "Eraring",     "fuel": "Black Coal", "region": "NSW1", "capacity": 720},
    "ERARING3": {"name": "Eraring 3", "station": "Eraring",     "fuel": "Black Coal", "region": "NSW1", "capacity": 720},
    "ERARING4": {"name": "Eraring 4", "station": "Eraring",     "fuel": "Black Coal", "region": "NSW1", "capacity": 720},
    # Mortlake (Gas, VIC)
    "MORTLK1":  {"name": "Mortlake 1","station": "Mortlake",    "fuel": "Gas",        "region": "VIC1", "capacity": 282},
    "MORTLK2":  {"name": "Mortlake 2","station": "Mortlake",    "fuel": "Gas",        "region": "VIC1", "capacity": 282},
    # Darlington Point Solar (NSW)
    "DARLPNT1": {"name": "Darlington Pt","station":"Darlington Point","fuel":"Solar",  "region": "NSW1", "capacity": 275},
    # Shoalhaven (Hydro, NSW)
    "SHGEN":    {"name": "Shoalhaven","station": "Shoalhaven",  "fuel": "Hydro",      "region": "NSW1", "capacity": 240},
    # Quarantine (Gas, SA)
    "QPSCL1":   {"name": "Quarantine 1","station":"Quarantine", "fuel": "Gas",        "region": "SA1",  "capacity": 60},
    "QPSCL2":   {"name": "Quarantine 2","station":"Quarantine", "fuel": "Gas",        "region": "SA1",  "capacity": 60},
    "QPSCL3":   {"name": "Quarantine 3","station":"Quarantine", "fuel": "Gas",        "region": "SA1",  "capacity": 60},
    "QPSCL4":   {"name": "Quarantine 4","station":"Quarantine", "fuel": "Gas",        "region": "SA1",  "capacity": 60},
    # Uranquinty (Gas, NSW)
    "URANQ1":   {"name": "Uranquinty 1","station":"Uranquinty", "fuel": "Gas",        "region": "NSW1", "capacity": 170},
    "URANQ2":   {"name": "Uranquinty 2","station":"Uranquinty", "fuel": "Gas",        "region": "NSW1", "capacity": 170},
    "URANQ3":   {"name": "Uranquinty 3","station":"Uranquinty", "fuel": "Gas",        "region": "NSW1", "capacity": 170},
    "URANQ4":   {"name": "Uranquinty 4","station":"Uranquinty", "fuel": "Gas",        "region": "NSW1", "capacity": 170},
    # Osborne (Gas/Cogen, SA)
    "OSBORNPS": {"name": "Osborne",    "station": "Osborne",    "fuel": "Gas",        "region": "SA1",  "capacity": 180},
    # Ladbroke Grove (Gas, SA)
    "LBBG1":    {"name": "Ladbroke 1", "station": "Ladbroke Grove","fuel":"Gas",      "region": "SA1",  "capacity": 80},
    # Hallett (Wind, SA)
    "HALLWF1":  {"name": "Hallett Wind","station":"Hallett",    "fuel": "Wind",       "region": "SA1",  "capacity": 95},
    # Stockyard Hill (Wind, VIC) — Origin's large wind farm
    "STOCKYD1": {"name": "Stockyard Hill","station":"Stockyard Hill","fuel":"Wind",   "region": "VIC1", "capacity": 530},
    # Lake Bonney (Wind, SA)
    "LKBONNY1": {"name": "Lake Bonney 1","station":"Lake Bonney","fuel":"Wind",       "region": "SA1",  "capacity": 81},
    "LKBONNY2": {"name": "Lake Bonney 2","station":"Lake Bonney","fuel":"Wind",       "region": "SA1",  "capacity": 159},
    "LKBONNY3": {"name": "Lake Bonney 3","station":"Lake Bonney","fuel":"Wind",       "region": "SA1",  "capacity": 39},
    # Kareeya (Hydro, QLD)
    "KAREEYA1": {"name": "Kareeya 1",  "station": "Kareeya",    "fuel": "Hydro",     "region": "QLD1", "capacity": 44},
    "KAREEYA2": {"name": "Kareeya 2",  "station": "Kareeya",    "fuel": "Hydro",     "region": "QLD1", "capacity": 44},
    "KAREEYA3": {"name": "Kareeya 3",  "station": "Kareeya",    "fuel": "Hydro",     "region": "QLD1", "capacity": 44},
    "KAREEYA4": {"name": "Kareeya 4",  "station": "Kareeya",    "fuel": "Hydro",     "region": "QLD1", "capacity": 44},
    # Cullerin Range (Wind, NSW)
    "CULLRGWF1":{"name": "Cullerin",   "station": "Cullerin Range","fuel":"Wind",     "region": "NSW1", "capacity": 30},
    # Shoal Point (TAS — Hydro)
    "BARCALDN1":{"name": "Barcaldine", "station": "Barcaldine", "fuel": "Gas",        "region": "QLD1", "capacity": 58},
}

ORIGIN_DUIDS = set(ORIGIN_ASSETS.keys())


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NEM-Dashboard/1.0"})

def _get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"GET failed {url}: {e}")
        return None


def _list_hrefs(url: str) -> list[str]:
    r = _get(url, timeout=10)
    if not r:
        return []
    found = []
    for m in re.finditer(r'href="([^"]+\.zip)"', r.text, re.IGNORECASE):
        href = m.group(1)
        if href.startswith("http"):
            found.append(href)
        elif href.startswith("/"):
            found.append(f"{NEMWEB_BASE}{href}")
        else:
            found.append(url.rstrip("/") + "/" + href)
    return sorted(set(found))


def _read_zip(url: str) -> str:
    r = _get(url, timeout=30)
    if not r:
        return ""
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                return ""
            with z.open(csvs[0]) as f:
                return f.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"ZIP read failed {url}: {e}")
        return ""


# ---------------------------------------------------------------------------
# AEMO CSV parser
# ---------------------------------------------------------------------------

def _parse_aemo(text: str, table_key: str) -> list[dict]:
    """Return data rows for table matching table_key (checked against TABLE_SUBTABLE)."""
    results = []
    headers: list[str] = []
    in_table = False
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row:
            continue
        ind = row[0].strip().upper()
        if ind == "I" and len(row) >= 5:
            key = f"{row[1].strip()}_{row[2].strip()}".upper()
            if table_key.upper() in key:
                headers = [c.strip().upper() for c in row[4:] if c.strip()]
                in_table = True
            else:
                in_table = False
        elif ind == "D" and in_table and headers:
            vals = [c.strip() for c in row[4:]]
            vals = vals[:len(headers)]
            while len(vals) < len(headers):
                vals.append("")
            results.append(dict(zip(headers, vals)))
    return results


def get_latest_file_url(directory_url: str, prefix: str = "") -> Optional[str]:
    urls = _list_hrefs(directory_url)
    if prefix:
        urls = [u for u in urls if prefix.lower() in u.lower()]
    return urls[-1] if urls else None


def get_all_file_urls(directory_url: str, prefix: str = "") -> list[str]:
    urls = _list_hrefs(directory_url)
    if prefix:
        urls = [u for u in urls if prefix.lower() in u.lower()]
    return urls


# ---------------------------------------------------------------------------
# Dispatch IS — prices, demand, generation, interconnectors (one file fetch)
# ---------------------------------------------------------------------------

def _fetch_dispatch_is() -> str:
    url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
    return _read_zip(url) if url else ""


def scrape_region_summary(text: str) -> dict:
    summary: dict[str, dict] = {}
    for row in _parse_aemo(text, "DISPATCH_PRICE"):
        region = row.get("REGIONID", "")
        if region not in NEM_REGIONS:
            continue
        if row.get("INTERVENTION", "0") not in ("0", ""):
            continue  # skip intervention runs
        e = summary.setdefault(region, {})
        for f in ["RRP"]:
            v = row.get(f, "")
            if v:
                try: e[f] = round(float(v), 2)
                except ValueError: pass
    for row in _parse_aemo(text, "DISPATCH_REGIONSUM"):
        region = row.get("REGIONID", "")
        if region not in NEM_REGIONS:
            continue
        if row.get("INTERVENTION", "0") not in ("0", ""):
            continue  # skip intervention runs
        e = summary.setdefault(region, {})
        for f in ["TOTALDEMAND","DEMANDFORECAST","INITIALSUPPLY",
                  "DISPATCHABLEGENERATION","SEMISCHEDULEDGENERATION","NETINTERCHANGE"]:
            v = row.get(f, "")
            if v:
                try: e[f] = round(float(v), 1)
                except ValueError: pass
    return summary


def scrape_interconnectors(text: str) -> dict:
    flows = {}
    for row in _parse_aemo(text, "DISPATCH_INTERCONNECTORRES"):
        ic = row.get("INTERCONNECTORID", "")
        if not ic:
            continue
        try:
            flows[ic] = {
                "flow":   round(float(row.get("MWFLOW", 0) or 0), 1),
                "losses": round(float(row.get("MWLOSSES", 0) or 0), 1),
            }
        except ValueError:
            pass
    return flows


# ---------------------------------------------------------------------------
# SCADA — per-DUID actual MW output (for Origin assets)
# ---------------------------------------------------------------------------

def scrape_scada_duids(duids: set) -> dict:
    """
    Fetch DISPATCH_UNIT_SCADA and return { duid: mw } for requested DUIDs.
    """
    url = get_latest_file_url(SCADA_URL, "PUBLIC_DISPATCHSCADA")
    if not url:
        # fallback: try DispatchIS UNIT_SOLUTION
        return {}
    text = _read_zip(url)
    result = {}
    for row in _parse_aemo(text, "DISPATCH_UNIT_SCADA"):
        duid = row.get("DUID", "").strip().upper()
        if duid in duids:
            v = row.get("SCADAVALUE", "")
            try:
                result[duid] = round(float(v), 1)
            except (ValueError, TypeError):
                pass
    # Also try UNIT_SOLUTION in DispatchIS if SCADA didn't find them
    return result


def scrape_unit_solution(text: str, duids: set) -> dict:
    """Extract INITIALMW from DISPATCH_UNIT_SOLUTION for given DUIDs."""
    result = {}
    for row in _parse_aemo(text, "DISPATCH_UNIT_SOLUTION"):
        duid = row.get("DUID", "").strip().upper()
        if duid in duids:
            v = row.get("INITIALMW", row.get("TOTALCLEARED", ""))
            try:
                result[duid] = round(float(v), 1)
            except (ValueError, TypeError):
                pass
    return result


# ---------------------------------------------------------------------------
# Trading prices (historical — archive)
# ---------------------------------------------------------------------------

def scrape_trading_prices_today() -> tuple:
    """
    Returns (prices_dict, trading_zips_used).
    Uses CURRENT TradingIS directory — individual 30-min ZIP files for today.
    File pattern: PUBLIC_TRADINGIS_YYYYMMDDHHMM_<id>.zip
    """
    now_aest = datetime.now(AEST)
    today_str = now_aest.strftime("%Y%m%d")

    all_zips = _list_hrefs(TRADING_CURRENT)
    # Each file name contains the datetime stamp — filter to today's files only
    today_zips = [u for u in all_zips if today_str in u]
    if not today_zips:
        # Fallback: take the last 48 files (covers full day even if date string differs)
        today_zips = all_zips[-48:]
    # Always cap at last 50 so we don't over-fetch (48 intervals per day max)
    today_zips = today_zips[-50:]

    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}

    def fetch_one(url):
        text = _read_zip(url)
        rows = _parse_aemo(text, "TRADING_PRICE")
        pts = []
        for row in rows:
            region = row.get("REGIONID", "")
            if region not in NEM_REGIONS:
                continue
            if row.get("INTERVENTION", "0") not in ("0", ""):
                continue
            dt_str = row.get("SETTLEMENTDATE", "")
            rrp_str = row.get("RRP", "")
            if not dt_str or not rrp_str:
                continue
            try:
                rrp = round(float(rrp_str), 2)
                # Shift back 30min: AEMO timestamps are end-of-interval
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                pts.append((region, dt.strftime("%H:%M"), rrp))
            except (ValueError, TypeError):
                pass
        return pts

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(fetch_one, u) for u in today_zips]
        for fut in as_completed(futures):
            for region, label, rrp in fut.result():
                region_series[region][label] = rrp

    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "rrp": v} for k, v in sorted(series.items())]
    logger.info(f"Trading prices: {sum(len(v) for v in result.values())} pts from {len(today_zips)} zips")
    return result, today_zips


# ---------------------------------------------------------------------------
# Predispatch
# ---------------------------------------------------------------------------

def _fetch_predispatch() -> str:
    url = get_latest_file_url(PREDISPATCH_URL, "PUBLIC_PREDISPATCHIS")
    return _read_zip(url) if url else ""


def scrape_predispatch_prices(text: str) -> dict:
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    for tk in ["PREDISPATCH_REGION_PRICES", "PREDISPATCH_PRICE", "PREDISPATCH_REGIONPRICE"]:
        rows = _parse_aemo(text, tk)
        if not rows:
            continue
        for row in rows:
            region = row.get("REGIONID", "")
            if region not in NEM_REGIONS:
                continue
            if row.get("INTERVENTION", "0") not in ("0", ""):
                continue
            dt_str = row.get("DATETIME", row.get("SETTLEMENTDATE", ""))
            rrp_str = row.get("RRP", "")
            if not dt_str or not rrp_str:
                continue
            try:
                rrp = round(float(rrp_str), 2)
                # Predispatch DATETIME is end-of-interval — shift back 30min
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt >= now_aest:
                    region_series[region][dt.strftime("%H:%M")] = rrp
            except (ValueError, TypeError):
                pass
        break
    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "rrp": v} for k, v in sorted(series.items())]
    logger.info(f"Predispatch prices: {sum(len(v) for v in result.values())} pts")
    return result


def scrape_predispatch_demand(text: str) -> dict:
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    for tk in ["PREDISPATCH_REGION_SOLUTION", "PREDISPATCH_REGIONSOLUTION"]:
        rows = _parse_aemo(text, tk)
        if not rows:
            continue
        for row in rows:
            region = row.get("REGIONID", "")
            if region not in NEM_REGIONS:
                continue
            if row.get("INTERVENTION", "0") not in ("0", ""):
                continue
            dt_str = row.get("DATETIME", row.get("SETTLEMENTDATE", ""))
            demand_str = row.get("TOTALDEMAND", row.get("DEMAND", ""))
            if not dt_str or not demand_str:
                continue
            try:
                demand = round(float(demand_str), 1)
                # Predispatch DATETIME is end-of-interval — shift back 30min
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt >= now_aest:
                    region_series[region][dt.strftime("%H:%M")] = demand
            except (ValueError, TypeError):
                pass
        break
    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "demand": v} for k, v in sorted(series.items())]
    return result


def scrape_predispatch_generation(text: str) -> dict:
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    for tk in ["PREDISPATCH_REGION_SOLUTION", "PREDISPATCH_REGIONSOLUTION"]:
        rows = _parse_aemo(text, tk)
        if not rows:
            continue
        for row in rows:
            region = row.get("REGIONID", "")
            if region not in NEM_REGIONS:
                continue
            dt_str = row.get("DATETIME", row.get("SETTLEMENTDATE", ""))
            ss  = row.get("SEMISCHEDULEDGENERATION", "")
            sch = row.get("DISPATCHABLEGENERATION", "")
            if not dt_str:
                continue
            try:
                dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                if dt.replace(tzinfo=None) >= now_aest:
                    region_series[region][dt.strftime("%H:%M")] = {
                        "SemiScheduled": round(float(ss), 1) if ss else 0,
                        "Scheduled":     round(float(sch), 1) if sch else 0,
                    }
            except (ValueError, TypeError):
                pass
        break
    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, **v} for k, v in sorted(series.items())]
    return result


# ---------------------------------------------------------------------------
# Fuel mix — OpenNEM (single NEM-wide call, not 5 calls)
# ---------------------------------------------------------------------------

def _normalise_fuel(fuel: str) -> str:
    f = fuel.upper()
    if "COAL_BLACK" in f or "BLACK" in f:          return "Black Coal"
    if "COAL_BROWN" in f or "BROWN" in f:          return "Brown Coal"
    if "GAS" in f or "OCGT" in f or "CCGT" in f:  return "Gas"
    if "HYDRO" in f or "WATER" in f:               return "Hydro"
    if "WIND" in f:                                return "Wind"
    if "SOLAR" in f or "ROOFTOP" in f:             return "Solar"
    if "BATTERY" in f or "STORAGE" in f:           return "Battery"
    if "LIQUID" in f or "DISTILLATE" in f:         return "Liquid"
    return "Other"


def scrape_fuel_mix_history_opennem() -> dict:
    """
    Single API call to OpenNEM for the whole NEM, then split by region.
    Returns { region: [ {interval, Black Coal, Gas, ...}, ... ] }
    """
    try:
        url = f"{OPENNEM_API}/v4/stats/power/network/NEM?interval=30m&period=1d"
        r = _get(url, timeout=15)
        if not r:
            return {}
        data = r.json()
        # result: region -> interval_str -> fuel -> mw
        result: dict[str, dict] = {}
        for series in data.get("data", []):
            fuel_raw = series.get("fuel_tech") or series.get("type") or ""
            net_region = series.get("network_region", "")
            if not fuel_raw or not net_region:
                continue
            region = net_region if net_region.endswith("1") else net_region + "1"
            if region not in NEM_REGIONS:
                continue
            fuel = _normalise_fuel(fuel_raw)
            history = series.get("history", {})
            start_str = history.get("start", "")
            interval_mins = history.get("interval", 30)
            values = history.get("data", []) or []
            if not start_str or not values:
                continue
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00")).astimezone(AEST)
                for i, val in enumerate(values):
                    if val is None:
                        continue
                    slot = (start_dt + timedelta(minutes=i * interval_mins)).strftime("%H:%M")
                    rd = result.setdefault(region, {})
                    sd = rd.setdefault(slot, {})
                    sd[fuel] = sd.get(fuel, 0) + round(float(val), 1)
            except (ValueError, TypeError):
                pass
        final = {}
        for region, series in result.items():
            if series:
                final[region] = [{"interval": k, **v} for k, v in sorted(series.items())]
        logger.info(f"Fuel mix: {sum(len(v) for v in final.values())} pts across {len(final)} regions")
        return final
    except Exception as e:
        logger.warning(f"OpenNEM fuel mix failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Demand history from TradingIS archive (30-min, reuses already-listed zips)
# Same files used for price history — no extra directory listing needed.
# ---------------------------------------------------------------------------

def scrape_demand_history_today(trading_zips: list) -> dict:
    """
    Extract TOTALDEMAND from today's TradingIS archive zips (30-min intervals).
    AEMO SETTLEMENTDATE is end-of-interval, so we shift back 30min for display.
    Returns { region: [{interval, demand}] }
    """
    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}

    def fetch_one(url):
        text = _read_zip(url)
        pts = []
        for row in _parse_aemo(text, "TRADING_REGIONSUM"):
            region = row.get("REGIONID", "")
            if region not in NEM_REGIONS:
                continue
            if row.get("INTERVENTION", "0") not in ("0", ""):
                continue
            dt_str = row.get("SETTLEMENTDATE", "")
            demand_str = row.get("TOTALDEMAND", "")
            if not dt_str or not demand_str:
                continue
            try:
                demand = round(float(demand_str), 1)
                # Shift back 30min: AEMO timestamps are end-of-interval
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                label = dt.strftime("%H:%M")
                pts.append((region, label, demand))
            except (ValueError, TypeError):
                pass
        return pts

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(fetch_one, u) for u in trading_zips]
        for fut in as_completed(futures):
            for region, label, demand in fut.result():
                region_series[region][label] = demand

    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "demand": v} for k, v in sorted(series.items())]
    logger.info(f"Demand history: {sum(len(v) for v in result.values())} pts across {len(result)} regions")
    return result


# ---------------------------------------------------------------------------
# In-memory demand history — lightweight fallback if archive fetch fails
# ---------------------------------------------------------------------------

_demand_history: dict[str, dict] = {r: {} for r in NEM_REGIONS}


def _update_demand_history(region_summary: dict) -> None:
    label = datetime.now(AEST).strftime("%H:%M")
    for region, data in region_summary.items():
        if region in NEM_REGIONS and "TOTALDEMAND" in data:
            _demand_history[region][label] = data["TOTALDEMAND"]


def _get_demand_history() -> dict:
    result = {}
    for region, series in _demand_history.items():
        if series:
            result[region] = [{"interval": k, "demand": v} for k, v in sorted(series.items())]
    return result


# ---------------------------------------------------------------------------
# In-memory interconnector history + predispatch IC flows
# ---------------------------------------------------------------------------

_ic_history: dict[str, dict] = {}


def _update_ic_history(ic_snapshot: dict) -> None:
    label = datetime.now(AEST).strftime("%H:%M")
    for ic_id, vals in ic_snapshot.items():
        if ic_id not in _ic_history:
            _ic_history[ic_id] = {}
        _ic_history[ic_id][label] = vals.get("flow", 0)


def _get_ic_history() -> dict:
    result = {}
    for ic_id, series in _ic_history.items():
        if series:
            result[ic_id] = [{"interval": k, "flow": v} for k, v in sorted(series.items())]
    return result


def scrape_predispatch_interconnectors(text: str) -> dict:
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    ic_series: dict[str, dict] = {}
    for tk in ["PREDISPATCH_INTERCONNECTOR_SOLN", "PREDISPATCH_INTERCONNECTORSOLN"]:
        rows = _parse_aemo(text, tk)
        if not rows:
            continue
        for row in rows:
            ic = row.get("INTERCONNECTORID", "").strip()
            dt_str = row.get("DATETIME", row.get("SETTLEMENTDATE", ""))
            flow_str = row.get("MWFLOW", "")
            if not ic or not dt_str or not flow_str:
                continue
            try:
                flow = round(float(flow_str), 1)
                dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                if dt.replace(tzinfo=None) >= now_aest:
                    ic_series.setdefault(ic, {})[dt.strftime("%H:%M")] = flow
            except (ValueError, TypeError):
                pass
        break
    return {ic: [{"interval": k, "flow": v} for k, v in sorted(s.items())]
            for ic, s in ic_series.items() if s}


# ---------------------------------------------------------------------------
# Main scrape_all — parallel fetches
# ---------------------------------------------------------------------------

def scrape_all() -> dict:
    logger.info("scrape_all starting...")

    # Run all IO-bound fetches concurrently
    with ThreadPoolExecutor(max_workers=8) as ex:
        f_dispatch_is   = ex.submit(_fetch_dispatch_is)
        f_predispatch   = ex.submit(_fetch_predispatch)
        f_trading       = ex.submit(scrape_trading_prices_today)
        f_fuel_mix      = ex.submit(scrape_fuel_mix_history_opennem)
        f_scada         = ex.submit(scrape_scada_duids, ORIGIN_DUIDS)

    dispatch_text    = f_dispatch_is.result()
    predispatch_text = f_predispatch.result()
    trading_result   = f_trading.result()
    fuel_mix         = f_fuel_mix.result()
    scada_vals       = f_scada.result()

    # Unpack trading — returns (prices_dict, zips_used)
    if isinstance(trading_result, tuple):
        trading_prices, trading_zips = trading_result
    else:
        trading_prices, trading_zips = trading_result, []

    # Fetch demand history reusing the same trading zips (no extra HTTP calls)
    dispatch_history = scrape_demand_history_today(trading_zips) if trading_zips else {}

    # Parse dispatch IS
    region_summary  = scrape_region_summary(dispatch_text)
    interconnectors = scrape_interconnectors(dispatch_text)

    # Supplement SCADA with unit solution if needed
    missing = ORIGIN_DUIDS - set(scada_vals.keys())
    if missing:
        unit_sol = scrape_unit_solution(dispatch_text, missing)
        scada_vals.update(unit_sol)

    # Parse predispatch
    pd_prices = scrape_predispatch_prices(predispatch_text)
    pd_demand = scrape_predispatch_demand(predispatch_text)
    pd_gen    = scrape_predispatch_generation(predispatch_text)

    # Use full-day dispatch archive for demand history; fall back to in-memory
    _update_demand_history(region_summary)
    if dispatch_history:
        demand_history = {
            region: [{"interval": pt["interval"], "demand": pt["demand"]}
                     for pt in pts if pt.get("demand") is not None]
            for region, pts in dispatch_history.items()
        }
    else:
        demand_history = _get_demand_history()
    _update_ic_history(interconnectors)
    ic_history = _get_ic_history()
    pd_interconnectors = scrape_predispatch_interconnectors(predispatch_text)

    # Build simple current values
    prices     = {r: d["RRP"]                    for r, d in region_summary.items() if "RRP" in d}
    demand     = {r: d["TOTALDEMAND"]             for r, d in region_summary.items() if "TOTALDEMAND" in d}
    generation = {r: {
        "Scheduled":      d.get("DISPATCHABLEGENERATION", 0),
        "Semi-Scheduled": d.get("SEMISCHEDULEDGENERATION", 0),
        "Net Interchange":d.get("NETINTERCHANGE", 0),
    } for r, d in region_summary.items() if "DISPATCHABLEGENERATION" in d}

    # Build Origin assets output
    origin_assets_out = {}
    for duid, info in ORIGIN_ASSETS.items():
        mw = scada_vals.get(duid)
        origin_assets_out[duid] = {
            **info,
            "mw":      mw,
            "pct":     round(mw / info["capacity"] * 100, 1) if mw is not None and info["capacity"] else None,
            "status":  "running" if (mw is not None and mw > 5) else ("off" if mw is not None else "unknown"),
        }

    logger.info(
        f"scrape_all done — prices:{list(prices.keys())} "
        f"origin_duids_found:{len(scada_vals)} "
        f"fuel_mix_regions:{list(fuel_mix.keys())}"
    )

    return {
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "prices":             prices,
        "demand":             demand,
        "generation":         generation,
        "interconnectors":    interconnectors,
        "raw_summary":        region_summary,
        "historical_prices":  trading_prices,
        "predispatch_prices": pd_prices,
        "demand_history":     demand_history,
        "predispatch_demand": pd_demand,
        "fuel_mix_history":   fuel_mix,
        "predispatch_gen":    pd_gen,
        "dispatch_history":   dispatch_history,
        "ic_history":         ic_history,
        "predispatch_ic":     pd_interconnectors,
        "origin_assets":      origin_assets_out,
        "fuel_colors":        FUEL_COLORS,
        "all_fuels":          ALL_FUELS,
    }


# ---------------------------------------------------------------------------
# Registration list cache — DUID -> {station, fuel, region, capacity}
# ---------------------------------------------------------------------------

_reg_cache: dict = {}
_reg_cache_date: str = ""


def _fuel_from_reg(fuel_raw: str) -> str:
    """Map AEMO registration fuel source string to display fuel."""
    f = (fuel_raw or "").upper()
    if "BLACK COAL" in f or "COAL" in f and "BROWN" not in f: return "Black Coal"
    if "BROWN COAL" in f or "BROWN" in f:                      return "Brown Coal"
    if "GAS" in f or "OCGT" in f or "CCGT" in f or "LIQUID FUEL" in f and "GAS" in f: return "Gas"
    if "HYDRO" in f or "WATER" in f:                           return "Hydro"
    if "WIND" in f:                                             return "Wind"
    if "SOLAR" in f or "PHOTOVOLTAIC" in f:                    return "Solar"
    if "BATTERY" in f or "STORAGE" in f:                       return "Battery"
    if "LIQUID" in f or "DISTILLATE" in f or "DIESEL" in f:   return "Liquid"
    return "Other"


def _load_registration_list() -> dict:
    """
    Download and parse AEMO NEM Registration and Exemption List (XLS).
    Returns { DUID: {station, fuel, region, capacity_mw, participant} }
    Cached once per day.
    """
    global _reg_cache, _reg_cache_date
    today = datetime.now(AEST).strftime("%Y-%m-%d")
    if _reg_cache and _reg_cache_date == today:
        return _reg_cache

    try:
        import openpyxl
    except ImportError:
        logger.warning("openpyxl not installed; registration list unavailable")
        return _reg_cache

    try:
        r = _get(AEMO_REG_LIST_URL, timeout=30)
        if not r:
            return _reg_cache

        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
        # Sheet name varies; find the generators sheet
        sheet = None
        for name in wb.sheetnames:
            if "generator" in name.lower() or "scheduled" in name.lower():
                sheet = wb[name]
                break
        if sheet is None and wb.sheetnames:
            sheet = wb[wb.sheetnames[0]]
        if sheet is None:
            return _reg_cache

        rows = list(sheet.iter_rows(values_only=True))
        # Find header row
        header_row = None
        for i, row in enumerate(rows[:10]):
            cells = [str(c or "").upper() for c in row]
            if any("DUID" in c for c in cells):
                header_row = i
                break
        if header_row is None:
            return _reg_cache

        headers = [str(c or "").strip().upper() for c in rows[header_row]]

        def col(name_parts):
            for i, h in enumerate(headers):
                if any(p.upper() in h for p in name_parts):
                    return i
            return None

        ci_duid     = col(["DUID"])
        ci_station  = col(["STATION NAME", "STATION"])
        ci_fuel     = col(["FUEL SOURCE - DESCRIPTOR", "FUEL SOURCE", "FUEL"])
        ci_region   = col(["REGION"])
        ci_capacity = col(["REG CAP", "REGISTERED CAPACITY", "MAX CAP", "CAPACITY"])
        ci_part     = col(["PARTICIPANT"])

        result = {}
        for row in rows[header_row + 1:]:
            if not row or ci_duid is None:
                continue
            duid = str(row[ci_duid] or "").strip().upper()
            if not duid or duid == "DUID":
                continue
            station  = str(row[ci_station]  or "").strip() if ci_station  is not None else ""
            fuel_raw = str(row[ci_fuel]     or "").strip() if ci_fuel     is not None else ""
            region   = str(row[ci_region]   or "").strip() if ci_region   is not None else ""
            cap_raw  = row[ci_capacity]                     if ci_capacity is not None else None
            part     = str(row[ci_part]     or "").strip() if ci_part     is not None else ""
            try:
                capacity = round(float(cap_raw), 1) if cap_raw not in (None, "") else None
            except (ValueError, TypeError):
                capacity = None
            # Normalise region
            if region and not region.endswith("1"):
                region = region + "1"
            result[duid] = {
                "station":     station or duid,
                "fuel":        _fuel_from_reg(fuel_raw),
                "fuel_raw":    fuel_raw,
                "region":      region,
                "capacity":    capacity,
                "participant": part,
            }

        _reg_cache = result
        _reg_cache_date = today
        logger.info(f"Registration list loaded: {len(result)} DUIDs")
        return result

    except Exception as e:
        logger.warning(f"Registration list load failed: {e}")
        return _reg_cache


# ---------------------------------------------------------------------------
# Full SCADA fetch — all DUIDs (not just Origin)
# ---------------------------------------------------------------------------

def _fetch_full_scada() -> dict:
    """Return { DUID: mw } for every unit in DISPATCH_UNIT_SCADA."""
    url = get_latest_file_url(SCADA_URL, "PUBLIC_DISPATCHSCADA")
    if not url:
        return {}
    text = _read_zip(url)
    result = {}
    for row in _parse_aemo(text, "DISPATCH_UNIT_SCADA"):
        duid = row.get("DUID", "").strip().upper()
        v = row.get("SCADAVALUE", "")
        try:
            result[duid] = round(float(v), 1)
        except (ValueError, TypeError):
            pass
    logger.info(f"Full SCADA: {len(result)} DUIDs")
    return result


# ---------------------------------------------------------------------------
# ST PASA — 7-day ahead regional demand forecast
# ---------------------------------------------------------------------------

def scrape_stpasa_demand() -> dict:
    """
    Fetch latest ST PASA file and extract STPASA_REGIONSOLUTION.
    Returns { region: [{interval, demand_50, demand_10}] } for next ~7 days.
    """
    try:
        urls = _list_hrefs(ST_PASA_URL)
        # Find latest STPASA file
        pasa_urls = sorted([u for u in urls if "STPASA" in u.upper()])
        if not pasa_urls:
            logger.warning("No ST PASA files found")
            return {}
        url = pasa_urls[-1]
        text = _read_zip(url)

        now_aest = datetime.now(AEST).replace(tzinfo=None)
        region_series: dict = {r: {} for r in NEM_REGIONS}

        for tk in ["STPASA_REGIONSOLUTION", "ST_PASA_REGIONSOLUTION"]:
            rows = _parse_aemo(text, tk)
            if not rows:
                continue
            for row in rows:
                region = row.get("REGIONID", "").strip()
                if region not in NEM_REGIONS:
                    continue
                dt_str = row.get("INTERVAL_DATETIME", row.get("SETTLEMENTDATE", ""))
                d50 = row.get("DEMAND50", row.get("TOTALDEMAND", ""))
                d10 = row.get("DEMAND10", "")
                if not dt_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                    if dt.replace(tzinfo=None) < now_aest:
                        continue
                    label = dt.strftime("%Y-%m-%d %H:%M")
                    region_series[region][label] = {
                        "demand_50": round(float(d50), 1) if d50 else None,
                        "demand_10": round(float(d10), 1) if d10 else None,
                    }
                except (ValueError, TypeError):
                    pass
            if any(region_series.values()):
                break

        result = {}
        for region, series in region_series.items():
            if series:
                result[region] = [{"interval": k, **v} for k, v in sorted(series.items())]
        logger.info(f"ST PASA: {sum(len(v) for v in result.values())} pts across {len(result)} regions")
        return result
    except Exception as e:
        logger.warning(f"ST PASA fetch failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# All-generators scrape (for /api/slow endpoint)
# ---------------------------------------------------------------------------

def scrape_all_generators() -> dict:
    """
    Fetch full SCADA output for all NEM DUIDs, join with registration list.
    Returns grouped structure: { region: { fuel: [ {duid, station, mw, capacity, pct} ] } }
    """
    # Fetch SCADA and registration list concurrently
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_scada = ex.submit(_fetch_full_scada)
        f_reg   = ex.submit(_load_registration_list)
    scada  = f_scada.result()
    reg    = f_reg.result()

    timestamp = datetime.now(timezone.utc).isoformat()

    # Group by region → fuel → list of units
    grouped: dict = {}
    for duid, mw in scada.items():
        info = reg.get(duid, {})
        region   = info.get("region", "UNKNOWN")
        fuel     = info.get("fuel", "Other")
        station  = info.get("station", duid)
        capacity = info.get("capacity")
        pct = round(mw / capacity * 100, 1) if (mw is not None and capacity and capacity > 0) else None

        if region not in NEM_REGIONS:
            continue  # skip non-NEM (WA, etc.)

        rg = grouped.setdefault(region, {})
        fg = rg.setdefault(fuel, [])
        fg.append({
            "duid":     duid,
            "station":  station,
            "mw":       round(mw, 1) if mw is not None else None,
            "capacity": capacity,
            "pct":      pct,
            "participant": info.get("participant", ""),
        })

    # Sort within each fuel group by MW descending
    for region in grouped:
        for fuel in grouped[region]:
            grouped[region][fuel].sort(key=lambda x: x["mw"] or 0, reverse=True)

    # Summary stats per region/fuel
    summary: dict = {}
    for region, fuels in grouped.items():
        summary[region] = {}
        for fuel, units in fuels.items():
            total_mw  = sum(u["mw"] or 0 for u in units)
            total_cap = sum(u["capacity"] or 0 for u in units)
            summary[region][fuel] = {
                "total_mw":  round(total_mw, 1),
                "total_cap": round(total_cap, 1),
                "unit_count": len(units),
            }

    logger.info(f"scrape_all_generators: {sum(len(v) for r in grouped.values() for v in r.values())} units across {len(grouped)} regions")
    return {
        "timestamp":   timestamp,
        "grouped":     grouped,
        "summary":     summary,
        "fuel_colors": FUEL_COLORS,
    }


# ---------------------------------------------------------------------------
# scrape_slow — background fetch for generators + week-ahead
# ---------------------------------------------------------------------------

def scrape_slow() -> dict:
    """
    Heavy scrape for generators page and week-ahead page.
    Runs in background after fast scrape completes.
    """
    logger.info("scrape_slow starting...")
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_gens   = ex.submit(scrape_all_generators)
        f_pasa   = ex.submit(scrape_stpasa_demand)
        f_fuel7  = ex.submit(scrape_fuel_mix_history_opennem)  # reuse for week-ahead fuel context

    gens  = f_gens.result()
    pasa  = f_pasa.result()
    fuel7 = f_fuel7.result()

    logger.info("scrape_slow done")
    return {
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "generators":       gens,
        "stpasa_demand":    pasa,
        "fuel_mix_today":   fuel7,
        "fuel_colors":      FUEL_COLORS,
        "all_fuels":        ALL_FUELS,
    }


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    data = scrape_all()
    summary = {
        "prices":       data["prices"],
        "hist_prices":  {r: len(v) for r, v in data["historical_prices"].items()},
        "pd_prices":    {r: len(v) for r, v in data["predispatch_prices"].items()},
        "demand_hist":  {r: len(v) for r, v in data["demand_history"].items()},
        "fuel_mix":     {r: len(v) for r, v in data["fuel_mix_history"].items()},
        "origin_found": {k: v["mw"] for k, v in data["origin_assets"].items() if v["mw"] is not None},
    }
    print(json.dumps(summary, indent=2))
