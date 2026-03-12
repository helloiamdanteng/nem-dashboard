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

AEST = ZoneInfo("Australia/Brisbane")  # UTC+10 fixed — AEMO never uses daylight saving
NEMWEB_BASE       = "https://www.nemweb.com.au"
DISPATCH_IS_URL   = f"{NEMWEB_BASE}/Reports/CURRENT/DispatchIS_Reports/"
PREDISPATCH_URL   = f"{NEMWEB_BASE}/Reports/CURRENT/PredispatchIS_Reports/"
SCADA_URL         = f"{NEMWEB_BASE}/Reports/CURRENT/Dispatch_SCADA/"
TRADING_CURRENT   = f"{NEMWEB_BASE}/Reports/CURRENT/TradingIS_Reports/"
ST_PASA_URL       = f"{NEMWEB_BASE}/Reports/CURRENT/Short_Term_PASA_Reports/"
OPENNEM_API       = "https://api.opennem.org.au"
AEMO_REG_LIST_URL = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/Current-Participants/NEM-Registration-and-Exemption-List.xls"

NEM_REGIONS = ["QLD1", "NSW1", "VIC1", "SA1", "TAS1"]

# ---------------------------------------------------------------------------
# Static unit registry — loaded once from nem_units.json at startup
# ---------------------------------------------------------------------------
import json as _json
from pathlib import Path as _Path

_AEMO_REG_URL = "https://www.aemo.com.au/-/media/files/electricity/nem/participant_information/nem-registration-and-exemption-list.xlsx"

# Fuel type mapping from AEMO's "Fuel Source - Primary" and "Technology Type" columns
_FUEL_MAP = {
    # Coal
    "black coal": "Black Coal",
    "coal": "Black Coal",
    "brown coal": "Brown Coal",
    "lignite": "Brown Coal",
    # Gas
    "natural gas": "Gas",
    "gas": "Gas",
    "coal seam methane": "Gas",
    "coal seam gas": "Gas",
    "landfill methane gas": "Gas",
    "biogas": "Gas",
    "waste coal mine gas": "Gas",
    # Hydro
    "water": "Hydro",
    "hydro": "Hydro",
    "pumped hydro": "Hydro",
    # Wind
    "wind": "Wind",
    # Solar
    "solar": "Solar",
    "solar thermal": "Solar",
    # Battery
    "battery storage": "Battery",
    "battery": "Battery",
    # Liquid
    "liquid fuel": "Liquid",
    "diesel": "Liquid",
    "fuel oil": "Liquid",
    # Other
    "biomass": "Other",
    "waste": "Other",
    "kerosene": "Liquid",
}

def _fetch_aemo_registration() -> dict:
    """
    Download and parse the AEMO NEM Registration and Exemption List XLSX.
    Returns { DUID: { station, fuel, region, capacity } }
    """
    import io
    try:
        import openpyxl
    except ImportError:
        logger.warning("openpyxl not available for registration fetch")
        return {}

    try:
        logger.info("Fetching AEMO registration list…")
        resp = requests.get(_AEMO_REG_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True, data_only=True)

        # Find the "Generators and Scheduled Loads" sheet
        sheet = None
        for name in wb.sheetnames:
            if "generator" in name.lower() or "scheduled" in name.lower():
                sheet = wb[name]
                break
        if sheet is None:
            sheet = wb.active

        # Read header row to find column indices
        rows = list(sheet.iter_rows(values_only=True))
        # Find header row (contains "DUID")
        header_row = None
        header_idx = 0
        for i, row in enumerate(rows[:10]):
            if row and any(str(c).strip().upper() == "DUID" for c in row if c):
                header_row = [str(c).strip() if c else "" for c in row]
                header_idx = i
                break

        if not header_row:
            logger.warning("Could not find header row in AEMO registration list")
            return {}

        # Map column names to indices (case-insensitive)
        col = {h.lower(): i for i, h in enumerate(header_row)}
        logger.info(f"Registration sheet columns: {list(col.keys())[:20]}")

        # Find key columns
        def find_col(*names):
            for n in names:
                if n in col: return col[n]
            # Partial match
            for k, v in col.items():
                for n in names:
                    if n in k: return v
            return None

        duid_col     = find_col("duid")
        station_col  = find_col("station name", "station", "generating unit name", "name")
        region_col   = find_col("region", "regionid")
        fuel_col     = find_col("fuel source - primary", "fuel source", "fuel type", "primary fuel")
        tech_col     = find_col("technology type - primary", "technology type", "tech type")
        capacity_col = find_col("reg cap (mw)", "registered capacity", "capacity", "max cap", "reg cap")

        if duid_col is None:
            logger.warning("Could not find DUID column in registration list")
            return {}

        result = {}
        for row in rows[header_idx + 1:]:
            if not row or not row[duid_col]:
                continue
            duid = str(row[duid_col]).strip().upper()
            if not duid or duid == "NONE":
                continue

            station  = str(row[station_col]).strip()  if station_col  is not None and row[station_col]  else duid
            region   = str(row[region_col]).strip()   if region_col   is not None and row[region_col]   else ""
            fuel_raw = str(row[fuel_col]).strip().lower() if fuel_col is not None and row[fuel_col] else ""
            tech_raw = str(row[tech_col]).strip().lower() if tech_col is not None and row[tech_col] else ""

            # Normalise region
            if region and not region.endswith("1"):
                region = region + "1"
            if region not in ("QLD1", "NSW1", "VIC1", "SA1", "TAS1"):
                continue  # skip non-NEM regions

            # Normalise fuel
            fuel = None
            for key, val in _FUEL_MAP.items():
                if key in fuel_raw or key in tech_raw:
                    fuel = val
                    break
            # Battery override via tech
            if "battery" in tech_raw or "storage" in tech_raw:
                fuel = "Battery"
            if fuel is None:
                fuel = "Other"

            # Capacity
            cap = None
            if capacity_col is not None and row[capacity_col]:
                try:
                    cap = int(round(float(str(row[capacity_col]).replace(",", ""))))
                except (ValueError, TypeError):
                    pass

            result[duid] = {"station": station, "fuel": fuel, "region": region, "capacity": cap}

        logger.info(f"AEMO registration list: {len(result)} DUIDs parsed")
        return result

    except Exception as e:
        logger.warning(f"AEMO registration fetch failed: {e}")
        return {}


def _load_nem_units() -> dict:
    """Load DUID registry: try live AEMO registration list first, fall back to static JSON."""
    live = _fetch_aemo_registration()
    if len(live) > 100:
        # Merge static file on top to fill any gaps / override bad fuel mappings
        p = _Path(__file__).parent / "nem_units.json"
        try:
            static = _json.loads(p.read_text())
            # Static overrides live for DUIDs we've manually verified
            merged = {**live, **static}
            logger.info(f"NEM_UNITS: {len(live)} live + {len(static)} static overrides = {len(merged)} total")
            return merged
        except Exception:
            return live
    # Fallback to static
    logger.warning("Live AEMO registration failed — using static nem_units.json")
    p = _Path(__file__).parent / "nem_units.json"
    try:
        return _json.loads(p.read_text())
    except Exception as e:
        logger.warning(f"nem_units.json load failed: {e}")
        return {}


NEM_UNITS: dict = _load_nem_units()
logger.info(f"NEM_UNITS loaded: {len(NEM_UNITS)} DUIDs")

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

def _get(url: str, timeout: int = 15, retries: int = 2) -> Optional[requests.Response]:
    import time as _time
    for attempt in range(retries + 1):
        try:
            r = SESSION.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries:
                _time.sleep(0.5 * (attempt + 1))  # 0.5s, 1.0s backoff
                logger.debug(f"GET retry {attempt+1} {url}: {e}")
            else:
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
                  "DISPATCHABLEGENERATION","SEMISCHEDULE_CLEAREDMW","NETINTERCHANGE",
                  "DEMAND_AND_NONSCHEDGEN","TOTALINTERMITTENTGENERATION"]:
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
# Historical prices + demand from TradingIS CURRENT
# Each file is ~1.5KB and covers exactly ONE 30-min interval.
# Files are named PUBLIC_TRADINGIS_YYYYMMDDHHMM_<id>.zip
# Contains TRADING_PRICE (actual settled RRP) and TRADING_REGIONSUM (actual demand).
# SETTLEMENTDATE is end-of-interval — subtract 30min to get interval start.
# ---------------------------------------------------------------------------

def scrape_trading_history() -> dict:
    """
    Fetch today's TradingIS CURRENT files for price history.
    Each file covers ONE 30-min interval (~1.5KB).
    TRADING_PRICE uses INVALIDFLAG (not INTERVENTION).
    SETTLEMENTDATE is end-of-interval — subtract 30min.
    Returns { "prices": {region: [{interval, rrp}]} }
    """
    now_aest = datetime.now(AEST)
    today_str = now_aest.strftime("%Y%m%d")

    all_zips = _list_hrefs(TRADING_CURRENT)
    today_zips = sorted([u for u in all_zips if today_str in u])
    if not today_zips:
        today_zips = sorted(all_zips)[-48:]

    # TradingIS has ~5 files per 30-min interval (one per dispatch run).
    # Keep only the LAST file per HHMM timestamp to get the final settled price.
    # Filename format: PUBLIC_TRADINGIS_YYYYMMDDHHММ_<seq>.zip
    seen_hhmm = {}
    for url in today_zips:
        fname = url.split('/')[-1]
        parts = fname.split('_')
        # parts[2] is the datetime stamp e.g. 202603121335
        if len(parts) >= 3 and len(parts[2]) >= 12:
            hhmm = parts[2][8:12]  # extract HHMM
            seen_hhmm[hhmm] = url  # last one wins (sorted ascending)
    today_zips = sorted(seen_hhmm.values())

    prices: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    fetch_ok = 0
    fetch_fail = 0
    fetch_empty = 0
    now_aest   = datetime.now(AEST)
    today_date = now_aest.date()
    now_label  = now_aest.strftime("%H:%M")

    def fetch_one(url):
        import time as _time, random
        _time.sleep(random.uniform(0, 0.1))  # small jitter to avoid burst
        try:
            text = _read_zip(url)
            if not text:
                return [], "empty"
            pts = []
            for row in _parse_aemo(text, "TRADING_PRICE"):
                region = row.get("REGIONID", "")
                if region not in NEM_REGIONS:
                    continue
                # TradingIS uses INVALIDFLAG, not INTERVENTION
                if row.get("INVALIDFLAG", "0") not in ("0", ""):
                    continue
                dt_str = row.get("SETTLEMENTDATE", "")
                rrp_str = row.get("RRP", "")
                if not dt_str or not rrp_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                    # Only keep today's intervals, capped at now
                    if dt.date() != today_date:
                        continue
                    label = dt.strftime("%H:%M")
                    if label > now_label:
                        continue
                    pts.append((region, label, round(float(rrp_str), 2)))
                except (ValueError, TypeError):
                    pass
            return pts, "ok" if pts else "empty"
        except Exception as e:
            logger.warning(f"fetch_one failed {url}: {e}")
            return [], "fail"

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_one, u): u for u in today_zips}
        for fut in as_completed(futures):
            pts, status = fut.result()
            if status == "ok":
                fetch_ok += 1
            elif status == "fail":
                fetch_fail += 1
            else:
                fetch_empty += 1
            for region, label, val in pts:
                prices[region][label] = val

    price_result = {r: [{"interval": k, "rrp": v} for k, v in sorted(s.items())]
                    for r, s in prices.items() if s}

    logger.info(f"TradingIS prices: {sum(len(v) for v in price_result.values())} pts "
                f"from {len(today_zips)} files (ok={fetch_ok} empty={fetch_empty} fail={fetch_fail})")
    return {"prices": price_result, "fetch_stats": {"ok": fetch_ok, "empty": fetch_empty, "fail": fetch_fail}}


# ---------------------------------------------------------------------------
# Historical demand from DispatchIS CURRENT (5-min intervals)
# ---------------------------------------------------------------------------

def scrape_dispatch_history() -> dict:
    """
    Fetch today's DispatchIS files for 5-min demand AND price history in one pass.
    Returns {
        "demand": { region: [{interval, demand}] },
        "prices": { region: [{interval, rrp}] }   ← 5-min dispatch prices
    }
    These 5-min dispatch prices fill the ~1hr gap that TradingIS (30-min) lags behind.
    Cap at 300 files.
    """
    now_aest = datetime.now(AEST)
    today_str = now_aest.strftime("%Y%m%d")

    all_zips = _list_hrefs(DISPATCH_IS_URL)
    today_zips = [u for u in all_zips if today_str in u and "PUBLIC_DISPATCHIS" in u.upper()]
    if not today_zips:
        today_zips = all_zips[-288:]
    today_zips = sorted(today_zips)[-300:]

    demand: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    op_demand: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    prices: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    fetch_ok = fetch_fail = fetch_empty = 0
    now_aest   = datetime.now(AEST)
    today_date = now_aest.date()
    now_label  = now_aest.strftime("%H:%M")

    def fetch_one(url):
        import time as _time, random
        _time.sleep(random.uniform(0, 0.05))
        try:
            text = _read_zip(url)
            if not text:
                return [], "empty"
            pts = []
            # Extract demand from DISPATCH_REGIONSUM
            for row in _parse_aemo(text, "DISPATCH_REGIONSUM"):
                region = row.get("REGIONID", "")
                if region not in NEM_REGIONS:
                    continue
                if row.get("INTERVENTION", "0") not in ("0", ""):
                    continue
                dt_str = row.get("SETTLEMENTDATE", "")
                demand_str = row.get("TOTALDEMAND", "")
                op_demand_str = row.get("DEMAND_AND_NONSCHEDGEN", "")
                if not dt_str or not demand_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=5)
                    if dt.date() != today_date or dt.strftime("%H:%M") > now_label:
                        continue
                    label = dt.strftime("%H:%M")
                    pts.append(("demand", region, label, round(float(demand_str), 1)))
                    if op_demand_str:
                        pts.append(("op_demand", region, label, round(float(op_demand_str), 1)))
                except (ValueError, TypeError):
                    pass
            # Extract price from DISPATCH_PRICE
            for row in _parse_aemo(text, "DISPATCH_PRICE"):
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
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=5)
                    if dt.date() != today_date or dt.strftime("%H:%M") > now_label:
                        continue
                    pts.append(("price", region, dt.strftime("%H:%M"), round(float(rrp_str), 2)))
                except (ValueError, TypeError):
                    pass
            return pts, "ok" if pts else "empty"
        except Exception as e:
            logger.warning(f"dispatch_history fetch_one failed {url}: {e}")
            return [], "fail"

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_one, u): u for u in today_zips}
        for fut in as_completed(futures):
            pts, status = fut.result()
            if status == "ok":
                fetch_ok += 1
            elif status == "fail":
                fetch_fail += 1
            else:
                fetch_empty += 1
            for kind, region, label, val in pts:
                if kind == "demand":
                    demand[region][label] = val
                elif kind == "op_demand":
                    op_demand[region][label] = val
                else:
                    prices[region][label] = val

    demand_result = {r: [{"interval": k, "demand": v} for k, v in sorted(s.items())]
                     for r, s in demand.items() if s}
    op_demand_result = {r: [{"interval": k, "demand": v} for k, v in sorted(s.items())]
                        for r, s in op_demand.items() if s}
    price_result  = {r: [{"interval": k, "rrp": v}    for k, v in sorted(s.items())]
                     for r, s in prices.items() if s}

    logger.info(f"DispatchIS history: demand={sum(len(v) for v in demand_result.values())} pts, "
                f"prices={sum(len(v) for v in price_result.values())} pts "
                f"from {len(today_zips)} files (ok={fetch_ok} empty={fetch_empty} fail={fetch_fail})")
    return {"demand": demand_result, "op_demand": op_demand_result, "prices": price_result}


# Keep old name as alias for compatibility
def scrape_dispatch_demand_history() -> dict:
    return scrape_dispatch_history()["demand"]


# ---------------------------------------------------------------------------
# Predispatch
# ---------------------------------------------------------------------------

def _fetch_predispatch() -> str:
    url = get_latest_file_url(PREDISPATCH_URL, "PUBLIC_PREDISPATCHIS")
    return _read_zip(url) if url else ""


def scrape_predispatch_prices(text: str) -> dict:
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    today    = now_aest.date()
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
                # DATETIME is end-of-interval; shift back 30min for display
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                # Only keep today's intervals (AEST) that are >= now
                if dt.date() == today and dt >= now_aest:
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
    today    = now_aest.date()
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
                # DATETIME is end-of-interval; shift back 30min for display
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt.date() == today and dt >= now_aest:
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


def scrape_fuel_mix_live() -> dict:
    """
    Compute current fuel mix from SCADA output + registration list.
    Returns { region: { fuel: mw } } — a single snapshot (not history).
    Used as primary source; OpenNEM used for history if available.
    """
    try:
        scada = _fetch_full_scada()
        reg   = _load_registration_list()
        if not scada or not reg:
            return {}

        result: dict[str, dict[str, float]] = {r: {} for r in NEM_REGIONS}
        for duid, mw in scada.items():
            info = reg.get(duid, {})
            region = info.get("region", "")
            fuel   = info.get("fuel", "Other")
            if region not in NEM_REGIONS or mw is None or mw <= 0:
                continue
            result[region][fuel] = round(result[region].get(fuel, 0) + mw, 1)

        final = {r: v for r, v in result.items() if v}
        logger.info(f"Live fuel mix: {len(final)} regions")
        return final
    except Exception as e:
        logger.warning(f"Live fuel mix failed: {e}")
        return {}


def scrape_fuel_mix_history_opennem() -> dict:
    """
    Fetch fuel mix history from OpenNEM API.
    Returns { region: [ {interval, Black Coal, Gas, ...}, ... ] }
    Falls back to live SCADA snapshot if OpenNEM is unavailable.
    """
    try:
        url = f"{OPENNEM_API}/v4/stats/power/network/NEM?interval=5m&period=1d"
        r = _get(url, timeout=15)
        if not r or r.status_code != 200:
            raise ValueError(f"OpenNEM returned {r.status_code if r else 'no response'}")
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
            interval_mins = history.get("interval", 5)
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
        if final:
            logger.info(f"Fuel mix (OpenNEM): {sum(len(v) for v in final.values())} pts across {len(final)} regions")
            return final
        raise ValueError("OpenNEM returned empty data")
    except Exception as e:
        logger.warning(f"OpenNEM fuel mix failed: {e} — using live SCADA snapshot")
        # Fall back: wrap live snapshot as single-point history
        live = scrape_fuel_mix_live()
        now_label = datetime.now(AEST).strftime("%H:%M")
        return {r: [{"interval": now_label, **fuels}] for r, fuels in live.items()}


# ---------------------------------------------------------------------------
# In-memory demand history — lightweight fallback used only if TradingIS fetch fails
# ---------------------------------------------------------------------------

_demand_history: dict[str, dict] = {r: {} for r in NEM_REGIONS}

# ---------------------------------------------------------------------------
# In-memory fuel mix history — populated by scrape_gen every 15 min
# { region: { "HH:MM": { fuel: mw } } }
# Keyed by AEST time string; old entries pruned to keep only today's data.
# ---------------------------------------------------------------------------
_fuel_history: dict[str, dict] = {r: {} for r in NEM_REGIONS}

def _update_fuel_history(fuel_mix: dict) -> None:
    """Store a fuel mix snapshot. Prune entries from before midnight today."""
    label = datetime.now(AEST).strftime("%H:%M")
    today = datetime.now(AEST).date()
    for region in NEM_REGIONS:
        if region not in fuel_mix:
            continue
        _fuel_history[region][label] = dict(fuel_mix[region])
        # Prune old entries — only keep today (handles midnight rollover)
        # We can't compare HH:MM to dates easily, so just keep last 290 slots (24hrs @ 5min)
        if len(_fuel_history[region]) > 290:
            oldest = sorted(_fuel_history[region].keys())[0]
            del _fuel_history[region][oldest]

def _get_fuel_history() -> dict:
    result = {}
    for region, series in _fuel_history.items():
        if series:
            result[region] = [{"interval": k, **v} for k, v in sorted(series.items())]
    return result


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

    # Run all IO-bound fetches concurrently — prices/demand/history/predispatch only
    with ThreadPoolExecutor(max_workers=6) as ex:
        f_dispatch_is   = ex.submit(_fetch_dispatch_is)
        f_predispatch   = ex.submit(_fetch_predispatch)
        f_trading       = ex.submit(scrape_trading_history)
        f_dispatch_hist = ex.submit(scrape_dispatch_history)
        f_scada         = ex.submit(scrape_scada_duids, ORIGIN_DUIDS)

    dispatch_text    = f_dispatch_is.result()
    predispatch_text = f_predispatch.result()
    trading          = f_trading.result()
    dispatch_hist    = f_dispatch_hist.result()
    scada_vals       = f_scada.result()

    dispatch_demand       = dispatch_hist.get("demand", {})
    dispatch_op_demand    = dispatch_hist.get("op_demand", {})
    dispatch_price_5min   = dispatch_hist.get("prices", {})

    # Parse live dispatch snapshot (prices, demand, generation, ICs)
    region_summary  = scrape_region_summary(dispatch_text)
    interconnectors = scrape_interconnectors(dispatch_text)

    # Supplement SCADA with unit solution if needed
    missing = ORIGIN_DUIDS - set(scada_vals.keys())
    if missing:
        unit_sol = scrape_unit_solution(dispatch_text, missing)
        scada_vals.update(unit_sol)

    # Parse predispatch (future forecasts only)
    pd_prices = scrape_predispatch_prices(predispatch_text)
    pd_demand = scrape_predispatch_demand(predispatch_text)
    pd_gen    = scrape_predispatch_generation(predispatch_text)

    # In-memory accumulators — IC builds up over process lifetime
    _update_demand_history(region_summary)
    _update_ic_history(interconnectors)
    # Use DispatchIS history for demand (full day), fall back to in-memory if empty
    demand_history     = dispatch_demand if dispatch_demand else _get_demand_history()
    ic_history         = _get_ic_history()
    pd_interconnectors = scrape_predispatch_interconnectors(predispatch_text)

    # Live current values from latest dispatch interval
    prices     = {r: d["RRP"]         for r, d in region_summary.items() if "RRP" in d}
    demand     = {r: d["TOTALDEMAND"] for r, d in region_summary.items() if "TOTALDEMAND" in d}
    op_demand  = {r: d["DEMAND_AND_NONSCHEDGEN"] for r, d in region_summary.items() if "DEMAND_AND_NONSCHEDGEN" in d}
    generation = {r: {
        "Scheduled":      d.get("DISPATCHABLEGENERATION", 0),
        "Semi-Scheduled": d.get("SEMISCHEDULE_CLEAREDMW", 0),
        "Net Interchange":d.get("NETINTERCHANGE", 0),
    } for r, d in region_summary.items() if "DISPATCHABLEGENERATION" in d}

    # Build Origin assets output
    origin_assets_out = {}
    for duid, info in ORIGIN_ASSETS.items():
        mw = scada_vals.get(duid)
        origin_assets_out[duid] = {
            **info,
            "mw":     mw,
            "pct":    round(mw / info["capacity"] * 100, 1) if mw is not None and info["capacity"] else None,
            "status": "running" if (mw is not None and mw > 5) else ("off" if mw is not None else "unknown"),
        }

    # Keep trading (firm 30-min) and dispatch 5-min prices separate
    # so the frontend can style them differently
    trading_prices = trading["prices"]
    # Cap dispatch at now
    now_label = datetime.now(AEST).strftime("%H:%M")
    capped_dispatch_prices = {}
    for r in NEM_REGIONS:
        pts = [p for p in dispatch_price_5min.get(r, []) if p["interval"] <= now_label]
        if pts:
            capped_dispatch_prices[r] = pts

    logger.info(
        f"scrape_all done — prices:{list(prices.keys())} "
        f"trading_pts:{sum(len(v) for v in trading_prices.values())} "
        f"dispatch_5min_pts:{sum(len(v) for v in capped_dispatch_prices.values())} "
        f"origin_duids_found:{len(scada_vals)}"
    )

    return {
        "timestamp":             datetime.now(timezone.utc).isoformat(),
        "prices":                prices,
        "demand":                demand,
        "op_demand":             op_demand,
        "generation":            generation,
        "interconnectors":       interconnectors,
        "raw_summary":           region_summary,
        "historical_prices":     trading_prices,
        "dispatch_prices_5min":  capped_dispatch_prices,
        "price_fetch_stats":     trading.get("fetch_stats", {}),
        "predispatch_prices":    pd_prices,
        "demand_history":        demand_history,
        "op_demand_history":     dispatch_op_demand,
        "dispatch_history":      dispatch_demand,
        "predispatch_demand":    pd_demand,
        "predispatch_gen":       pd_gen,
        "ic_history":            ic_history,
        "predispatch_ic":        pd_interconnectors,
        "origin_assets":         origin_assets_out,
        "fuel_colors":           FUEL_COLORS,
        "all_fuels":             ALL_FUELS,
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
    Download and parse AEMO NEM Registration list from NEMWeb Static CSV.
    Falls back to the AEMO website XLS if CSV unavailable.
    Returns { DUID: {station, fuel, region, capacity_mw, participant} }
    Cached once per day.
    """
    global _reg_cache, _reg_cache_date
    today = datetime.now(AEST).strftime("%Y-%m-%d")
    if _reg_cache and _reg_cache_date == today:
        return _reg_cache

    # Primary: NEMWeb Generators and Scheduled Loads CSV (no auth, no XLS)
    CSV_URLS = [
        "https://www.nemweb.com.au/Reports/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
        "https://nemweb.com.au/Reports/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
        f"{NEMWEB_BASE}/Reports/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
    ]
    # Fallback: AEMO static file page (sometimes available without redirect)
    AEMO_URLS = [
        "https://aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/Current-Participants/NEM-Registration-and-Exemption-List.xls",
        AEMO_REG_LIST_URL,
    ]

    result = _try_load_nemweb_csv()
    if result:
        _reg_cache = result
        _reg_cache_date = today
        return result

    # Fallback: try AEMO XLS
    result = _try_load_aemo_xls()
    if result:
        _reg_cache = result
        _reg_cache_date = today
        return result

    logger.warning("Registration list: all sources failed, using empty cache")
    return _reg_cache


def _try_load_nemweb_csv() -> dict:
    """Try to load registration data from NEMWeb SEMP CSV."""
    # NEMWeb publishes Generators and Scheduled Loads via MMS tables
    # Try the Generators_and_Scheduled_Loads static file
    urls_to_try = [
        f"{NEMWEB_BASE}/Reports/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
        # Also try the NEM Registration list as a direct download from the data portal
        "https://data.wa.aemo.com.au/public/public-data/datafiles/facilities/facilities.csv",  # WA only, skip
    ]
    # The most reliable source: MMS DUDETAILSUMMARY via NEMWeb
    # Available as a static file updated daily
    url = f"{NEMWEB_BASE}/Reports/CURRENT/Ancillary_Services/PUBLIC_DVD_DUDETAILSUMMARY_202503120000.zip"

    # Actually use the correct approach: scrape the Generators listing page
    # NEMWeb has a static CSV at this well-known path:
    gen_csv_url = "https://www.nemweb.com.au/Reports/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV"
    r = _get(gen_csv_url, timeout=15)
    if r and r.status_code == 200 and len(r.content) > 1000:
        return _parse_registration_csv(r.text)
    return {}


def _parse_registration_csv(text: str) -> dict:
    """Parse a CSV registration file into {DUID: info} dict."""
    result = {}
    try:
        reader = csv.DictReader(io.StringIO(text))
        headers_upper = {k: k.upper() for k in (reader.fieldnames or [])}

        def get(row, *keys):
            for k in keys:
                for fk, fu in headers_upper.items():
                    if k.upper() in fu:
                        return str(row.get(fk, "") or "").strip()
            return ""

        for row in reader:
            duid = get(row, "DUID").upper()
            if not duid:
                continue
            region = get(row, "REGION", "REGIONID")
            if region and not region.endswith("1"):
                region = region + "1"
            fuel_raw = get(row, "FUEL SOURCE", "FUEL_SOURCE", "FUEL", "TECHNOLOGY")
            cap_str = get(row, "REG CAP", "REGISTERED_CAPACITY", "CAPACITY", "MAX_CAP")
            try:
                capacity = round(float(cap_str), 1) if cap_str else None
            except (ValueError, TypeError):
                capacity = None
            result[duid] = {
                "station":     get(row, "STATION NAME", "STATION", "STATIONNAME") or duid,
                "fuel":        _fuel_from_reg(fuel_raw),
                "fuel_raw":    fuel_raw,
                "region":      region,
                "capacity":    capacity,
                "participant": get(row, "PARTICIPANT", "PARTICIPANTID"),
            }
        logger.info(f"Registration CSV parsed: {len(result)} DUIDs")
    except Exception as e:
        logger.warning(f"Registration CSV parse failed: {e}")
    return result


def _try_load_aemo_xls() -> dict:
    """Try to load AEMO registration XLS using xlrd."""
    try:
        import xlrd
    except ImportError:
        logger.warning("xlrd not installed")
        return {}

    headers_to_try = {
        "User-Agent": "Mozilla/5.0 (compatible; NEM-Dashboard/1.0)",
        "Accept": "application/vnd.ms-excel,*/*",
        "Referer": "https://aemo.com.au/",
    }
    try:
        r = SESSION.get(AEMO_REG_LIST_URL, timeout=20,
                        headers=headers_to_try, allow_redirects=True)
        if not r or r.status_code != 200 or len(r.content) < 10000:
            logger.warning(f"AEMO XLS fetch failed: status={getattr(r,'status_code','?')} size={len(getattr(r,'content',b''))}")
            return {}

        logger.info(f"AEMO XLS: {len(r.content)} bytes, content-type={r.headers.get('Content-Type','?')}")
        wb = xlrd.open_workbook(file_contents=r.content)
        sheet = None
        for name in wb.sheet_names():
            if "generator" in name.lower() or "scheduled" in name.lower():
                sheet = wb.sheet_by_name(name)
                break
        if sheet is None:
            sheet = wb.sheet_by_index(0)

        # Find DUID header row
        header_row_idx = None
        for i in range(min(20, sheet.nrows)):
            cells = [str(sheet.cell_value(i, j)).upper() for j in range(sheet.ncols)]
            if any("DUID" in c for c in cells):
                header_row_idx = i
                logger.info(f"XLS header at row {i}: {cells[:8]}")
                break
        if header_row_idx is None:
            logger.warning("XLS: no DUID header found")
            return {}

        headers = [str(sheet.cell_value(header_row_idx, j)).strip().upper()
                   for j in range(sheet.ncols)]

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
        for i in range(header_row_idx + 1, sheet.nrows):
            if ci_duid is None:
                continue
            duid = str(sheet.cell_value(i, ci_duid)).strip().upper()
            if not duid or duid == "DUID":
                continue
            station  = str(sheet.cell_value(i, ci_station)).strip()  if ci_station  is not None else ""
            fuel_raw = str(sheet.cell_value(i, ci_fuel)).strip()     if ci_fuel     is not None else ""
            region   = str(sheet.cell_value(i, ci_region)).strip()   if ci_region   is not None else ""
            cap_raw  = sheet.cell_value(i, ci_capacity)               if ci_capacity is not None else None
            part     = str(sheet.cell_value(i, ci_part)).strip()     if ci_part     is not None else ""
            try:
                capacity = round(float(cap_raw), 1) if cap_raw not in (None, "") else None
            except (ValueError, TypeError):
                capacity = None
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
        logger.info(f"AEMO XLS loaded: {len(result)} DUIDs")
        return result

    except Exception as e:
        logger.warning(f"AEMO XLS load failed: {e}", exc_info=True)
        return {}




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
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "grouped":        grouped,
        "summary":        summary,
        "fuel_colors":    FUEL_COLORS,
        "reg_list_count": len(reg),
        "scada_count":    len(scada),
    }


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# scrape_scada_history — backfill 24hr fuel mix history from SCADA CURRENT files
# ---------------------------------------------------------------------------

def scrape_scada_history() -> None:
    """
    Fetch all of today's DISPATCH_SCADA files in parallel and populate
    _fuel_history with historical fuel mix snapshots.
    Called once at startup so the gen chart shows 24hr history immediately.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time as _time, random

    now_aest   = datetime.now(AEST)
    today_str  = now_aest.strftime("%Y%m%d")
    today_date = now_aest.date()
    now_label  = now_aest.strftime("%H:%M")

    all_urls = _list_hrefs(SCADA_URL)
    today_urls = sorted([u for u in all_urls if today_str in u and "PUBLIC_DISPATCHSCADA" in u.upper()])
    # Limit to 300 files (25hrs worth at 5min cadence)
    today_urls = today_urls[-300:]

    if not today_urls:
        logger.warning("scrape_scada_history: no SCADA files found for today")
        return

    logger.info(f"scrape_scada_history: fetching {len(today_urls)} SCADA files…")

    def fetch_one(url):
        _time.sleep(random.uniform(0, 0.03))
        try:
            text = _read_zip(url)
            if not text:
                return None
            # Each file: { HH:MM -> { duid: mw } }
            snapshot = {}
            for row in _parse_aemo(text, "DISPATCH_UNIT_SCADA"):
                duid = row.get("DUID", "").strip().upper()
                dt_str = row.get("SETTLEMENTDATE", "")
                v = row.get("SCADAVALUE", "")
                if not dt_str or not v:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=5)
                    if dt.date() != today_date or dt.strftime("%H:%M") > now_label:
                        continue
                    label = dt.strftime("%H:%M")
                    if label not in snapshot:
                        snapshot[label] = {}
                    snapshot[label][duid] = round(float(v), 1)
                except (ValueError, TypeError):
                    pass
            return snapshot
        except Exception as e:
            logger.debug(f"scrape_scada_history fetch error: {e}")
            return None

    # Parallel fetch
    all_snapshots = {}  # { HH:MM -> { duid: mw } }
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(fetch_one, u): u for u in today_urls}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                for label, duids in result.items():
                    if label not in all_snapshots:
                        all_snapshots[label] = {}
                    all_snapshots[label].update(duids)

    # Now convert to fuel_mix per timestamp and load into _fuel_history
    reg = NEM_UNITS
    loaded = 0
    for label in sorted(all_snapshots.keys()):
        duids = all_snapshots[label]
        fuel_mix: dict = {r: {} for r in NEM_REGIONS}
        for duid, mw in duids.items():
            info = reg.get(duid, {})
            region = info.get("region", "")
            fuel   = info.get("fuel", "Other")
            if region not in NEM_REGIONS:
                continue
            mw_pos = max(mw, 0) if mw is not None else 0
            fuel_mix[region][fuel] = round(fuel_mix[region].get(fuel, 0) + mw_pos, 1)
        # Store directly into _fuel_history keyed by label
        for region in NEM_REGIONS:
            if fuel_mix[region]:
                _fuel_history[region][label] = dict(fuel_mix[region])
        loaded += 1

    logger.info(f"scrape_scada_history: loaded {loaded} time slots into fuel history")


# scrape_gen — medium speed: fuel mix from SCADA + NEM_UNITS static registry
# Refreshed every 15 min.
# ---------------------------------------------------------------------------

def scrape_gen() -> dict:
    """
    SCADA-first: every DUID in DISPATCH_UNIT_SCADA is included.
    Registry (NEM_UNITS) enriches with station name / fuel / capacity.
    For unregistered DUIDs, region is inferred from CONNECTIONPOINTID
    (first letter: N=NSW1, Q=QLD1, V=VIC1, S=SA1, T=TAS1).
    """
    logger.info("scrape_gen starting...")

    # Fetch SCADA and latest DispatchIS in parallel
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_scada    = ex.submit(_fetch_full_scada)
        f_dispatch = ex.submit(_fetch_dispatch_is)
    scada         = f_scada.result()
    dispatch_text = f_dispatch.result()

    reg = NEM_UNITS

    # Build DUID->region from CONNECTIONPOINTID in DISPATCH_UNIT_SOLUTION
    _CPID_REGION = {"N": "NSW1", "Q": "QLD1", "V": "VIC1", "S": "SA1", "T": "TAS1"}
    cpid_map: dict = {}
    for row in _parse_aemo(dispatch_text, "DISPATCH_UNIT_SOLUTION"):
        duid = row.get("DUID", "").strip().upper()
        cpid = row.get("CONNECTIONPOINTID", "").strip().upper()
        if duid and cpid and cpid[0] in _CPID_REGION:
            cpid_map[duid] = _CPID_REGION[cpid[0]]

    fuel_mix:   dict = {r: {} for r in NEM_REGIONS}
    nem_totals: dict = {}
    grouped:    dict = {}
    unmatched_log: list = []

    for duid, mw in scada.items():
        info     = reg.get(duid.upper(), {})
        region   = info.get("region", "") or cpid_map.get(duid.upper(), "")
        fuel     = info.get("fuel", "Other")
        station  = info.get("station", duid)
        capacity = info.get("capacity")

        if region not in NEM_REGIONS:
            continue

        mw_val = mw if mw is not None else 0
        mw_pos = max(mw_val, 0)

        fuel_mix[region][fuel] = round(fuel_mix[region].get(fuel, 0) + mw_pos, 1)
        nem_totals[fuel]       = round(nem_totals.get(fuel, 0) + mw_pos, 1)

        pct = round(mw_val / capacity * 100, 1) if (mw_val and capacity and capacity > 0) else None
        grouped.setdefault(region, {}).setdefault(fuel, []).append({
            "duid":     duid,
            "station":  station,
            "mw":       round(mw_val, 1),
            "capacity": capacity,
            "pct":      pct,
            "matched":  bool(info),
        })

        if not info:
            unmatched_log.append((duid, mw_val))

    # Sort units within each fuel group by MW desc
    for region in grouped:
        for fuel in grouped[region]:
            grouped[region][fuel].sort(key=lambda x: x["mw"] or 0, reverse=True)

    if unmatched_log:
        top = sorted(unmatched_log, key=lambda x: x[1] or 0, reverse=True)[:20]
        logger.info(f"scrape_gen: {len(unmatched_log)} SCADA DUIDs region-inferred (no registry entry): "
                    + ", ".join(f"{d}={mw:.0f}MW" for d, mw in top if mw and mw > 1))

    # Accumulate into in-memory history
    _update_fuel_history(fuel_mix)

    logger.info(f"scrape_gen done — {len(scada)} SCADA DUIDs, "
                f"reg={len(reg)}, buckets={sum(len(v) for v in fuel_mix.values())}")
    return {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "fuel_mix":      fuel_mix,
        "fuel_history":  _get_fuel_history(),
        "nem_totals":    nem_totals,
        "grouped":       grouped,
        "fuel_colors":   FUEL_COLORS,
        "all_fuels":     ALL_FUELS,
        "scada_count":   len(scada),
        "reg_count":     len(reg),
    }


# scrape_slow — background fetch for generators + week-ahead
# ---------------------------------------------------------------------------

def scrape_slow() -> dict:
    """
    Week-ahead ST PASA demand forecast only.
    Generators/fuel mix now handled by scrape_gen (medium cache).
    """
    logger.info("scrape_slow starting...")
    pasa = scrape_stpasa_demand()
    logger.info("scrape_slow done")
    return {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "stpasa_demand": pasa,
        "fuel_colors":   FUEL_COLORS,
        "all_fuels":     ALL_FUELS,
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
