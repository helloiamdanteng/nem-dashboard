"""
NEMWeb scraper for AEMO NEM data.

AEMO CSV format notes:
- Each file is a multi-table CSV
- C rows = comments/file info
- I rows = column headers for the NEXT table (cols: indicator, table_name, sub_table, version, col1, col2, ...)
- D rows = data rows matching the most recent I row headers
- Files have a trailing comma so last CSV column is always empty — we strip it

Data sources:
- CURRENT/DispatchIS_Reports  -> latest 5-min dispatch prices, demand, generation, interconnectors
- ARCHIVE/TradingIS_Reports   -> today's 30-min trading prices (historical)
- CURRENT/PredispatchIS_Reports -> predispatch prices for rest of day
"""

import re
import requests
import zipfile
import io
import csv
import logging
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

NEMWEB_BASE = "https://www.nemweb.com.au"
DISPATCH_IS_URL    = f"{NEMWEB_BASE}/Reports/CURRENT/DispatchIS_Reports/"
PREDISPATCH_URL    = f"{NEMWEB_BASE}/Reports/CURRENT/PredispatchIS_Reports/"
# Archive has per-day subdirectories: /YYYY/MM/DD/
TRADING_ARCHIVE_BASE = f"{NEMWEB_BASE}/Reports/ARCHIVE/TradingIS_Reports"

AEST = ZoneInfo("Australia/Sydney")
NEM_REGIONS = ["QLD1", "NSW1", "VIC1", "SA1", "TAS1"]


# ---------------------------------------------------------------------------
# Core HTTP + ZIP helpers
# ---------------------------------------------------------------------------

def _get(url: str, timeout: int = 20) -> Optional[requests.Response]:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        logger.warning(f"GET failed {url}: {e}")
        return None


def _list_hrefs(url: str, suffix: str = ".zip") -> list[str]:
    """Return sorted absolute URLs of all hrefs ending with `suffix` from a directory listing."""
    r = _get(url)
    if not r:
        return []
    found = []
    for m in re.finditer(r'href="([^"]+)"', r.text, re.IGNORECASE):
        href = m.group(1)
        if not href.lower().endswith(suffix.lower()):
            continue
        if href.startswith("http"):
            found.append(href)
        elif href.startswith("/"):
            found.append(f"{NEMWEB_BASE}{href}")
        else:
            found.append(url.rstrip("/") + "/" + href)
    return sorted(set(found))


def _read_zip_csv(url: str) -> str:
    """Download a ZIP and return the text of the first CSV inside."""
    r = _get(url, timeout=40)
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
# AEMO multi-table CSV parser
#
# AEMO files look like:
#   C,NEMSOLUTION,... (comment/header rows)
#   I,DISPATCH,PRICE,4,SETTLEMENTDATE,RUNNO,REGIONID,...  <- header row
#   D,DISPATCH,PRICE,4,2026/03/11 10:00:00,1,NSW1,...     <- data row
#   I,DISPATCH,REGIONSUM,5,...
#   D,DISPATCH,REGIONSUM,5,...
#
# The "table key" we match on is cols[1] + "_" + cols[2]  (e.g. "DISPATCH_PRICE")
# ---------------------------------------------------------------------------

def _parse_aemo(text: str, table_key: str) -> list[dict]:
    """
    Parse AEMO multi-table CSV and return data rows for `table_key`.
    table_key is matched against  f"{col1}_{col2}"  (case-insensitive, partial match ok).
    Returns list of dicts with UPPERCASE keys.
    """
    results = []
    headers: list[str] = []
    in_table = False

    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row:
            continue
        indicator = row[0].strip().upper()

        if indicator == "I":
            # cols: I, table, subtable, version, col1, col2, ...
            if len(row) < 5:
                continue
            key = f"{row[1].strip()}_{row[2].strip()}".upper()
            if table_key.upper() in key:
                # Build header list from col index 4 onwards, drop trailing empty
                headers = [c.strip().upper() for c in row[4:] if c.strip()]
                in_table = True
            else:
                in_table = False

        elif indicator == "D" and in_table and headers:
            # Data cols start at index 4
            data_cols = row[4:]
            # Pad or trim to match headers length
            data_cols = data_cols[:len(headers)]
            while len(data_cols) < len(headers):
                data_cols.append("")
            results.append(dict(zip(headers, [c.strip() for c in data_cols])))

    return results


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

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
# Current dispatch data (prices, demand, generation, interconnectors)
# ---------------------------------------------------------------------------

def scrape_region_summary() -> dict:
    url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
    if not url:
        logger.warning("No DISPATCHIS file found")
        return {}

    text = _read_zip_csv(url)
    summary: dict[str, dict] = {}

    # DISPATCH_PRICE table
    for row in _parse_aemo(text, "DISPATCH_PRICE"):
        region = row.get("REGIONID", "").strip()
        if region not in NEM_REGIONS:
            continue
        entry = summary.setdefault(region, {})
        for f in ["RRP", "RAISE6SECRRP", "LOWER6SECRRP"]:
            v = row.get(f, "")
            if v:
                try:
                    entry[f] = round(float(v), 2)
                except ValueError:
                    pass

    # DISPATCH_REGIONSUM table
    for row in _parse_aemo(text, "DISPATCH_REGIONSUM"):
        region = row.get("REGIONID", "").strip()
        if region not in NEM_REGIONS:
            continue
        entry = summary.setdefault(region, {})
        for f in ["TOTALDEMAND", "DEMANDFORECAST", "INITIALSUPPLY",
                  "DISPATCHABLEGENERATION", "SEMISCHEDULEDGENERATION", "NETINTERCHANGE"]:
            v = row.get(f, "")
            if v:
                try:
                    entry[f] = round(float(v), 1)
                except ValueError:
                    pass

    return summary


def scrape_interconnectors() -> dict:
    url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
    if not url:
        return {}

    text = _read_zip_csv(url)
    flows = {}

    for row in _parse_aemo(text, "DISPATCH_INTERCONNECTORRES"):
        ic = row.get("INTERCONNECTORID", "").strip()
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
# Historical trading prices — from ARCHIVE (today's 30-min intervals)
# ---------------------------------------------------------------------------

def scrape_trading_prices_today() -> dict:
    """
    Fetch today's 30-min trading interval prices from the ARCHIVE.
    URL pattern: /Reports/ARCHIVE/TradingIS_Reports/PUBLIC_TRADINGIS_YYYYMMDD.zip
    which contains multiple interval files, OR a per-day directory listing.
    We try the archive directory for today's date.
    """
    now_aest = datetime.now(AEST)
    today_str = now_aest.strftime("%Y%m%d")

    # Try archive directory — AEMO archives are at a dated subfolder or a dated zip
    archive_url = f"{TRADING_ARCHIVE_BASE}/"
    all_zips = _list_hrefs(archive_url)
    logger.info(f"Trading archive total zips: {len(all_zips)}")

    # Filter to today — filename contains today's date
    today_zips = [u for u in all_zips if today_str in u]
    logger.info(f"Trading archive today zips: {len(today_zips)} for {today_str}")

    # If none found for today, try yesterday (data may lag)
    if not today_zips:
        yesterday_str = (now_aest.replace(hour=0, minute=0, second=0, microsecond=0)
                         .__class__(now_aest.year, now_aest.month, now_aest.day,
                                    tzinfo=AEST) ).__class__
        # simpler:
        from datetime import timedelta
        yesterday = (now_aest - timedelta(days=1)).strftime("%Y%m%d")
        today_zips = [u for u in all_zips if yesterday in u]
        logger.info(f"Falling back to yesterday {yesterday}: {len(today_zips)} zips")

    if not today_zips:
        # Last resort: use the single CURRENT file
        today_zips = all_zips[-1:] if all_zips else []

    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}

    for url in today_zips:
        text = _read_zip_csv(url)
        for row in _parse_aemo(text, "TRADING_PRICE"):
            region = row.get("REGIONID", "").strip()
            if region not in NEM_REGIONS:
                continue
            dt_str = row.get("SETTLEMENTDATE", "").strip()
            rrp_str = row.get("RRP", "").strip()
            if not dt_str or not rrp_str:
                continue
            try:
                rrp = round(float(rrp_str), 2)
                dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                label = dt.strftime("%H:%M")
                region_series[region][label] = rrp
            except (ValueError, TypeError):
                pass

    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "rrp": v}
                               for k, v in sorted(series.items())]
    logger.info(f"Historical prices: {sum(len(v) for v in result.values())} points across {len(result)} regions")
    return result


# ---------------------------------------------------------------------------
# Predispatch prices — rest of day forecast
# ---------------------------------------------------------------------------

def scrape_predispatch_prices() -> dict:
    url = get_latest_file_url(PREDISPATCH_URL, "PUBLIC_PREDISPATCHIS")
    if not url:
        logger.warning("No predispatch file found")
        return {}

    text = _read_zip_csv(url)
    now_aest = datetime.now(AEST).replace(tzinfo=None)

    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}

    # Try PREDISPATCH_REGION_PRICES first, then PREDISPATCH_PRICE
    for table_key in ["PREDISPATCH_REGION_PRICES", "PREDISPATCH_PRICE", "PREDISPATCH_REGIONPRICE"]:
        rows = _parse_aemo(text, table_key)
        if rows:
            logger.info(f"Predispatch using table key: {table_key}, rows: {len(rows)}")
            for row in rows:
                region = row.get("REGIONID", "").strip()
                if region not in NEM_REGIONS:
                    continue
                dt_str = row.get("DATETIME", row.get("SETTLEMENTDATE", "")).strip()
                rrp_str = row.get("RRP", "").strip()
                if not dt_str or not rrp_str:
                    continue
                try:
                    rrp = round(float(rrp_str), 2)
                    dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                    if dt.replace(tzinfo=None) >= now_aest:
                        label = dt.strftime("%H:%M")
                        region_series[region][label] = rrp
                except (ValueError, TypeError):
                    pass
            break  # stop after first successful table

    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "rrp": v}
                               for k, v in sorted(series.items())]
    logger.info(f"Predispatch prices: {sum(len(v) for v in result.values())} points across {len(result)} regions")
    return result


# ---------------------------------------------------------------------------
# Predispatch demand — rest of day demand forecast
# ---------------------------------------------------------------------------

def scrape_predispatch_demand() -> dict:
    """Fetch predispatch demand forecast for rest of day."""
    url = get_latest_file_url(PREDISPATCH_URL, "PUBLIC_PREDISPATCHIS")
    if not url:
        return {}

    text = _read_zip_csv(url)
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}

    for table_key in ["PREDISPATCH_REGION_SOLUTION", "PREDISPATCH_REGIONSOLUTION"]:
        rows = _parse_aemo(text, table_key)
        if rows:
            logger.info(f"Predispatch demand using table: {table_key}, rows: {len(rows)}")
            for row in rows:
                region = row.get("REGIONID", "").strip()
                if region not in NEM_REGIONS:
                    continue
                dt_str = row.get("DATETIME", row.get("SETTLEMENTDATE", "")).strip()
                demand_str = row.get("TOTALDEMAND", row.get("DEMAND", "")).strip()
                if not dt_str or not demand_str:
                    continue
                try:
                    demand = round(float(demand_str), 1)
                    dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                    if dt.replace(tzinfo=None) >= now_aest:
                        label = dt.strftime("%H:%M")
                        region_series[region][label] = demand
                except (ValueError, TypeError):
                    pass
            break

    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "demand": v}
                               for k, v in sorted(series.items())]
    logger.info(f"Predispatch demand: {sum(len(v) for v in result.values())} points across {len(result)} regions")
    return result


# ---------------------------------------------------------------------------
# Historical demand — from today's dispatch intervals (5-min, stored in cache)
# ---------------------------------------------------------------------------

_dispatch_demand_history: dict[str, dict] = {r: {} for r in NEM_REGIONS}


def update_dispatch_demand_history(region_summary: dict) -> None:
    """Append current dispatch demand snapshot to in-memory history."""
    now_aest = datetime.now(AEST)
    label = now_aest.strftime("%H:%M")
    for region, data in region_summary.items():
        if region in NEM_REGIONS and "TOTALDEMAND" in data:
            _dispatch_demand_history[region][label] = data["TOTALDEMAND"]


def get_demand_history() -> dict:
    result = {}
    for region, series in _dispatch_demand_history.items():
        if series:
            result[region] = [{"interval": k, "demand": v}
                               for k, v in sorted(series.items())]
    return result


# ---------------------------------------------------------------------------
# Main scrape_all
# ---------------------------------------------------------------------------

def scrape_all() -> dict:
    logger.info("Starting NEMWeb scrape...")

    region_summary   = scrape_region_summary()
    interconnectors  = scrape_interconnectors()
    historical_prices = scrape_trading_prices_today()
    predispatch_prices = scrape_predispatch_prices()
    predispatch_demand = scrape_predispatch_demand()

    # Snapshot current demand into history
    update_dispatch_demand_history(region_summary)
    demand_history = get_demand_history()

    prices     = {}
    demand     = {}
    generation = {}

    for region, data in region_summary.items():
        if "RRP" in data:
            prices[region] = data["RRP"]
        if "TOTALDEMAND" in data:
            demand[region] = data["TOTALDEMAND"]
        if "DISPATCHABLEGENERATION" in data:
            generation[region] = {
                "Scheduled":      data.get("DISPATCHABLEGENERATION", 0),
                "Semi-Scheduled": data.get("SEMISCHEDULEDGENERATION", 0),
                "Net Interchange": data.get("NETINTERCHANGE", 0),
            }

    logger.info(
        f"Scrape done — prices: {list(prices.keys())}, "
        f"hist_price pts: {sum(len(v) for v in historical_prices.values())}, "
        f"pd_price pts: {sum(len(v) for v in predispatch_prices.values())}, "
        f"demand_hist pts: {sum(len(v) for v in demand_history.values())}, "
        f"pd_demand pts: {sum(len(v) for v in predispatch_demand.values())}"
    )

    return {
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "prices":             prices,
        "demand":             demand,
        "generation":         generation,
        "interconnectors":    interconnectors,
        "raw_summary":        region_summary,
        "historical_prices":  historical_prices,
        "predispatch_prices": predispatch_prices,
        "demand_history":     demand_history,
        "predispatch_demand": predispatch_demand,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    data = scrape_all()
    print(json.dumps({
        "prices": data["prices"],
        "historical_price_counts": {r: len(v) for r, v in data["historical_prices"].items()},
        "predispatch_price_counts": {r: len(v) for r, v in data["predispatch_prices"].items()},
        "demand_history_counts": {r: len(v) for r, v in data["demand_history"].items()},
        "predispatch_demand_counts": {r: len(v) for r, v in data["predispatch_demand"].items()},
    }, indent=2))
