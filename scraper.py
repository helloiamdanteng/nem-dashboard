"""
NEMWeb scraper for AEMO NEM data.
Fetches: Spot prices (RRP), Generation by fuel type, Demand/load, Interconnector flows
"""

import requests
import zipfile
import io
import csv
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

NEMWEB_BASE = "https://www.nemweb.com.au"

DISPATCH_URL = f"{NEMWEB_BASE}/Reports/CURRENT/Dispatch_SCADA/"
DISPATCH_IS_URL = f"{NEMWEB_BASE}/Reports/CURRENT/DispatchIS_Reports/"

NEM_REGIONS = ["QLD1", "NSW1", "VIC1", "SA1", "TAS1"]

INTERCONNECTORS = [
    "N-Q-MNSP1",  # NSW-QLD
    "NSW1-QLD1",  # NSW-QLD
    "VIC1-NSW1",  # VIC-NSW
    "V-SA",       # VIC-SA
    "V-S-MNSP1",  # VIC-SA
    "T-V-MNSP1",  # TAS-VIC
]

FUEL_TYPES = {
    "Black Coal": ["BLACK_COAL", "COAL"],
    "Brown Coal": ["BROWN_COAL"],
    "Gas": ["GAS", "CCGT", "OCGT", "GAS_CCGT", "GAS_OCGT", "GAS_STEAM", "GAS_RECIP"],
    "Hydro": ["HYDRO"],
    "Wind": ["WIND"],
    "Solar": ["SOLAR", "SOLAR_ROOFTOP"],
    "Liquid": ["LIQUID"],
    "Battery": ["BATTERY_DISCHARGING", "BATTERY"],
    "Pumps": ["PUMP"],
    "Other": [],
}


def get_latest_file_url(directory_url: str, prefix: str = "") -> Optional[str]:
    """Fetch directory listing and return URL of latest matching file."""
    try:
        resp = requests.get(directory_url, timeout=15)
        resp.raise_for_status()
        lines = resp.text.split("\n")
        files = []
        for line in lines:
            if ".zip" in line.lower() or ".csv" in line.lower():
                import re
                match = re.search(r'href="([^"]*\.(?:zip|csv|ZIP|CSV))"', line, re.IGNORECASE)
                if match:
                    fname = match.group(1)
                    if prefix and prefix.lower() not in fname.lower():
                        continue
                    files.append(fname)
        if not files:
            return None
        files.sort()
        latest = files[-1]
        if latest.startswith("http"):
            return latest
        if latest.startswith("/"):
            return f"{NEMWEB_BASE}{latest}"
        return f"{directory_url}{latest}"
    except Exception as e:
        logger.error(f"Error fetching directory {directory_url}: {e}")
        return None


def fetch_zip_csv(url: str) -> list[dict]:
    """Download a zip file and return rows from the first CSV inside."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            if not csv_files:
                return []
            with z.open(csv_files[0]) as f:
                content = f.read().decode("utf-8", errors="replace")
                reader = csv.DictReader(io.StringIO(content))
                return list(reader)
    except Exception as e:
        logger.error(f"Error fetching zip {url}: {e}")
        return []


def parse_dispatch_price(rows: list[dict]) -> dict:
    """Extract RRP by region from DISPATCHPRICE rows."""
    prices = {}
    for row in rows:
        row_id = row.get("I", row.get("", ""))
        record_type = None
        # AEMO CSV format: first col is I/C/D, second is table name
        cols = list(row.values())
        if len(cols) >= 2:
            record_type = cols[1] if cols[0] in ("D", "I") else None

        if "REGIONID" in row and "RRP" in row:
            region = row.get("REGIONID", "").strip()
            try:
                rrp = float(row.get("RRP", 0))
                if region in NEM_REGIONS:
                    prices[region] = round(rrp, 2)
            except (ValueError, TypeError):
                pass
    return prices


def scrape_dispatch_prices() -> dict:
    """Scrape latest dispatch prices for all NEM regions."""
    try:
        url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
        if not url:
            logger.warning("Could not find DISPATCHIS file")
            return {}

        rows = fetch_zip_csv(url)
        prices = {}

        for row in rows:
            vals = list(row.values())
            # Look for DISPATCHPRICE data rows
            if len(vals) > 5:
                # Try to find REGIONID and RRP columns
                row_str = ",".join(str(v) for v in vals)
                if "DISPATCHPRICE" in row_str or ("RRP" in row and "REGIONID" in row):
                    region = row.get("REGIONID", "").strip()
                    rrp_val = row.get("RRP", "")
                    if region in NEM_REGIONS and rrp_val:
                        try:
                            prices[region] = round(float(rrp_val), 2)
                        except (ValueError, TypeError):
                            pass

        # Fallback: parse using positional approach
        if not prices:
            prices = _parse_aemo_csv_for_prices(rows)

        return prices
    except Exception as e:
        logger.error(f"Error scraping dispatch prices: {e}")
        return {}


def _parse_aemo_csv_for_prices(rows: list[dict]) -> dict:
    """Parse AEMO-format CSV for prices using header detection."""
    prices = {}
    headers = None

    for row in rows:
        vals = list(row.values())
        if not vals:
            continue

        # AEMO CSV uses I rows for headers, D rows for data, C for comments
        record_indicator = str(vals[0]).strip().upper()

        if record_indicator == "I":
            headers = [str(v).strip().upper() for v in vals]
        elif record_indicator == "D" and headers:
            row_dict = dict(zip(headers, vals))
            if row_dict.get("") in ("DISPATCHPRICE", "PRICE") or "RRP" in row_dict:
                region = row_dict.get("REGIONID", "").strip()
                rrp_str = row_dict.get("RRP", "")
                if region in NEM_REGIONS and rrp_str:
                    try:
                        prices[region] = round(float(rrp_str), 2)
                    except (ValueError, TypeError):
                        pass
    return prices


def scrape_generation_by_fuel() -> dict:
    """Scrape generation data from DISPATCH_SCADA."""
    try:
        # Use ROOFTOP_PV and generation summary from DispatchIS
        url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
        if not url:
            return {}

        rows = fetch_zip_csv(url)
        generation = {region: {} for region in NEM_REGIONS}
        headers = None

        for row in rows:
            vals = list(row.values())
            if not vals:
                continue
            record_indicator = str(vals[0]).strip().upper()

            if record_indicator == "I":
                headers = [str(v).strip().upper() for v in vals]
            elif record_indicator == "D" and headers:
                row_dict = dict(zip(headers, vals))
                # Look for DISPATCHREGIONSUM for total generation
                table = str(vals[1]).strip().upper() if len(vals) > 1 else ""
                if "REGIONSUM" in table or "DISPATCHREGIONSUM" in table:
                    region = row_dict.get("REGIONID", "").strip()
                    if region in NEM_REGIONS:
                        try:
                            total_gen = float(row_dict.get("TOTALDEMAND", 0) or 0)
                            generation[region]["Total"] = round(total_gen, 1)
                        except (ValueError, TypeError):
                            pass

        return generation
    except Exception as e:
        logger.error(f"Error scraping generation: {e}")
        return {}


def scrape_region_summary() -> dict:
    """Scrape DISPATCHREGIONSUM for demand, generation, price data."""
    try:
        url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
        if not url:
            logger.warning("No DISPATCHIS URL found")
            return {}

        rows = fetch_zip_csv(url)
        summary = {}
        headers = None
        current_table = None

        for row in rows:
            vals = list(row.values())
            if not vals:
                continue
            record_indicator = str(vals[0]).strip().upper()

            if record_indicator == "I" and len(vals) > 1:
                current_table = str(vals[1]).strip().upper()
                headers = [str(v).strip().upper() for v in vals]

            elif record_indicator == "D" and headers:
                row_dict = dict(zip(headers, vals))
                table = current_table or ""

                if "REGIONSUM" in table:
                    region = row_dict.get("REGIONID", "").strip()
                    if region in NEM_REGIONS:
                        entry = summary.setdefault(region, {})
                        for field in ["TOTALDEMAND", "DEMANDFORECAST", "INITIALSUPPLY",
                                      "DISPATCHABLEGENERATION", "SEMISCHEDULEDGENERATION",
                                      "NETINTERCHANGE", "LOWER5MINDISPATCH", "RAISE5MINDISPATCH"]:
                            val = row_dict.get(field, "")
                            if val:
                                try:
                                    entry[field] = round(float(val), 1)
                                except (ValueError, TypeError):
                                    pass

                elif "DISPATCHPRICE" in table or table == "PRICE":
                    region = row_dict.get("REGIONID", "").strip()
                    if region in NEM_REGIONS:
                        entry = summary.setdefault(region, {})
                        rrp = row_dict.get("RRP", "")
                        if rrp:
                            try:
                                entry["RRP"] = round(float(rrp), 2)
                            except (ValueError, TypeError):
                                pass

        return summary
    except Exception as e:
        logger.error(f"Error scraping region summary: {e}")
        return {}


def scrape_interconnectors() -> dict:
    """Scrape interconnector flows."""
    try:
        url = get_latest_file_url(DISPATCH_IS_URL, "PUBLIC_DISPATCHIS")
        if not url:
            return {}

        rows = fetch_zip_csv(url)
        flows = {}
        headers = None
        current_table = None

        for row in rows:
            vals = list(row.values())
            if not vals:
                continue
            record_indicator = str(vals[0]).strip().upper()

            if record_indicator == "I" and len(vals) > 1:
                current_table = str(vals[1]).strip().upper()
                headers = [str(v).strip().upper() for v in vals]

            elif record_indicator == "D" and headers and current_table:
                if "INTERCONNECTOR" in current_table and "RES" in current_table:
                    row_dict = dict(zip(headers, vals))
                    ic_id = row_dict.get("INTERCONNECTORID", "").strip()
                    if ic_id:
                        mw_flow = row_dict.get("MWFLOW", "")
                        mw_losses = row_dict.get("MWLOSSES", "")
                        try:
                            flows[ic_id] = {
                                "flow": round(float(mw_flow), 1) if mw_flow else 0,
                                "losses": round(float(mw_losses), 1) if mw_losses else 0,
                            }
                        except (ValueError, TypeError):
                            pass

        return flows
    except Exception as e:
        logger.error(f"Error scraping interconnectors: {e}")
        return {}


TRADING_PRICE_URL = f"{NEMWEB_BASE}/Reports/CURRENT/TradingIS_Reports/"
PREDISPATCH_URL = f"{NEMWEB_BASE}/Reports/CURRENT/PredispatchIS_Reports/"


def get_all_file_urls(directory_url: str, prefix: str = "") -> list[str]:
    """Fetch directory listing and return URLs of ALL matching files, sorted."""
    import re
    try:
        resp = requests.get(directory_url, timeout=15)
        resp.raise_for_status()
        files = []
        for line in resp.text.split("\n"):
            if ".zip" not in line.lower() and ".csv" not in line.lower():
                continue
            match = re.search(r'href="([^"]*\.(?:zip|csv|ZIP|CSV))"', line, re.IGNORECASE)
            if match:
                fname = match.group(1)
                if prefix and prefix.lower() not in fname.lower():
                    continue
                if fname.startswith("http"):
                    files.append(fname)
                elif fname.startswith("/"):
                    files.append(f"{NEMWEB_BASE}{fname}")
                else:
                    files.append(f"{directory_url}{fname}")
        files.sort()
        return files
    except Exception as e:
        logger.error(f"Error fetching directory {directory_url}: {e}")
        return []


def parse_aemo_csv(rows: list[dict], target_table: str, fields: list[str]) -> list[dict]:
    """Generic AEMO CSV parser. Returns list of dicts for matching table rows."""
    results = []
    headers = None
    current_table = None

    for row in rows:
        vals = list(row.values())
        if not vals:
            continue
        indicator = str(vals[0]).strip().upper()

        if indicator == "I" and len(vals) > 1:
            current_table = str(vals[1]).strip().upper()
            headers = [str(v).strip().upper() for v in vals]
        elif indicator == "D" and headers and current_table:
            if target_table.upper() in current_table:
                row_dict = dict(zip(headers, vals))
                entry = {}
                for f in fields:
                    entry[f] = row_dict.get(f.upper(), "")
                results.append(entry)
    return results


def scrape_trading_prices_today() -> dict:
    """
    Scrape today's 30-min trading interval prices from TradingIS reports.
    Returns: { region: [ {interval: "HH:MM", rrp: float}, ... ] }
    """
    try:
        urls = get_all_file_urls(TRADING_PRICE_URL, "PUBLIC_TRADINGIS")
        if not urls:
            logger.warning("No TradingIS files found")
            return {}

        # Get AEST date for today
        from zoneinfo import ZoneInfo
        aest = ZoneInfo("Australia/Sydney")
        today = datetime.now(aest).strftime("%Y/%m/%d")
        today_compact = datetime.now(aest).strftime("%Y%m%d")

        region_series: dict[str, list] = {r: [] for r in NEM_REGIONS}

        for url in urls:
            # Only process today's files (filename contains today's date)
            if today_compact not in url:
                continue
            rows = fetch_zip_csv(url)
            entries = parse_aemo_csv(rows, "TRADINGPRICE", ["SETTLEMENTDATE", "REGIONID", "RRP"])
            for e in entries:
                region = e.get("REGIONID", "").strip()
                if region not in NEM_REGIONS:
                    continue
                dt_str = e.get("SETTLEMENTDATE", "").strip()
                rrp_str = e.get("RRP", "").strip()
                if not dt_str or not rrp_str:
                    continue
                try:
                    rrp = round(float(rrp_str), 2)
                    # Parse datetime - AEMO format: "YYYY/MM/DD HH:MM:SS"
                    dt_str_clean = dt_str.replace("/", "-")
                    dt = datetime.fromisoformat(dt_str_clean)
                    interval_label = dt.strftime("%H:%M")
                    region_series[region].append({"interval": interval_label, "rrp": rrp, "sort_dt": dt_str})
                except (ValueError, TypeError):
                    pass

        # Sort and deduplicate by interval
        result = {}
        for region, series in region_series.items():
            seen = {}
            for pt in sorted(series, key=lambda x: x["sort_dt"]):
                seen[pt["interval"]] = pt["rrp"]
            if seen:
                result[region] = [{"interval": k, "rrp": v} for k, v in sorted(seen.items())]

        return result
    except Exception as e:
        logger.error(f"Error scraping trading prices: {e}")
        return {}


def scrape_predispatch_prices() -> dict:
    """
    Scrape predispatch prices for the remainder of the day.
    Returns: { region: [ {interval: "HH:MM", rrp: float}, ... ] }
    """
    try:
        urls = get_all_file_urls(PREDISPATCH_URL, "PUBLIC_PREDISPATCHIS")
        if not urls:
            logger.warning("No PredispatchIS files found")
            return {}

        # Use only the latest predispatch file
        latest_url = urls[-1]
        rows = fetch_zip_csv(latest_url)

        entries = parse_aemo_csv(
            rows, "PREDISPATCH_REGION_PRICES",
            ["DATETIME", "REGIONID", "RRP", "PREDISPATCHSEQNO"]
        )

        # Also try PRICE table name variant
        if not entries:
            entries = parse_aemo_csv(
                rows, "REGIONPRICE",
                ["DATETIME", "REGIONID", "RRP", "PREDISPATCHSEQNO"]
            )

        from zoneinfo import ZoneInfo
        aest = ZoneInfo("Australia/Sydney")
        now_aest = datetime.now(aest)

        region_series: dict[str, list] = {r: [] for r in NEM_REGIONS}

        for e in entries:
            region = e.get("REGIONID", "").strip()
            if region not in NEM_REGIONS:
                continue
            dt_str = e.get("DATETIME", "").strip()
            rrp_str = e.get("RRP", "").strip()
            if not dt_str or not rrp_str:
                continue
            try:
                rrp = round(float(rrp_str), 2)
                dt_str_clean = dt_str.replace("/", "-")
                dt = datetime.fromisoformat(dt_str_clean)
                # Only future intervals
                if dt.replace(tzinfo=None) >= now_aest.replace(tzinfo=None):
                    interval_label = dt.strftime("%H:%M")
                    region_series[region].append({
                        "interval": interval_label,
                        "rrp": rrp,
                        "sort_dt": dt_str
                    })
            except (ValueError, TypeError):
                pass

        result = {}
        for region, series in region_series.items():
            seen = {}
            for pt in sorted(series, key=lambda x: x["sort_dt"]):
                seen[pt["interval"]] = pt["rrp"]
            if seen:
                result[region] = [{"interval": k, "rrp": v} for k, v in sorted(seen.items())]

        return result
    except Exception as e:
        logger.error(f"Error scraping predispatch: {e}")
        return {}


def scrape_all() -> dict:
    """Scrape all data and return consolidated result."""
    logger.info("Starting NEMWeb scrape...")

    region_summary = scrape_region_summary()
    interconnectors = scrape_interconnectors()
    historical_prices = scrape_trading_prices_today()
    predispatch_prices = scrape_predispatch_prices()

    # Build prices from region summary
    prices = {}
    demand = {}
    generation = {}

    for region, data in region_summary.items():
        if "RRP" in data:
            prices[region] = data["RRP"]
        if "TOTALDEMAND" in data:
            demand[region] = data["TOTALDEMAND"]
        if "DISPATCHABLEGENERATION" in data:
            generation[region] = {
                "Scheduled": data.get("DISPATCHABLEGENERATION", 0),
                "Semi-Scheduled": data.get("SEMISCHEDULEDGENERATION", 0),
                "Net Interchange": data.get("NETINTERCHANGE", 0),
            }

    # If prices still empty, try direct price scrape
    if not prices:
        prices = scrape_dispatch_prices()

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "prices": prices,
        "demand": demand,
        "generation": generation,
        "interconnectors": interconnectors,
        "raw_summary": region_summary,
        "historical_prices": historical_prices,
        "predispatch_prices": predispatch_prices,
    }

    logger.info(f"Scrape complete. Regions with prices: {list(prices.keys())}, "
                f"historical points: {sum(len(v) for v in historical_prices.values())}, "
                f"predispatch points: {sum(len(v) for v in predispatch_prices.values())}")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import json
    data = scrape_all()
    print(json.dumps(data, indent=2))
