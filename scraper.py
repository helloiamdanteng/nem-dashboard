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
DISPATCH_IS_URL   = f"{NEMWEB_BASE}/REPORTS/CURRENT/DispatchIS_Reports/"
PREDISPATCH_URL   = f"{NEMWEB_BASE}/REPORTS/CURRENT/PredispatchIS_Reports/"
SCADA_URL         = f"{NEMWEB_BASE}/REPORTS/CURRENT/Dispatch_SCADA/"
TRADING_CURRENT   = f"{NEMWEB_BASE}/REPORTS/CURRENT/TradingIS_Reports/"
DISPATCH_IS_ARCHIVE = f"{NEMWEB_BASE}/REPORTS/ARCHIVE/DispatchIS_Reports/"
TRADING_ARCHIVE   = f"{NEMWEB_BASE}/REPORTS/ARCHIVE/TradingIS_Reports/"
MTPASA_DUID_URL   = f"{NEMWEB_BASE}/REPORTS/CURRENT/MTPASA_DUIDAvailability/"
ST_PASA_URL       = f"{NEMWEB_BASE}/REPORTS/CURRENT/Short_Term_PASA_Reports/"
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

# DUID prefix/pattern → fuel — used when registry has no entry for a DUID
_DUID_FUEL_PATTERNS = [
    # ── Black Coal ────────────────────────────────────────────────────────────
    (re.compile(r"^(KSG|KOGAN)", re.I),            "Black Coal"),  # Kogan Creek
    (re.compile(r"^SW[1-6]$", re.I),               "Black Coal"),  # Stanwell
    (re.compile(r"^(STAN[-_]?\d)", re.I),           "Black Coal"),  # Stanwell alt
    (re.compile(r"^TARONG\d", re.I),                "Black Coal"),  # Tarong
    (re.compile(r"^TNPS\d", re.I),                  "Black Coal"),  # Tarong North
    (re.compile(r"^MILLMERRAN\d", re.I),             "Black Coal"),  # Millmerran
    (re.compile(r"^GLAD\d", re.I),                   "Black Coal"),  # Gladstone
    (re.compile(r"^CALL", re.I),                     "Black Coal"),  # Callide B/C
    (re.compile(r"^(ERARING|ER0\d)", re.I),          "Black Coal"),  # Eraring
    (re.compile(r"^BW0\d$", re.I),                   "Black Coal"),  # Bayswater
    (re.compile(r"^(VP[56]|VALES)", re.I),           "Black Coal"),  # Vales Point
    (re.compile(r"^(MTPIPER|MT.?PIPER|MP\d$)", re.I),"Black Coal"), # Mt Piper
    (re.compile(r"^(LD0\d|LIDDELL)", re.I),          "Black Coal"),  # Liddell
    (re.compile(r"^(LYA\d|LYB\d)", re.I),            "Brown Coal"),  # Loy Yang A/B
    (re.compile(r"^(LOYYB|LOYANG)", re.I),           "Brown Coal"),  # Loy Yang alt
    (re.compile(r"^(HPS\d|HAZEL)", re.I),            "Brown Coal"),  # Hazelwood (retired)
    (re.compile(r"^(YWPS\d|YALLOURN)", re.I),        "Brown Coal"),  # Yallourn
    (re.compile(r"^(ANGLESEA)", re.I),               "Brown Coal"),  # Anglesea (retired)
    (re.compile(r"^(ENERGY.?BRIX|EBRIX)", re.I),     "Brown Coal"),  # Energy Brix (ret.)

    # ── Gas / CCGT / OCGT ────────────────────────────────────────────────────
    (re.compile(r"^DDPS\d", re.I),                   "Gas"),   # Darling Downs
    (re.compile(r"^BRAEMAR\d", re.I),                "Gas"),   # Braemar
    (re.compile(r"^SWAN_E", re.I),                   "Gas"),   # Swanbank E
    (re.compile(r"^CONDAMINE\d", re.I),              "Gas"),   # Condamine
    (re.compile(r"^OAKEY\d", re.I),                  "Gas"),   # Oakey
    (re.compile(r"^MSTUART\d", re.I),                "Gas"),   # Mt Stuart
    (re.compile(r"^YABULU\d", re.I),                 "Gas"),   # Yabulu
    (re.compile(r"^LOYNB\d", re.I),                  "Gas"),   # Loynton
    (re.compile(r"^COLONGRA\d", re.I),               "Gas"),   # Colongra
    (re.compile(r"^TALLAWARRA", re.I),               "Gas"),   # Tallawarra
    (re.compile(r"^URANQ\d", re.I),                  "Gas"),   # Uranquinty
    (re.compile(r"^HVGTS", re.I),                    "Gas"),   # Hunter Valley GT
    (re.compile(r"^(TORRA|TORRB)\d", re.I),          "Gas"),   # Torrens Island
    (re.compile(r"^PPCCGT", re.I),                   "Gas"),   # Pelican Point
    (re.compile(r"^OSBORNE\d", re.I),                "Gas"),   # Osborne
    (re.compile(r"^AGLSOM", re.I),                   "Gas"),   # Somerton SA
    (re.compile(r"^MINTARO\d", re.I),                "Gas"),   # Mintaro
    (re.compile(r"^DRYCGT\d", re.I),                 "Gas"),   # Dry Creek
    (re.compile(r"^LADBROKE\d", re.I),               "Gas"),   # Ladbroke Grove
    (re.compile(r"^JEERALANG\d", re.I),              "Gas"),   # Jeeralang A
    (re.compile(r"^JLBGT\d", re.I),                  "Gas"),   # Jeeralang B
    (re.compile(r"^(MORTLK|MORTLAKE)\d", re.I),      "Gas"),   # Mortlake
    (re.compile(r"^LAVNORTH", re.I),                 "Gas"),   # Laverton North
    (re.compile(r"^NEWPORT$", re.I),                 "Gas"),   # Newport
    (re.compile(r"^SOMERTON$", re.I),                "Gas"),   # Somerton VIC
    (re.compile(r"^VPGS\d", re.I),                   "Gas"),   # Valley Power
    (re.compile(r"^QPS\d", re.I),                    "Gas"),   # QPS Peakers
    (re.compile(r"^ROMA_\d", re.I),                  "Gas"),   # Roma
    (re.compile(r"^TOWN.?VILLE", re.I),              "Gas"),   # Townsville
    (re.compile(r"^MACKAYGT\d", re.I),               "Gas"),   # Mackay GT
    # Generic patterns for unrecognised gas units
    (re.compile(r"(OCGT|CCGT|_GT\d|GT_\d|GAS)", re.I), "Gas"),

    # ── Hydro ─────────────────────────────────────────────────────────────────
    (re.compile(r"^(KAREEYA|W.?HOE_|WIVENHOE)", re.I),  "Hydro"),
    (re.compile(r"(HYDRO|TUMUT|HUME|MURRAY|DARTM|EILDON|GORDON|POATINA|"
                r"TREVALLYN|JOHN.?BUTTERS|SNOWY|SHOALHAVEN|JINGELLIC|"
                r"BLOWERING|BENDEELA|BATESMAN|DARTM|CETHANA|MACKINTOSH|"
                r"BASTYAN|CATAGUNYA|DEVILS|FISHER|LEMONTHYME|DEVILS_G|"
                r"REPULSE|ROWALLAN|TARRALEAH|TUNGATINAH|LIAPOOTAH)", re.I), "Hydro"),
    # Pumped hydro load DUIDs (negative MW = pumping mode)
    (re.compile(r"(PUMP|_PUMP\d?$|PUMP\d?$)", re.I), "Hydro"),

    # ── Wind ──────────────────────────────────────────────────────────────────
    (re.compile(r"(WF\d|_WF\d?$|WIND|SNOWYWIND)", re.I),  "Wind"),
    # Wind farms by name pattern
    (re.compile(r"^(ARWF|MUWF|CROOKWF|RENWF|BODWF|BUNGWF|HORWF|YAWF|"
                r"CAPTL_WF|SAPN.*WF|MACARTHUR|YATPF|CATTLE.?HILL|"
                r"WATERLOO|HALLETT|BLUFF|CLEMENTS.?GAP|SNOWTOWN|CLAGUNA|"
                r"MUSSELROE|STUDLAND|WOOLNORTH|BLAYNEY|CULLERIN|CAPITAL|"
                r"GUNNING|WOODLAWN|BILOELA|COOKARDINIA|CROOKWELL|"
                r"LKBONNY|MOUNT.?MERCER|WAUBRA|MORTONS.?LANE|"
                r"STARFISH|BALD.?HILLS|MACARTHUR|MOORABOOL|OAKLANDS|"
                r"BULGANA|MT.?GELLIBRAND|DUNDONNELL|STOCKYARD|"
                r"CRAGB|NEOEN|YENDON|HAWKESDALE|BERRYBANK|CROWLANDS|"
                r"SILVERTON|SAPPHIRE|TILT|COOPERS.?GAP|KENNEDY|"
                r"DULACCA|KARARA|MOUNT.?EMERALD|HAUGHTON|CLARK.?CREEK|"
                r"GOLDEN.?PLAINS|GOLDPLA|RYE.?PARK|PALEN.?CREEK|COPPABELLA|"
                r"STOCKYD|COOPERS.?GAP|COOPERSGAP|COOPERS1)", re.I), "Wind"),

    # ── Solar ─────────────────────────────────────────────────────────────────
    (re.compile(r"(SOLAR|_SF\d|SF\d$|_PV|PV\d|BUNGALA|TAILEM|DARLINGTON.?PT"
                r"|DARLPNT|FINNSF|BOMEN)", re.I), "Solar"),
    # Solar farms by name pattern
    (re.compile(r"^(BNGSF|LILYSF|HPRG|SAPN.*SF|MOREE|NYNGAN|BROKEN.?HILL|"
                r"SUNRAYSIA|BANNERTON|KARADOC|WEMEN|LIMONDALE|WHITEGATE|"
                r"WOOLOOGA|DAYDREAM|HAMILTON|CLARE.?SOLAR|GANNAWARRA.?SOLAR|"
                r"NUMURKAH|MITRE.?SOLAR|YATPOOL|COHUNA|IRAAK|MURRA.?WARRA|"
                r"CLELND|SEBASTOPOL|GENERATION.*SF|CLERMONT|ROSS.?SOLAR|"
                r"SPRINGRIDGE|MILDURA|WEMEN|SUNRAYSIA|BAROOTA|"
                r"YATPOOL|KIAMAL|WINTON|BERYL|GOONUMBLA|PARKES|"
                r"FINLEY|GRIFFITH|COLEAMBALLY|FRASERSF|SAPHOENIX|"
                r"HOOKWOOD|DAROOBALGIE|MANILDRA|MERRIWA|UUNGULA|"
                r"GANGARRI|CONDONG|EMERALD|HAYMAN|KIDSTON|"
                r"LAKELAND|WHITSUNDAY|NORMANTON|MOUNT.?PLEASANT|"
                r"COLUMBOOLA|BLUEGRASS|MIDDLEMOUNT|WANDOAN.?SOLAR|"
                r"NARROMINE|DUBBO|ORANGE|ARMIDALE|TAMWORTH|"
                r"LONGREACH|CLONCURRY|CHARLEVILLE)", re.I), "Solar"),

    # ── Battery ───────────────────────────────────────────────────────────────
    (re.compile(r"(BATT|BATTERY|_BAT\d?|BAT_|HPR\d|HORNSDALE.?P|BYP|"
                r"BESS|_BESS|BARCABAT|GANNAWARRA|LAKELANDS|WANDOAN)", re.I), "Battery"),
    # Batteries by name
    (re.compile(r"^(VBESS|TBESS|NBESS|SBESS|CBESS|DBESS|FBESS|GBESS|"
                r"SNB|ERB|WALGETT|GELONG|TORRENS.*BAT|LDES|"
                r"NEOEN.*BAT|AGL.*BAT|ORIGIN.*BAT|EDF.*BAT|"
                r"WARATAH|GRID.*BAT|BIG.*BAT|VIRTUAL.*BAT)", re.I), "Battery"),

    # ── Liquid (diesel/oil) ───────────────────────────────────────────────────
    (re.compile(r"(DIESEL|DISTILLATE|LIQUID|FUEL.?OIL)", re.I), "Liquid"),
]

def _infer_fuel_from_duid(duid: str) -> str:
    """Last-resort fuel type inference from DUID name patterns."""
    for pattern, fuel in _DUID_FUEL_PATTERNS:
        if pattern.search(duid):
            return fuel
    return "Other"

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
        # AEMO XLSX actual column: "Fuel Source - Descriptor" (not "primary")
        fuel_col     = find_col("fuel source - descriptor", "fuel source - primary", "fuel source", "fuel type", "primary fuel", "fuel")
        tech_col     = find_col("technology type - primary", "technology type - descriptor", "technology type", "tech type", "technology")
        capacity_col = find_col("reg cap (mw)", "registered capacity", "capacity", "max cap", "reg cap")

        if duid_col is None:
            logger.warning("Could not find DUID column in registration list")
            return {}

        logger.info(f"Registration columns found — duid:{duid_col} station:{station_col} "
                    f"region:{region_col} fuel:{fuel_col} tech:{tech_col} cap:{capacity_col}")

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
            if fuel is None or fuel == "Other":
                # Try to infer from DUID and station name before giving up
                fuel = _infer_fuel_from_duid(duid) or _infer_fuel_from_duid(station.upper().replace(" ", "_"))
                if not fuel:
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
            # Static overrides live for DUIDs we have manually verified
            merged = {**live, **static}
            logger.info(f"NEM_UNITS: {len(live)} live + {len(static)} static = {len(merged)} total")
            return merged
        except Exception:
            return live
    # Fallback to static only
    logger.warning("Live AEMO registration failed - using static nem_units.json")
    p = _Path(__file__).parent / "nem_units.json"
    try:
        return _json.loads(p.read_text())
    except Exception as e:
        logger.warning(f"nem_units.json load failed: {e}")
        return {}


NEM_UNITS: dict = _load_nem_units()
logger.info(f"NEM_UNITS loaded: {len(NEM_UNITS)} DUIDs")

# Pump-load DUIDs: registered as scheduled LOAD in AEMO — SCADA reports positive MW
# when consuming (pumping). We negate so they display as negative generation.
PUMP_LOAD_DUIDS: set = {
    "SHPUMP",       # Shoalhaven pump (NSW)
    "WIVENPUMP1",   # Wivenhoe pump 1 (QLD)
    "WIVENPUMP2",   # Wivenhoe pump 2 (QLD)
    "MURRAY_PUMP",  # Murray pump (NSW)
    "BENDEELA_P",   # Bendeela pump (NSW)
    "KANGVALLEY",   # Kangaroo Valley pump (NSW)
}

FUEL_COLORS = {
    "Black Coal":        "#6b7280",
    "Brown Coal":        "#8B4513",
    "Gas":               "#ff9f40",
    "Hydro":             "#36a2eb",
    "Wind":              "#4bc0c0",
    "Solar":             "#ffd700",
    "Rooftop Solar":     "#ffe066",
    "Battery":           "#9b59b6",
    "Battery (charging)":"#a855f7",
    "Pump Hydro":        "#06b6d4",
    "Liquid":            "#e74c3c",
    "Other":             "#95a5a6",
}
ALL_FUELS = list(FUEL_COLORS.keys())

# ---------------------------------------------------------------------------
# Origin Energy assets — DUID -> display info
# These are Origin's registered generating units in the NEM
# ---------------------------------------------------------------------------
# ── Origin Energy asset registry ──────────────────────────────────────────────
# ORIGIN_DUIDS: the set of DUIDs that belong to Origin's portfolio.
# All metadata (station, fuel, region, capacity) comes from nem_units.json / live reg list.
# ORIGIN_DISPLAY_NAMES: overrides the AEMO station name with an Origin-branded name.
# Only add overrides where the AEMO name differs from what we want to show.
ORIGIN_DUIDS: set = {
    # QLD
    "DDPS1", "MSTUART1", "MSTUART2", "MSTUART3", "ROMA_7",
    "CLARESF1", "DDSF1", "DAYDSF1",
    "SNB01",
    # NSW
    "ER01", "ER02", "ER03", "ER04",
    "URANQ1", "URANQ2", "URANQ3", "URANQ4",
    "URANQ11", "URANQ12", "URANQ13", "URANQ14",  # aliases used in some AEMO files
    "SHGEN", "SHPUMP",
    "MOREESF1", "GUNNING1",
    "ERGT01",
    "ERB01",
    # VIC
    "MORTLK11", "MORTLK12",
    "STOCKYD1",
    # SA
    "OSB-AG",
    "QPS1", "QPS2", "QPS3", "QPS4", "QPS5",
    "LADBROK1", "LADBROK2",
    "BNGSF1", "BNGSF2",
    "SNOWNTH1", "SNOWSTH1",
}

# Display name overrides — maps DUID → display station name shown on Origin page.
# Only needed where AEMO's registered name differs from what Origin calls it.
ORIGIN_DISPLAY_NAMES: dict = {
    "SNB01":    "Supernode Battery",
    "OSB-AG":   "Osborne Cogen",
    "ROMA_7":   "Roma Gas",
    "BNGSF1":   "Bungala Solar 1",
    "BNGSF2":   "Bungala Solar 2",
    "GUNNING1": "Gunning Wind Farm",
    "ERB01":     "Eraring Battery",
    "SHPUMP":   "Shoalhaven Pump",
    "ERGT01":   "Eraring",
}


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
            # Normalise any absolute URL to use www. and uppercase /REPORTS/
            href = re.sub(r'https?://(?:www\.)?nemweb\.com\.au', NEMWEB_BASE, href, flags=re.I)
            href = re.sub(r'/[Rr]eports/[Cc]urrent/', '/REPORTS/CURRENT/', href)
            href = re.sub(r'/[Rr]eports/[Aa]rchive/', '/REPORTS/ARCHIVE/', href)
            found.append(href)
        elif href.startswith("/"):
            # Normalise path casing
            href = re.sub(r'/[Rr]eports/[Cc]urrent/', '/REPORTS/CURRENT/', href)
            href = re.sub(r'/[Rr]eports/[Aa]rchive/', '/REPORTS/ARCHIVE/', href)
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
                  "DISPATCHABLEGENERATION","DISPATCHABLELOAD","SEMISCHEDULE_CLEAREDMW","NETINTERCHANGE",
                  "DEMAND_AND_NONSCHEDGEN","TOTALINTERMITTENTGENERATION",
                  "BDU_ENERGY_STORAGE","BDU_MAX_AVAIL","BDU_MIN_AVAIL",
                  "BDU_CLEAREDMW_GEN","BDU_CLEAREDMW_LOAD","BDU_INITIAL_ENERGY_STORAGE"]:
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
    gen_demand: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    prices: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    ss_solar: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    ss_wind: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    ic_flows: dict[str, dict] = {}   # { ic_id: { label: flow } }
    rooftop: dict[str, dict] = {r: {} for r in NEM_REGIONS}  # TOTALINTERMITTENTGENERATION
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
                semisched_str = row.get("SEMISCHEDULE_CLEAREDMW", "")
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
                    # gen_demand = TOTALDEMAND + SEMISCHEDULE_CLEAREDMW (sits at top of full supply stack)
                    if semisched_str:
                        try:
                            gen_dem = round(float(demand_str), 1) + round(float(semisched_str), 1)
                            pts.append(("gen_demand", region, label, gen_dem))
                        except (ValueError, TypeError):
                            pass
                    rooftop_str = row.get("SS_SOLAR_CLEAREDMW", "")
                    wind_str    = row.get("SS_WIND_CLEAREDMW", "")
                    if rooftop_str:
                        try:
                            rooftop = round(float(rooftop_str), 1)
                            if rooftop > 0:
                                pts.append(("rooftop", region, label, rooftop))
                            pts.append(("solar", region, label, rooftop))
                        except (ValueError, TypeError):
                            pass
                    if wind_str:
                        try:
                            pts.append(("wind", region, label, round(float(wind_str), 1)))
                        except (ValueError, TypeError):
                            pass
                    # BDU (battery) fields
                    bdu_gen  = row.get("BDU_CLEAREDMW_GEN", "")
                    bdu_load = row.get("BDU_CLEAREDMW_LOAD", "")
                    bdu_soc  = row.get("BDU_ENERGY_STORAGE", "")
                    bdu_cap  = row.get("BDU_MAX_AVAIL", "")
                    disp_load_str = row.get("DISPATCHABLELOAD", "")
                    if bdu_gen or bdu_load:
                        try:
                            g = round(float(bdu_gen or 0), 1)
                            l = round(float(bdu_load or 0), 1)
                            s = round(float(bdu_soc), 1) if bdu_soc else None
                            c = round(float(bdu_cap), 1) if bdu_cap else None
                            dl = round(float(disp_load_str), 1) if disp_load_str else l
                            pump = max(round(dl - l, 1), 0)
                            pts.append(("bdu", region, label, {"net_mw": round(g-l,1), "gen": g, "load": l, "pump_load": pump, "storage": s, "max_avail": c}))
                        except (ValueError, TypeError):
                            pass
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
            # Extract IC flows from DISPATCH_INTERCONNECTORRES
            for row in _parse_aemo(text, "DISPATCH_INTERCONNECTORRES"):
                ic_id = row.get("INTERCONNECTORID", "").strip()
                if not ic_id:
                    continue
                if row.get("INTERVENTION", "0") not in ("0", ""):
                    continue
                dt_str = row.get("SETTLEMENTDATE", "")
                flow_str = row.get("MWFLOW", "")
                if not dt_str or not flow_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=5)
                    if dt.date() != today_date or dt.strftime("%H:%M") > now_label:
                        continue
                    pts.append(("ic", ic_id, dt.strftime("%H:%M"), round(float(flow_str), 1)))
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
            for item in pts:
                kind = item[0]
                if kind == "demand":
                    _, region, label, val = item
                    demand[region][label] = val
                elif kind == "op_demand":
                    _, region, label, val = item
                    op_demand[region][label] = val
                elif kind == "gen_demand":
                    _, region, label, val = item
                    gen_demand[region][label] = val
                elif kind == "solar":
                    _, region, label, val = item
                    ss_solar[region][label] = val
                elif kind == "wind":
                    _, region, label, val = item
                    ss_wind[region][label] = val
                elif kind == "ic":
                    _, ic_id, label, val = item
                    if ic_id not in ic_flows:
                        ic_flows[ic_id] = {}
                    ic_flows[ic_id][label] = val
                elif kind == "rooftop":
                    _, region, label, val = item
                    rooftop[region][label] = val
                elif kind == "bdu":
                    _, region, label, val = item
                    _bdu_history[region][label] = val
                else:
                    _, region, label, val = item
                    prices[region][label] = val

    demand_result = {r: [{"interval": k, "demand": v} for k, v in sorted(s.items())]
                     for r, s in demand.items() if s}
    op_demand_result = {r: [{"interval": k, "demand": v} for k, v in sorted(s.items())]
                        for r, s in op_demand.items() if s}
    price_result  = {r: [{"interval": k, "rrp": v}    for k, v in sorted(s.items())]
                     for r, s in prices.items() if s}

    # Backfill _ic_history so IC chart shows full day on startup
    for ic_id, series in ic_flows.items():
        if ic_id not in _ic_history:
            _ic_history[ic_id] = {}
        _ic_history[ic_id].update(series)

    # Store rooftop data globally so scrape_scada_history can merge it in after backfill
    global _rooftop_history
    for region in NEM_REGIONS:
        if region not in _rooftop_history:
            _rooftop_history[region] = {}
        _rooftop_history[region].update(rooftop[region])

    logger.info(f"DispatchIS history: demand={sum(len(v) for v in demand_result.values())} pts, "
                f"prices={sum(len(v) for v in price_result.values())} pts, "
                f"ic_flows={sum(len(v) for v in ic_flows.values())} pts "
                f"from {len(today_zips)} files (ok={fetch_ok} empty={fetch_empty} fail={fetch_fail})")
    solar_result    = {r: [{"interval": k, "mw": v} for k, v in sorted(s.items())]
                        for r, s in ss_solar.items() if s}
    wind_result     = {r: [{"interval": k, "mw": v} for k, v in sorted(s.items())]
                        for r, s in ss_wind.items() if s}
    gen_demand_result = {r: [{"interval": k, "demand": v} for k, v in sorted(s.items())]
                         for r, s in gen_demand.items() if s}
    return {"demand": demand_result, "op_demand": op_demand_result,
            "prices": price_result, "solar": solar_result, "wind": wind_result,
            "gen_demand": gen_demand_result}


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
                # Only keep today's future intervals (AEST) — keep anything within last 30min too
                if dt.date() == today and dt >= now_aest - timedelta(minutes=30):
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
            demand_str = row.get("DEMAND_AND_NONSCHEDGEN", row.get("TOTALDEMAND", row.get("DEMAND", "")))
            if not dt_str or not demand_str:
                continue
            try:
                demand = round(float(demand_str), 1)
                # DATETIME is end-of-interval; shift back 30min for display
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt.date() == today and dt > now_aest:
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
    """
    Extract today's future generation forecast from PREDISPATCH files.
    Returns { region: [{interval, Scheduled, SemiScheduled}] } for today's future intervals.
    """
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
            if not dt_str:
                continue
            try:
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt.date() == today and dt >= now_aest:
                    label = dt.strftime("%H:%M")
                    sched = float(row.get("DISPATCHABLEGENERATION", 0) or 0)
                    semi  = float(row.get("SEMISCHEDULE_CLEAREDMW", 0) or 0)
                    sol   = float(row.get("SS_SOLAR_UIGF", row.get("SS_SOLAR_CLEAREDMW", 0)) or 0)
                    win   = float(row.get("SS_WIND_UIGF",  row.get("SS_WIND_CLEAREDMW",  0)) or 0)
                    region_series[region][label] = {
                        "Scheduled":     round(sched, 1),
                        "SemiScheduled": round(semi, 1),
                        "solar":         round(sol, 1),
                        "wind":          round(win, 1),
                    }
            except (ValueError, TypeError):
                pass
        break
    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, **v} for k, v in sorted(series.items())]
    return result


def scrape_p5min_unit_solution(text: str) -> dict:
    """
    Extract DUID-level MW forecasts from P5MIN_UNIT_SOLUTION.
    Returns { duid: [{interval: "HH:MM", mw: float}] } for future intervals today.
    P5MIN covers ~1hr ahead in 5-min intervals.
    INTERVAL_DATETIME is end-of-period — subtract 5min for display label.
    """
    if not text:
        return {}

    now_aest = datetime.now(AEST).replace(tzinfo=None)
    today    = now_aest.date()
    now_hhmm = now_aest.strftime("%H:%M")

    result: dict[str, dict] = {}

    for tk in ["P5MIN_UNIT_SOLUTION", "P5MIN_UNITSOLUTION"]:
        rows = _parse_aemo(text, tk)
        if not rows:
            continue
        for row in rows:
            if row.get("INTERVENTION", "0") not in ("0", ""):
                continue
            duid = row.get("DUID", "").strip()
            if not duid:
                continue
            dt_str = row.get("INTERVAL_DATETIME", "")
            if not dt_str:
                continue
            try:
                dt = datetime.strptime(dt_str[:16], "%Y/%m/%d %H:%M")
                dt -= timedelta(minutes=5)
            except Exception:
                try:
                    dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M")
                    dt -= timedelta(minutes=5)
                except Exception:
                    continue
            if dt.date() != today:
                continue
            hhmm = dt.strftime("%H:%M")
            if hhmm <= now_hhmm:
                continue
            mw_str = row.get("TOTALCLEARED", row.get("SEMIDISPATCHCAP", ""))
            try:
                mw = float(mw_str)
            except Exception:
                continue
            result.setdefault(duid, {})[hhmm] = mw
        if result:
            break

    return {duid: [{"interval": k, "mw": v} for k, v in sorted(pts.items())]
            for duid, pts in result.items() if pts}


# Keep old name as alias so nothing else breaks
scrape_predispatch_unit_solution = scrape_p5min_unit_solution




def scrape_tomorrow_prices(text: str) -> dict:
    """
    Extract tomorrow's predispatch price forecast from PREDISPATCH files.
    Returns { region: [{interval: "HH:MM", rrp: float}] } for tomorrow only.
    """
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    tomorrow = (now_aest + timedelta(days=1)).date()
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
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt.date() == tomorrow:
                    region_series[region][dt.strftime("%H:%M")] = rrp
            except (ValueError, TypeError):
                pass
        break
    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "rrp": v} for k, v in sorted(series.items())]
    logger.info(f"Tomorrow prices: {sum(len(v) for v in result.values())} pts")
    return result


def scrape_tomorrow_demand(text: str, stpasa: dict) -> dict:
    """
    Tomorrow's demand forecast — combines predispatch (finer resolution near midnight)
    and STPASA (broader 30-min intervals for the full day).
    Returns { region: [{interval: "HH:MM", demand: float}] }
    """
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    tomorrow = (now_aest + timedelta(days=1)).date()
    region_series: dict[str, dict] = {r: {} for r in NEM_REGIONS}

    # First: predispatch region solution (high-res near midnight)
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
            demand_str = row.get("DEMAND_AND_NONSCHEDGEN", row.get("TOTALDEMAND", row.get("DEMAND", "")))
            if not dt_str or not demand_str:
                continue
            try:
                demand = round(float(demand_str), 1)
                dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                if dt.date() == tomorrow:
                    region_series[region][dt.strftime("%H:%M")] = demand
            except (ValueError, TypeError):
                pass
        break

    # Second: fill remainder from STPASA (30-min intervals)
    for region, pts in stpasa.items():
        if region not in NEM_REGIONS:
            continue
        for pt in pts:
            label_full = pt.get("interval", "")  # "YYYY-MM-DD HH:MM"
            d50 = pt.get("demand_50")
            if not label_full or d50 is None:
                continue
            try:
                dt = datetime.strptime(label_full, "%Y-%m-%d %H:%M")
                if dt.date() == tomorrow:
                    hhmm = dt.strftime("%H:%M")
                    if hhmm not in region_series[region]:  # don't overwrite predispatch
                        region_series[region][hhmm] = d50
            except ValueError:
                pass

    result = {}
    for region, series in region_series.items():
        if series:
            result[region] = [{"interval": k, "demand": v} for k, v in sorted(series.items())]
    logger.info(f"Tomorrow demand: {sum(len(v) for v in result.values())} pts")
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
            raw_fuel = info.get("fuel", "")
            fuel   = raw_fuel if (raw_fuel and raw_fuel != "Other") else _infer_fuel_from_duid(duid)
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
_duid_history: dict[str, dict] = {}  # { duid: { "HH:MM": mw } } — per-unit today history
_rooftop_history: dict[str, dict] = {r: {} for r in NEM_REGIONS}  # TOTALINTERMITTENTGENERATION

def _update_fuel_history(fuel_mix: dict, scada: dict | None = None, pump_load: dict | None = None) -> None:
    """Store a fuel mix snapshot and per-DUID snapshot. Prune to today only."""
    now = datetime.now(AEST)
    # Snap to nearest 5-min boundary so labels align with the 5-min time spine in the frontend
    snapped_min = (now.minute // 5) * 5
    label = now.strftime("%H:") + f"{snapped_min:02d}"
    for region in NEM_REGIONS:
        if region not in fuel_mix:
            continue
        snap = dict(fuel_mix[region])
        # Store pump hydro load as negative value under 'Pump Hydro' key
        if pump_load and pump_load.get(region, 0) < -1:
            snap["Pump Hydro"] = round(pump_load[region], 1)
        _fuel_history[region][label] = snap
        if len(_fuel_history[region]) > 290:
            oldest = sorted(_fuel_history[region].keys())[0]
            del _fuel_history[region][oldest]

    # Store per-DUID history for station drill-down
    if scada:
        for duid, mw in scada.items():
            if mw is None:
                continue
            if duid not in _duid_history:
                _duid_history[duid] = {}
            # Pump-load DUIDs report positive MW when consuming — negate for display
            stored_mw = -round(mw, 1) if duid in PUMP_LOAD_DUIDS else round(mw, 1)
            _duid_history[duid][label] = stored_mw
            if len(_duid_history[duid]) > 290:
                oldest = sorted(_duid_history[duid].keys())[0]
                del _duid_history[duid][oldest]

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
_bdu_history: dict[str, dict] = {r: {} for r in NEM_REGIONS}  # { region: { HH:MM: {gen,load,storage,max_avail} } }


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


def _update_bdu_history(region_summary: dict) -> None:
    label = datetime.now(AEST).strftime("%H:%M")
    for region, d in region_summary.items():
        if region not in NEM_REGIONS:
            continue
        gen  = d.get("BDU_CLEAREDMW_GEN", 0) or 0
        load = d.get("BDU_CLEAREDMW_LOAD", 0) or 0
        soc  = d.get("BDU_ENERGY_STORAGE")
        cap  = d.get("BDU_MAX_AVAIL") or d.get("BDU_MIN_AVAIL")
        # Pump hydro load = total scheduled load - battery load
        disp_load = d.get("DISPATCHABLELOAD", 0) or 0
        pump_load = max(round(disp_load - load, 1), 0)  # never negative
        _bdu_history[region][label] = {
            "net_mw":    round(gen - load, 1),   # positive=discharging, negative=charging
            "gen":       round(gen, 1),
            "load":      round(load, 1),
            "pump_load": pump_load,               # pump hydro MW consuming
            "storage":   round(soc, 1) if soc is not None else None,
            "max_avail": round(cap, 1) if cap is not None else None,
        }


def _update_live_duid_history(scada: dict) -> None:
    """Write current SCADA snapshot into _duid_history so modal charts stay live.
    Called every 5 min from scrape_all. Only writes DUIDs we care about."""
    label = datetime.now(AEST).strftime("%H:%M")
    for duid, mw in scada.items():
        if mw is None:
            continue
        # Negate pump-load DUIDs so they show as negative
        stored_mw = -round(mw, 1) if duid in PUMP_LOAD_DUIDS else round(mw, 1)
        if duid not in _duid_history:
            _duid_history[duid] = {}
        _duid_history[duid][label] = stored_mw
        # Trim to 290 points (~24h of 5-min data)
        if len(_duid_history[duid]) > 290:
            oldest = sorted(_duid_history[duid].keys())[0]
            del _duid_history[duid][oldest]


def _get_bdu_history() -> dict:
    result = {}
    for region, series in _bdu_history.items():
        if series:
            result[region] = [{"interval": k, **v} for k, v in sorted(series.items())]
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

    # Run all IO-bound fetches concurrently with per-future timeout
    with ThreadPoolExecutor(max_workers=6) as ex:
        f_dispatch_is   = ex.submit(_fetch_dispatch_is)
        f_predispatch   = ex.submit(_fetch_predispatch)
        f_trading       = ex.submit(scrape_trading_history)
        f_dispatch_hist = ex.submit(scrape_dispatch_history)
        f_scada         = ex.submit(_fetch_full_scada)

    def _safe_result(fut, default, name):
        try:
            return fut.result(timeout=55)
        except Exception as e:
            logger.error(f"scrape_all: {name} failed — {e}")
            return default

    dispatch_text    = _safe_result(f_dispatch_is,   "",  "dispatch_is")
    predispatch_text = _safe_result(f_predispatch,    "",  "predispatch")
    trading          = _safe_result(f_trading,        {"prices": {}, "fetch_stats": {}}, "trading")
    dispatch_hist    = _safe_result(f_dispatch_hist,  {"demand": {}, "op_demand": {}, "prices": {}}, "dispatch_hist")
    scada_vals       = _safe_result(f_scada,          {}, "scada")

    dispatch_demand       = dispatch_hist.get("demand", {})
    dispatch_op_demand    = dispatch_hist.get("op_demand", {})
    dispatch_price_5min   = dispatch_hist.get("prices", {})
    dispatch_solar        = dispatch_hist.get("solar", {})
    dispatch_wind         = dispatch_hist.get("wind", {})

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
    pd_units  = {}  # AEMO does not publish unit-level predispatch on public NEMWeb

    # Tomorrow's forecasts from same predispatch file
    tomorrow_prices = scrape_tomorrow_prices(predispatch_text)
    tomorrow_demand = scrape_tomorrow_demand(predispatch_text, {})

    # In-memory accumulators — IC builds up over process lifetime
    _update_demand_history(region_summary)
    _update_ic_history(interconnectors)
    _update_bdu_history(region_summary)
    _update_live_duid_history(scada_vals)   # keep _duid_history current for modal charts
    # Inject live solar/wind into dispatch history for current interval
    now_label = datetime.now(AEST).strftime("%H:%M")
    for region, rdata in region_summary.items():
        if region not in NEM_REGIONS:
            continue
        sol = rdata.get("SS_SOLAR_CLEAREDMW")
        win = rdata.get("SS_WIND_CLEAREDMW")
        if sol is not None:
            dispatch_solar.setdefault(region, [])
            dispatch_solar[region] = [p for p in dispatch_solar[region] if p["interval"] != now_label]
            dispatch_solar[region].append({"interval": now_label, "mw": round(float(sol), 1)})
        if win is not None:
            dispatch_wind.setdefault(region, [])
            dispatch_wind[region] = [p for p in dispatch_wind[region] if p["interval"] != now_label]
            dispatch_wind[region].append({"interval": now_label, "mw": round(float(win), 1)})
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

    # Build Origin assets output — ORIGIN_DISPLAY_NAMES overrides nem_units station name
    origin_assets_out = {}
    for duid in ORIGIN_DUIDS:
        mw       = scada_vals.get(duid)
        reg_info = NEM_UNITS.get(duid, {})\
        # Solar and wind absent from SCADA = generating 0 (semi-scheduled, not truly unknown)
        fuel     = reg_info.get("fuel")     or "Other"
        if mw is None and fuel in ("Solar", "Wind"):
            mw = 0.0
        station  = ORIGIN_DISPLAY_NAMES.get(duid) or reg_info.get("station") or duid
        region   = reg_info.get("region")   or ""
        capacity = reg_info.get("capacity")
        origin_assets_out[duid] = {
            "station":  station,
            "fuel":     fuel,
            "region":   region,
            "capacity": capacity,
            "mw":       mw,
            "pct":      round(mw / capacity * 100, 1) if mw is not None and capacity else None,
            "status":   "running" if (mw is not None and mw > 5) else ("charging" if (mw is not None and mw < -5) else ("off" if mw is not None else "unknown")),
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
        "solar_history":         dispatch_solar,
        "wind_history":          dispatch_wind,
        "gen_demand_history":    dispatch_hist.get("gen_demand", {}),
        "predispatch_demand":    pd_demand,
        "predispatch_gen":       pd_gen,
        "predispatch_units":     pd_units,
        "ic_history":            ic_history,
        "predispatch_ic":        pd_interconnectors,
        "bdu_history":           _get_bdu_history(),
        "origin_assets":         origin_assets_out,
        "fuel_colors":           FUEL_COLORS,
        "all_fuels":             ALL_FUELS,
        "tomorrow_prices":       tomorrow_prices,
        "tomorrow_demand":       tomorrow_demand,
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
        "https://www.nemweb.com.au/REPORTS/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
        "https://nemweb.com.au/REPORTS/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
        f"{NEMWEB_BASE}/REPORTS/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
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
        f"{NEMWEB_BASE}/REPORTS/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV",
        # Also try the NEM Registration list as a direct download from the data portal
        "https://data.wa.aemo.com.au/public/public-data/datafiles/facilities/facilities.csv",  # WA only, skip
    ]
    # The most reliable source: MMS DUDETAILSUMMARY via NEMWeb
    # Available as a static file updated daily
    url = f"{NEMWEB_BASE}/REPORTS/CURRENT/Ancillary_Services/PUBLIC_DVD_DUDETAILSUMMARY_202503120000.zip"

    # Actually use the correct approach: scrape the Generators listing page
    # NEMWeb has a static CSV at this well-known path:
    gen_csv_url = "https://www.nemweb.com.au/REPORTS/CURRENT/SEMP/PUBLIC_SEMP_REGISTRATION.CSV"
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


ROOFTOP_PV_URL = f"{NEMWEB_BASE}/REPORTS/CURRENT/ROOFTOP_PV/ACTUAL/"

def _scrape_rooftop_pv_latest() -> dict:
    """
    Fetch the latest ROOFTOP_PV_ACTUAL file and return { regionid: mw }.
    Published every 30 min with 30-min actuals by state.
    Table: ROOFTOP_PV,ACTUAL  Cols: INTERVAL_DATETIME, REGIONID, POWER, QI
    """
    try:
        url = get_latest_file_url(ROOFTOP_PV_URL, "PUBLIC_ROOFTOP_PV_ACTUAL")
        if not url:
            return {}
        text = _read_zip(url)
        if not text:
            return {}
        # Take latest interval per region (file may have multiple 30-min rows)
        result = {}
        latest_dt = {}
        for row in _parse_aemo(text, "ROOFTOP_PV_ACTUAL"):
            region = row.get("REGIONID", "").strip().upper()
            if region not in NEM_REGIONS:
                continue
            dt_str  = row.get("INTERVAL_DATETIME", "")
            power_str = row.get("POWER", "")
            try:
                mw = round(float(power_str), 1)
                if mw < 0:
                    continue
                if dt_str > latest_dt.get(region, ""):
                    latest_dt[region] = dt_str
                    result[region] = mw
            except (ValueError, TypeError):
                pass
        logger.info(f"Rooftop PV actual: {result}")
        return result
    except Exception as e:
        logger.warning(f"Rooftop PV fetch failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# ST PASA — 7-day ahead regional demand forecast
# ---------------------------------------------------------------------------

def scrape_stpasa_demand() -> dict:
    """
    Fetch latest ST PASA file and extract STPASA_REGIONSOLUTION.
    Returns { region: [{interval, demand_50, demand_10, solar_uigf, wind_uigf}] }
    """
    try:
        urls = _list_hrefs(ST_PASA_URL)
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
                solar = row.get("SS_SOLAR_UIGF", "")
                wind  = row.get("SS_WIND_UIGF", "")
                if not dt_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-"))
                    if dt.replace(tzinfo=None) < now_aest:
                        continue
                    label = dt.strftime("%Y-%m-%d %H:%M")
                    region_series[region][label] = {
                        "demand_50":  round(float(d50), 1) if d50 else None,
                        "demand_10":  round(float(d10), 1) if d10 else None,
                        "solar_uigf": round(float(solar), 1) if solar else None,
                        "wind_uigf":  round(float(wind), 1) if wind else None,
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

    # Build CPID→region map from the latest DispatchIS for unregistered DUIDs
    _CPID_REGION = {"N": "NSW1", "Q": "QLD1", "V": "VIC1", "S": "SA1", "T": "TAS1"}
    cpid_map: dict = {}
    try:
        dispatch_text = _fetch_dispatch_is()
        for row in _parse_aemo(dispatch_text, "DISPATCH_UNIT_SOLUTION"):
            duid = row.get("DUID", "").strip().upper()
            cpid = row.get("CONNECTIONPOINTID", "").strip().upper()
            if duid and cpid and cpid[0] in _CPID_REGION:
                cpid_map[duid] = _CPID_REGION[cpid[0]]
    except Exception as e:
        logger.warning(f"scrape_scada_history: CPID map fetch failed: {e}")

    # Now convert to fuel_mix per timestamp and load into _fuel_history + _duid_history
    reg = NEM_UNITS
    loaded = 0
    for label in sorted(all_snapshots.keys()):
        duids = all_snapshots[label]
        fuel_mix: dict = {r: {} for r in NEM_REGIONS}
        pump_load_hist: dict = {}  # track pump hydro load per region
        for duid, mw_raw in duids.items():
            info   = reg.get(duid, {})
            region = info.get("region", "") or cpid_map.get(duid, "")
            raw_fuel = info.get("fuel", "")
            fuel   = raw_fuel if (raw_fuel and raw_fuel != "Other") else _infer_fuel_from_duid(duid)
            # Negate pump-load DUIDs (positive SCADA = consuming) for all fuel logic
            mw = -mw_raw if (duid in PUMP_LOAD_DUIDS and mw_raw is not None) else mw_raw
            # Always store per-DUID history regardless of region match
            if mw is not None:
                if duid not in _duid_history:
                    _duid_history[duid] = {}
                _duid_history[duid][label] = round(mw, 1)
            if region not in NEM_REGIONS:
                continue
            mw_val_signed = mw if mw is not None else 0
            mw_pos = max(mw_val_signed, 0)
            fuel_mix[region][fuel] = round(fuel_mix[region].get(fuel, 0) + mw_pos, 1)
            # Track pump hydro load (negative mw on Hydro DUIDs)
            if fuel == "Hydro" and mw_val_signed < -1:
                pump_load_hist[region] = round(pump_load_hist.get(region, 0) + mw_val_signed, 1)
        # Store into _fuel_history with pump hydro
        for region in NEM_REGIONS:
            if fuel_mix[region]:
                snap = dict(fuel_mix[region])
                if pump_load_hist.get(region, 0) < -1:
                    snap["Pump Hydro"] = round(pump_load_hist[region], 1)
                _fuel_history[region][label] = snap
        loaded += 1

    logger.info(f"scrape_scada_history: loaded {loaded} time slots into fuel history")

    # Merge rooftop solar data (from dispatch history backfill) into fuel_history
    for region in NEM_REGIONS:
        for label, val in _rooftop_history.get(region, {}).items():
            if label in _fuel_history.get(region, {}):
                _fuel_history[region][label]["Rooftop Solar"] = val
    logger.info("scrape_scada_history: merged rooftop solar into fuel history")


# scrape_gen — medium speed: fuel mix from SCADA + NEM_UNITS static registry
# Refreshed every 15 min.
# ---------------------------------------------------------------------------

def scrape_gen() -> dict:
    """
    SCADA-first: every DUID in DISPATCH_UNIT_SCADA is included.
    Uses live AEMO registration list as primary source for station/fuel/capacity.
    Falls back to NEM_UNITS static registry, then fuel inference from DUID name.
    """
    logger.info("scrape_gen starting...")

    # Fetch SCADA, DispatchIS and registration list concurrently
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_scada    = ex.submit(_fetch_full_scada)
        f_dispatch = ex.submit(_fetch_dispatch_is)
        f_reg      = ex.submit(_load_registration_list)
    scada         = f_scada.result()
    dispatch_text = f_dispatch.result()
    live_reg      = f_reg.result()

    # Merge: live registration list takes priority, NEM_UNITS fills any gaps
    reg = {**NEM_UNITS, **live_reg} if live_reg else NEM_UNITS
    logger.info(f"scrape_gen: using {len(live_reg)} live reg entries + {len(NEM_UNITS)} static")

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
    pump_load:  dict = {}  # { region: negative_mw } for pump hydro display
    unmatched_log: list = []

    for duid, mw_raw in scada.items():
        # Pump-load DUIDs report positive MW when consuming — negate for display
        mw = -mw_raw if (duid in PUMP_LOAD_DUIDS and mw_raw is not None) else mw_raw
        info     = reg.get(duid.upper(), {})
        region   = info.get("region", "") or cpid_map.get(duid.upper(), "")
        raw_fuel = info.get("fuel", "")
        fuel     = raw_fuel if (raw_fuel and raw_fuel != "Other") else _infer_fuel_from_duid(duid)
        station  = info.get("station", duid)
        capacity = info.get("capacity")

        in_nem = region in NEM_REGIONS
        if not in_nem:
            region = "Unknown"

        mw_val = mw if mw is not None else 0
        # fuel_mix is for the generation chart — always positive (generation only)
        # grouped is for DUID/station display — signed (negative = pumping/charging)
        mw_for_chart = max(mw_val, 0)
        # Batteries and pump hydro show negative MW in grouped for display
        mw_pos = mw_val if fuel in ("Battery", "Hydro") else mw_for_chart

        # Log significant unclassified units so we can fix them
        if fuel == "Other" and abs(mw_val) > 50:
            logger.warning(f"OTHER_DUID: {duid} region={region} mw={mw_pos:.0f} raw_fuel={raw_fuel!r} station={station!r}")

        if in_nem:
            fuel_mix[region][fuel] = round(fuel_mix[region].get(fuel, 0) + mw_for_chart, 1)
        if in_nem:
            nem_totals[fuel]       = round(nem_totals.get(fuel, 0) + mw_for_chart, 1)
        # Track pump hydro load (negative mw on Hydro DUIDs) for gen chart
        if in_nem and fuel == "Hydro" and mw_val < -1:
            pump_load[region] = round(pump_load.get(region, 0) + mw_val, 1)

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

    # Add real Rooftop Solar from AEMO ROOFTOP_PV/ACTUAL — 30-min intervals by state
    rooftop_pv = _scrape_rooftop_pv_latest()
    for region, mw in rooftop_pv.items():
        if mw > 0:
            fuel_mix[region]["Rooftop Solar"] = mw

    # Override Solar and Wind totals using authoritative DISPATCH_REGIONSUM fields
    # SS_SOLAR_CLEAREDMW / SS_WIND_CLEAREDMW are exact totals including all semi-scheduled
    # units regardless of whether they appear in NEM_UNITS or the registration list
    try:
        for row in _parse_aemo(dispatch_text, "DISPATCH_REGIONSUM"):
            region = row.get("REGIONID", "").strip()
            if region not in NEM_REGIONS:
                continue
            if row.get("INTERVENTION", "0") not in ("0", ""):
                continue
            sol_str = row.get("SS_SOLAR_CLEAREDMW", "")
            win_str = row.get("SS_WIND_CLEAREDMW", "")
            try:
                sol = round(float(sol_str), 1)
                if sol > 0:
                    fuel_mix[region]["Solar"] = sol
            except (ValueError, TypeError):
                pass
            try:
                win = round(float(win_str), 1)
                if win > 0:
                    fuel_mix[region]["Wind"] = win
            except (ValueError, TypeError):
                pass
    except Exception as e:
        logger.debug(f"scrape_gen: REGIONSUM solar/wind override failed: {e}")

    # Accumulate into in-memory history
    _update_fuel_history(fuel_mix, scada, pump_load)

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

def scrape_mtpasa_outages() -> list:
    """
    Fetch MTPASA DUID Availability report and identify units that are
    currently offline, derated, or returning within the next 30 days.

    Returns list of dicts:
      { duid, station, fuel, region, capacity,
        avail_today, state_today,
        change_mw, change_date, return_date }
    """
    try:
        files = _list_hrefs(MTPASA_DUID_URL)
    except Exception as e:
        logger.warning(f"scrape_mtpasa_outages: listing failed: {e}")
        return []

    if not files:
        logger.warning("scrape_mtpasa_outages: no files found")
        return []

    # Take the most recent file
    latest = sorted(files)[-1]
    logger.info(f"scrape_mtpasa_outages: fetching {latest}")

    try:
        text = _read_zip(latest)
    except Exception as e:
        logger.warning(f"scrape_mtpasa_outages: read failed: {e}")
        return []

    if not text:
        return []

    # Parse rows — table name is MTPASA_DUIDAVAILABILITY
    now_aest  = datetime.now(AEST)
    today_str = now_aest.strftime("%Y/%m/%d")

    # Collect per-DUID data: { duid: { date_str: {avail, state} } }
    duid_data: dict = {}
    for row in _parse_aemo(text, "MTPASA_DUIDAVAILABILITY"):
        duid = row.get("DUID", "").strip()
        if not duid:
            continue
        day  = row.get("DAY", "").strip()[:10]   # "YYYY/MM/DD"
        try:
            avail = float(row.get("PASAAVAILABILITY") or 0)
        except ValueError:
            avail = 0.0
        state = row.get("UNITSTATE", "").strip() or "Unknown"

        if duid not in duid_data:
            duid_data[duid] = {}
        duid_data[duid][day] = {"avail": avail, "state": state}

    results = []
    horizon = 30  # days to look ahead for return dates

    for duid, days in duid_data.items():
        unit = NEM_UNITS.get(duid, {})
        capacity = unit.get("capacity") or 0
        if capacity <= 0:
            continue  # skip units we don't know

        # Get today's or nearest availability
        sorted_days = sorted(days.keys())
        if not sorted_days:
            continue

        # Find the day entry closest to today
        today_entry = None
        for d in sorted_days:
            if d >= today_str:
                today_entry = days[d]
                break
        if today_entry is None:
            today_entry = days[sorted_days[-1]]

        avail_today = today_entry["avail"]
        state_today = today_entry["state"]

        # Skip fully available units with no derating
        if avail_today >= capacity and state_today in ("NoDeratings", "Unknown"):
            continue
        # Skip mothballed units (long-term, less interesting day-to-day)
        if state_today == "Mothballed":
            continue

        change_mw = avail_today - capacity

        # Find return date: first future day where avail recovers to >= capacity
        return_date = None
        change_date = None
        past_today  = False
        prev_avail  = avail_today

        for d in sorted_days:
            if d < today_str:
                continue
            if not past_today:
                past_today = True
                continue
            entry_avail = days[d]["avail"]
            # Return: availability increases significantly (>= capacity)
            if return_date is None and entry_avail >= capacity and prev_avail < capacity:
                return_date = d
            # Change date: first day availability changes from today
            if change_date is None and abs(entry_avail - avail_today) > 10:
                change_date = d
            prev_avail = entry_avail

        # Determine label
        if avail_today == 0 and state_today in ("Forced", "Unknown"):
            label = "Forced"
        elif avail_today == 0 and state_today == "Planned":
            label = "Planned"
        elif avail_today == 0:
            label = "Offline"
        elif return_date and avail_today > 0 and change_mw > 0:
            label = "Returning"
        elif avail_today < capacity * 0.95:
            label = "Derated"
        else:
            label = state_today

        results.append({
            "duid":        duid,
            "station":     unit.get("station", duid),
            "fuel":        unit.get("fuel", "Other"),
            "region":      unit.get("region", ""),
            "capacity":    int(capacity),
            "avail_today": int(avail_today),
            "state":       label,
            "change_mw":   int(change_mw),
            "change_date": change_date,
            "return_date": return_date,
        })

    # Sort by change_mw ascending (largest losses first)
    results.sort(key=lambda x: x["change_mw"])
    logger.info(f"scrape_mtpasa_outages: {len(results)} units with availability changes")
    return results



def scrape_historical_prices(date_str: str) -> dict:
    """
    Fetch 30-min trading prices for any date (YYYYMMDD format).
    Uses CURRENT directory for yesterday, ARCHIVE for older dates.
    Returns { region: [ {interval: "HH:MM", rrp: float} ] }
    """
    from datetime import datetime as _dt, timedelta as _td
    now_aest = datetime.now(AEST)
    today    = now_aest.date()
    yesterday = (now_aest - _td(days=1)).date()

    try:
        req_date = _dt.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return {}

    # Choose source directory
    if req_date >= yesterday:
        base_url = TRADING_CURRENT
    else:
        # Archive uses YYYYMM subdirectory
        ym = req_date.strftime("%Y%m")
        base_url = f"{TRADING_ARCHIVE}{ym}/"

    try:
        all_files = _list_hrefs(base_url)
    except Exception as e:
        logger.warning(f"scrape_historical_prices: listing failed for {date_str}: {e}")
        return {}

    date_files = sorted([u for u in all_files if date_str in u and "PUBLIC_TRADINGIS" in u.upper()])
    if not date_files:
        logger.warning(f"scrape_historical_prices: no files found for {date_str}")
        return {}

    prices: dict = {r: {} for r in NEM_REGIONS}
    for url in date_files:
        try:
            text = _read_zip(url)
            if not text:
                continue
            for row in _parse_aemo(text, "TRADING_PRICE"):
                if row.get("INVALIDFLAG", "0") != "0":
                    continue
                region = row.get("REGIONID", "").strip()
                if region not in prices:
                    continue
                try:
                    period = int(row["PERIODID"])
                    dt_str = row["SETTLEMENTDATE"]            # "YYYY/MM/DD HH:MM:SS"
                    dt     = datetime.strptime(dt_str, "%Y/%m/%d %H:%M:%S")
                    dt_adj = dt - timedelta(minutes=30)       # settlement end → start
                    label  = dt_adj.strftime("%H:%M")
                    prices[region][label] = round(float(row["RRP"]), 2)
                except (KeyError, ValueError):
                    continue
        except Exception as e:
            logger.debug(f"scrape_historical_prices: file error {url}: {e}")

    return {r: sorted([{"interval": k, "rrp": v} for k, v in d.items()],
                      key=lambda x: x["interval"])
            for r, d in prices.items() if d}




def scrape_historical_dispatch_prices(date_str: str) -> dict:
    """
    Fetch 5-min dispatch prices for a given date (YYYYMMDD format).
    - Today: use CURRENT (files appear at top of listing)
    - Any other date: use ARCHIVE/YYYYMM/ directly (avoids pagination issues
      with CURRENT which has 4000+ files and only returns the first page)
    Returns { region: [ {interval: "HH:MM", rrp: float} ] }
    """
    from datetime import datetime as _dt, timedelta as _td
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    try:
        req_dt = _dt.strptime(date_str, "%Y%m%d")
    except ValueError:
        return {}

    now_aest = datetime.now(AEST)
    current_ym = now_aest.strftime("%Y%m")  # e.g. "202603"

    if date_str[:6] == current_ym:
        # Current month: files are still in CURRENT directory (not yet archived)
        # AEMO CURRENT holds all files for the rolling ~84 days in one listing
        base_url = DISPATCH_IS_URL
    else:
        # Past month: use ARCHIVE which has a per-month subdirectory
        ym = date_str[:6]
        base_url = f"{DISPATCH_IS_ARCHIVE}{ym}/"

    try:
        all_files = _list_hrefs(base_url)
    except Exception as e:
        logger.warning(f"scrape_historical_dispatch_prices: listing failed for {date_str}: {e}")
        return {}

    date_files = sorted([u for u in all_files
                         if date_str in u and "PUBLIC_DISPATCHIS" in u.upper()])
    if not date_files:
        logger.warning(f"scrape_historical_dispatch_prices: no files found for {date_str} in {base_url}")
        return {}

    logger.info(f"scrape_historical_dispatch_prices: fetching {len(date_files)} files for {date_str}")
    prices: dict = {r: {} for r in NEM_REGIONS}

    def fetch_one(url):
        try:
            text = _read_zip(url)
            if not text:
                return {}
            result = {}
            for row in _parse_aemo(text, "DISPATCH_PRICE"):
                if row.get("INTERVENTION", "0") not in ("0", ""):
                    continue
                region = row.get("REGIONID", "").strip()
                if region not in NEM_REGIONS:
                    continue
                try:
                    rrp    = round(float(row["RRP"]), 2)
                    dt_str = row["SETTLEMENTDATE"]
                    dt     = datetime.strptime(dt_str, "%Y/%m/%d %H:%M:%S") - timedelta(minutes=5)
                    label  = dt.strftime("%H:%M")
                    if region not in result:
                        result[region] = {}
                    result[region][label] = rrp
                except (KeyError, ValueError):
                    pass
            return result
        except Exception as e:
            logger.debug(f"scrape_historical_dispatch_prices: file error {url}: {e}")
            return {}

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_one, u): u for u in date_files}
        for fut in _as_completed(futures):
            result = fut.result()
            for region, slots in result.items():
                prices[region].update(slots)

    return {r: sorted([{"interval": k, "rrp": v} for k, v in prices[r].items()],
                       key=lambda x: x["interval"])
            for r in NEM_REGIONS if prices[r]}

    prices: dict = {r: {} for r in NEM_REGIONS}

    def fetch_one(url):
        try:
            text = _read_zip(url)
            if not text:
                return {}
            result = {}
            for row in _parse_aemo(text, "DISPATCH_PRICE"):
                if row.get("INTERVENTION", "0") not in ("0", ""):
                    continue
                region = row.get("REGIONID", "").strip()
                if region not in NEM_REGIONS:
                    continue
                try:
                    rrp    = round(float(row["RRP"]), 2)
                    dt_str = row["SETTLEMENTDATE"]
                    dt     = datetime.strptime(dt_str, "%Y/%m/%d %H:%M:%S") - timedelta(minutes=5)
                    label  = dt.strftime("%H:%M")
                    if region not in result:
                        result[region] = {}
                    result[region][label] = rrp
                except (KeyError, ValueError):
                    pass
            return result
        except Exception as e:
            logger.debug(f"scrape_historical_dispatch_prices: file error {url}: {e}")
            return {}

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_one, u): u for u in date_files}
        for fut in as_completed(futures):
            result = fut.result()
            for region, slots in result.items():
                prices[region].update(slots)

    return {r: sorted([{"interval": k, "rrp": v} for k, v in prices[r].items()],
                      key=lambda x: x["interval"])
            for r in NEM_REGIONS if prices[r]}


def scrape_yesterday() -> dict:
    """
    Fetch yesterday's full-day data from CURRENT directory files.
    Returns price, demand, fuel_mix, ic_flows all keyed by region/interval.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
    now_aest    = datetime.now(AEST)
    yesterday   = (now_aest - timedelta(days=1)).date()
    ystr        = yesterday.strftime("%Y%m%d")
    logger.info(f"scrape_yesterday: fetching {ystr}")

    # ── Prices from TradingIS (30-min, firm) ──────────────────────────────────
    try:
        all_trading = _list_hrefs(TRADING_CURRENT)
        yest_trading = sorted([u for u in all_trading if ystr in u and "PUBLIC_TRADINGIS" in u.upper()])
    except Exception as e:
        logger.warning(f"scrape_yesterday: TradingIS listing failed: {e}")
        yest_trading = []

    prices: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    for url in yest_trading:
        try:
            text = _read_zip(url)
            if not text:
                continue
            for row in _parse_aemo(text, "TRADING_PRICE"):
                region = row.get("REGIONID", "")
                if region not in NEM_REGIONS:
                    continue
                if row.get("INVALIDFLAG", "0") not in ("0", ""):
                    continue
                dt_str = row.get("SETTLEMENTDATE", "")
                rrp_str = row.get("RRP", "")
                if not dt_str or not rrp_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=30)
                    if dt.date() == yesterday:
                        prices[region][dt.strftime("%H:%M")] = round(float(rrp_str), 2)
                except (ValueError, TypeError):
                    pass
        except Exception:
            pass

    # ── Demand + IC + fuel proxies from DispatchIS (5-min) ───────────────────
    try:
        all_dispatch = _list_hrefs(DISPATCH_IS_URL)
        yest_dispatch = sorted([u for u in all_dispatch if ystr in u and "PUBLIC_DISPATCHIS" in u.upper()])
    except Exception as e:
        logger.warning(f"scrape_yesterday: DispatchIS listing failed: {e}")
        yest_dispatch = []

    demand:   dict[str, dict] = {r: {} for r in NEM_REGIONS}
    op_demand: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    ic_flows: dict[str, dict] = {}
    ss_solar: dict[str, dict] = {r: {} for r in NEM_REGIONS}
    ss_wind:  dict[str, dict] = {r: {} for r in NEM_REGIONS}

    def _fetch_dispatch_yesterday(url):
        try:
            text = _read_zip(url)
            if not text:
                return []
            pts = []
            for row in _parse_aemo(text, "DISPATCH_REGIONSUM"):
                region = row.get("REGIONID", "")
                if region not in NEM_REGIONS:
                    continue
                if row.get("INTERVENTION", "0") not in ("0", ""):
                    continue
                dt_str = row.get("SETTLEMENTDATE", "")
                if not dt_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=5)
                    if dt.date() != yesterday:
                        continue
                    label = dt.strftime("%H:%M")
                    d = row.get("TOTALDEMAND", "")
                    od = row.get("DEMAND_AND_NONSCHEDGEN", "")
                    sol = row.get("SS_SOLAR_CLEAREDMW", "")
                    win = row.get("SS_WIND_CLEAREDMW", "")
                    if d:  pts.append(("demand",   region, label, round(float(d), 1)))
                    if od: pts.append(("op_demand", region, label, round(float(od), 1)))
                    if sol: pts.append(("solar",   region, label, round(float(sol), 1)))
                    if win: pts.append(("wind",    region, label, round(float(win), 1)))
                except (ValueError, TypeError):
                    pass
            for row in _parse_aemo(text, "DISPATCH_INTERCONNECTORRES"):
                ic_id = row.get("INTERCONNECTORID", "").strip()
                if not ic_id:
                    continue
                if row.get("INTERVENTION", "0") not in ("0", ""):
                    continue
                dt_str = row.get("SETTLEMENTDATE", "")
                flow_str = row.get("MWFLOW", "")
                if not dt_str or not flow_str:
                    continue
                try:
                    dt = datetime.fromisoformat(dt_str.replace("/", "-")) - timedelta(minutes=5)
                    if dt.date() == yesterday:
                        pts.append(("ic", ic_id, dt.strftime("%H:%M"), round(float(flow_str), 1)))
                except (ValueError, TypeError):
                    pass
            return pts
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_fetch_dispatch_yesterday, u): u for u in yest_dispatch}
        for fut in _as_completed(futs):
            for item in fut.result():
                kind = item[0]
                if kind == "demand":
                    demand[item[1]][item[2]] = item[3]
                elif kind == "op_demand":
                    op_demand[item[1]][item[2]] = item[3]
                elif kind == "solar":
                    ss_solar[item[1]][item[2]] = item[3]
                elif kind == "wind":
                    ss_wind[item[1]][item[2]] = item[3]
                elif kind == "ic":
                    ic_id = item[1]
                    if ic_id not in ic_flows:
                        ic_flows[ic_id] = {}
                    ic_flows[ic_id][item[2]] = item[3]

    def _to_series(d):
        return {r: [{"interval": k, "demand": v} for k, v in sorted(s.items())] for r, s in d.items() if s}
    def _to_mw_series(d):
        return {r: [{"interval": k, "mw": v} for k, v in sorted(s.items())] for r, s in d.items() if s}
    def _to_rrp_series(d):
        return {r: [{"interval": k, "rrp": v} for k, v in sorted(s.items())] for r, s in d.items() if s}
    def _to_flow_series(d):
        return {ic: [{"interval": k, "flow": v} for k, v in sorted(s.items())] for ic, s in d.items() if s}

    total_pts = sum(len(v) for v in prices.values()) + sum(len(v) for v in demand.values())
    logger.info(f"scrape_yesterday: {ystr} — price_pts={sum(len(v) for v in prices.values())} demand_pts={sum(len(v) for v in demand.values())}")
    return {
        "date":       yesterday.strftime("%Y-%m-%d"),
        "label":      yesterday.strftime("%A %-d %b"),
        "prices":     _to_rrp_series(prices),
        "demand":     _to_series(demand),
        "op_demand":  _to_series(op_demand),
        "ss_solar":   _to_mw_series(ss_solar),
        "ss_wind":    _to_mw_series(ss_wind),
        "ic_flows":   _to_flow_series(ic_flows),
        "fuel_colors": FUEL_COLORS,
    }


# BOM weather stations matching the screenshot locations (one per NEM region)
BOM_STATIONS = {
    "QLD1": {"name": "Archerfield",            "geohash": "r7hgdp"},
    "NSW1": {"name": "Bankstown",              "geohash": "r3gx2u"},
    "VIC1": {"name": "Melbourne",              "geohash": "r1r0fs"},
    "SA1":  {"name": "Adelaide (West Terrace)","geohash": "r1f91f"},
}

def _fetch_bom_station(region: str, station: dict) -> tuple:
    """Fetch BOM forecast for a single station. Returns (region, result_dict)."""
    import requests as req
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NEM-Dashboard/1.0)",
        "Accept": "application/json",
        "Referer": "https://www.bom.gov.au/",
    }
    url = f"https://api.weather.bom.gov.au/v1/locations/{station['geohash']}/forecasts/daily"
    try:
        r = req.get(url, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json().get("data", [])
        days = []
        for day in data:
            date_str = day.get("date", "")[:10]
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                dow = dt.strftime("%a")
                date_label = dt.strftime("%-d %b")
            except ValueError:
                dow = ""; date_label = date_str
            days.append({
                "date":        date_str,
                "day_of_week": dow,
                "date_label":  date_label,
                "temp_max":    day.get("temp_max"),
                "temp_min":    day.get("temp_min"),
                "short_text":  day.get("short_text", ""),
                "rain_chance": day.get("rain", {}).get("chance"),
            })
        logger.info(f"BOM weather {region} ({station['name']}): {len(days)} days")
        return region, {"name": station["name"], "days": days}
    except Exception as e:
        logger.warning(f"BOM weather fetch failed for {region}: {e}")
        return region, {"name": station["name"], "days": []}


def scrape_bom_weather() -> dict:
    """
    Fetch 7-day daily forecasts from BOM in parallel (one request per NEM region station).
    Returns { region: { name, days: [{date, day_of_week, temp_max, temp_min, short_text, rain_chance}] } }
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    result = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_fetch_bom_station, region, station): region
            for region, station in BOM_STATIONS.items()
        }
        for future in as_completed(futures, timeout=15):
            try:
                region, data = future.result()
                result[region] = data
            except Exception as e:
                region = futures[future]
                logger.warning(f"BOM weather future failed for {region}: {e}")
                result[region] = {"name": BOM_STATIONS[region]["name"], "days": []}
    return result


def _safe_mtpasa_outages() -> list:
    """Run scrape_mtpasa_outages in a thread with a hard timeout so it
    cannot block scrape_slow if NEMWeb is slow."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(scrape_mtpasa_outages)
            return fut.result(timeout=30)
    except FuturesTimeout:
        logger.warning("scrape_mtpasa_outages timed out after 30s — returning empty list")
        return []
    except Exception as e:
        logger.warning(f"scrape_mtpasa_outages failed: {e}")
        return []


def scrape_slow() -> dict:
    """
    Week-ahead ST PASA demand forecast only.
    Generators/fuel mix now handled by scrape_gen (medium cache).
    """
    logger.info("scrape_slow starting...")
    pasa = scrape_stpasa_demand()

    # Weather forecast from BOM for D+ page
    weather_data = {}
    try:
        weather_data = scrape_bom_weather()
    except Exception as e:
        logger.warning(f"scrape_bom_weather failed: {e}")

    # Extract tomorrow's demand from STPASA for Day Ahead page
    now_aest = datetime.now(AEST).replace(tzinfo=None)
    tomorrow = (now_aest + timedelta(days=1)).date()
    tomorrow_demand_stpasa: dict[str, list] = {}
    for region, pts in pasa.items():
        series = []
        for pt in pts:
            label_full = pt.get("interval", "")
            d50 = pt.get("demand_50")
            if not label_full or d50 is None:
                continue
            try:
                dt = datetime.strptime(label_full, "%Y-%m-%d %H:%M")
                if dt.date() == tomorrow:
                    series.append({"interval": dt.strftime("%H:%M"), "demand": d50})
            except ValueError:
                pass
        if series:
            tomorrow_demand_stpasa[region] = series

    # MTPASA outages
    mtpasa = []
    try:
        mtpasa = _safe_mtpasa_outages()
    except Exception as e:
        logger.warning(f"mtpasa_outages in scrape_slow failed: {e}")

    logger.info("scrape_slow done")
    return {
        "timestamp":              datetime.now(timezone.utc).isoformat(),
        "stpasa_demand":          pasa,
        "tomorrow_demand_stpasa": tomorrow_demand_stpasa,
        "yesterday":              {},
        "weather":                weather_data,
        "fuel_colors":            FUEL_COLORS,
        "all_fuels":              ALL_FUELS,
        "mtpasa_outages":         mtpasa,
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
