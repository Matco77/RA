# V5.2
import os
import argparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import time
from datetime import datetime
import xml.etree.ElementTree as ET

# ==========================================================
# V5.2 (generic fallback)
# Pipeline:
#  1) Search by radius steps for CURRENT explicit DCs (building=data_center or telecom=data_center).
#     - If found, analyze history (start_date -> first explicit DC tag -> first DC-like tag).
#  2) If not found, expand radius and FALL BACK to generic shells ONLY
#     - Accept candidates whose current tag is building=<allowed_generic_values> (default: yes).
#     - Reject anything with a more specific classification (office, industrial, apartments, etc.).
#     - Optionally require a usable date signal (start_date or DC-ever) to accept; otherwise keep expanding radius.
#  3) Deterministic pick among candidates: most recent relevant change timestamp (stable tie-breaker).
#  4) Output inferred year + provenance; record the rule and radius used.
# ==========================================================

def _build_session(max_retries:int = 5, backoff_factor:float = 1.2) -> requests.Session:
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "osm-dc-analyzer/1.0 (+https)"})
    return s

DEFAULT_OVERPASS_URL = os.getenv("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
DEFAULT_OSM_API_BASE = os.getenv("OSM_API_BASE", "https://api.openstreetmap.org/api/0.6")
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "90"))
DEFAULT_SLEEP_OVERPASS = float(os.getenv("SLEEP_OVERPASS", "0.5"))
DEFAULT_SLEEP_HISTORY = float(os.getenv("SLEEP_HISTORY", "1.0"))
DEFAULT_RADIUS_STEPS = os.getenv("RADIUS_STEPS", "50,100,200")
SESSION = _build_session()

# ---------- Tag helpers ----------
DC_EXPLICIT_KEYS = (("building","data_center"), ("telecom","data_center"))

def _norm(s: str) -> str:
    return ''.join(ch for ch in str(s).lower() if ch.isalnum())

DC_LIKE_VALUES = {"datacenter","datacentre","datacentreuk","datacentreca","datacentreau"}
DC_LIKE_KEYS = ["building","building:use","telecom","industrial"]

def has_explicit_dc_tag(tags: dict) -> bool:
    if not tags: return False
    t = {k.lower(): str(v).lower() for k,v in tags.items()}
    return any(t.get(k) == v for k, v in DC_EXPLICIT_KEYS)

def has_dc_like_tag(tags: dict) -> bool:
    if not tags: return False
    t = {k.lower(): _norm(v) for k,v in tags.items()}
    for k in DC_LIKE_KEYS:
        v = t.get(k)
        if v in DC_LIKE_VALUES:
            return True
    return False

def parse_start_date(date_string):
    if not date_string:
        return None, None, None
    date_string = str(date_string).strip()
    try:
        formats = ['%Y-%m-%d','%Y-%m','%Y','%m/%d/%Y','%d.%m.%Y','%d/%m/%Y']
        for fmt in formats:
            try:
                parsed = datetime.strptime(date_string, fmt)
                return (date_string, parsed.strftime('%Y-%m-%d'), parsed.year)
            except ValueError:
                continue
        if date_string.isdigit() and len(date_string) == 4:
            year = int(date_string)
            if 1900 <= year <= 2100:
                return date_string, f"{year}-01-01", year
        return date_string, None, None
    except Exception:
        return date_string, None, None

def _plausible_year(y):
    try:
        y = int(y); return 1900 <= y <= 2100
    except Exception:
        return False

# ---------- Overpass ----------
def find_buildings_at_coordinates(lat, lon, radius):
    overpass_query = f"""
    [out:json][timeout:25];
    (
      way["building"](around:{radius},{lat},{lon});
      relation["building"](around:{radius},{lat},{lon});
    );
    out meta;
    """
    try:
        response = SESSION.post(DEFAULT_OVERPASS_URL, data=overpass_query, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get('elements', [])
    except Exception as e:
        print(f"Error querying coordinates {lat}, {lon} @ {radius}m: {e}")
        return []

def get_current_building_info(element_type, element_id):
    overpass_query = f"""
    [out:json][timeout:25];
    {element_type}({element_id});
    out meta;
    """
    try:
        time.sleep(DEFAULT_SLEEP_OVERPASS)
        response = SESSION.post(DEFAULT_OVERPASS_URL, data=overpass_query, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        elements = data.get('elements', [])
        if not elements:
            return {}
        element = elements[0]
        tags = element.get('tags', {})
        start_date_raw = start_date_standardized = start_date_year = None
        date_tags = ['start_date', 'opening_date', 'opened', 'construction_date', 'start_date:edtf']
        src_tag = ''
        for tag in date_tags:
            if tags.get(tag):
                start_date_raw, start_date_standardized, start_date_year = parse_start_date(tags[tag])
                src_tag = tag
                break
        return {
            'current_tags': tags,
            'start_date_raw': start_date_raw or '',
            'start_date_standardized': start_date_standardized or '',
            'start_date_year': start_date_year or '',
            'start_date_source_tag': src_tag
        }
    except Exception as e:
        print(f"Error getting current info for {element_type} {element_id}: {e}")
        return {}

# ---------- History ----------
RELEVANT_KEYS = {
    "building","building:use","industrial","amenity","office","landuse",
    "name","operator","brand","telecom","power",
    "start_date","opening_date","opened","start_date:edtf"
}
IGNORED_PREFIXES = (
    "addr:","source","note","fixme","wheelchair","contact:","phone",
    "email","website","wikidata","wikipedia","short_name","alt_name"
)

def _filter_relevant(d: dict) -> dict:
    if not d: return {}
    out = {}
    for k,v in d.items():
        if any(k.startswith(pref) for pref in IGNORED_PREFIXES):
            continue
        if k in RELEVANT_KEYS:
            out[k] = v
    return out

def analyze_building_history(element_type, element_id):
    history_url = f"{DEFAULT_OSM_API_BASE}/{element_type}/{element_id}/history"
    try:
        time.sleep(DEFAULT_SLEEP_HISTORY)
        response = SESSION.get(history_url, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        versions = []
        for elem in root.findall(f'.//{element_type}'):
            tags = {}
            for tag in elem.findall('.//tag'):
                tags[tag.get('k')] = tag.get('v')
            versions.append({
                'version': int(elem.get('version')),
                'timestamp': elem.get('timestamp'),
                'user': elem.get('user'),
                'changeset': elem.get('changeset'),
                'tags': tags,
                'filtered_tags': _filter_relevant(tags),
            })
        if not versions:
            return None

        versions.sort(key=lambda x: x['version'])

        prev_rel = {}
        last_rel_ts = versions[0]['timestamp']
        for v in versions:
            rel = v['filtered_tags']
            if rel != prev_rel:
                last_rel_ts = v['timestamp']
                prev_rel = rel

        current_info = get_current_building_info(element_type, element_id)
        current_tags = current_info.get('current_tags', {})

        is_dc_current = has_explicit_dc_tag(current_tags)

        first_dc_explicit_ts = None
        for v in versions:
            if has_explicit_dc_tag(v["tags"]):
                first_dc_explicit_ts = v["timestamp"]
                break
        first_dc_explicit_year = int(first_dc_explicit_ts[:4]) if first_dc_explicit_ts else ''

        first_dc_like_ts = None
        for v in versions:
            if has_dc_like_tag(v["tags"]):
                first_dc_like_ts = v["timestamp"]
                break
        first_dc_like_year = int(first_dc_like_ts[:4]) if first_dc_like_ts else ''

        first_ts = versions[0]['timestamp']
        first_ts_readable = datetime.fromisoformat(first_ts.replace('Z','+00:00')).strftime('%Y-%m-%d %H:%M:%S')
        last_change = versions[-1]
        last_ts = last_change['timestamp']
        last_ts_readable = datetime.fromisoformat(last_ts.replace('Z','+00:00')).strftime('%Y-%m-%d %H:%M:%S')

        op_year = ''
        op_src = ''
        sdy = current_info.get('start_date_year', '')
        if _plausible_year(sdy):
            op_year = int(sdy); op_src = 'start_date'
        elif _plausible_year(first_dc_explicit_year):
            op_year = int(first_dc_explicit_year); op_src = 'dc_first_seen_explicit'
        elif _plausible_year(first_dc_like_year):
            op_year = int(first_dc_like_year); op_src = 'dc_first_seen_like'

        datacenter_tags_after = {}
        for k in ['name','building','landuse','industrial','operator','amenity','office','description','use','facility','power','telecom','start_date','opening_date','construction','opened']:
            if k in last_change['tags']:
                datacenter_tags_after[k] = last_change['tags'][k]

        return {
            'building_id': f"{element_type}/{element_id}",
            'last_change_timestamp': last_ts,
            'last_change_readable': last_ts_readable,
            'last_change_year': int(last_ts[:4]),
            'last_change_user': last_change['user'],
            'last_change_changeset': last_change['changeset'],
            'last_change_version': last_change['version'],
            'tags_before_change': '',
            'tags_after_change': str(datacenter_tags_after),
            'is_datacenter_now': has_explicit_dc_tag(last_change['tags']),
            'total_versions': len(versions),
            'first_timestamp': first_ts,
            'first_timestamp_readable': first_ts_readable,
            'start_date_raw': current_info.get('start_date_raw', ''),
            'start_date_standardized': current_info.get('start_date_standardized', ''),
            'start_date_year': current_info.get('start_date_year', ''),
            'start_date_source_tag': current_info.get('start_date_source_tag', ''),
            'current_name': current_tags.get('name', ''),
            'current_operator': current_tags.get('operator', ''),
            'current_building_type': current_tags.get('building', ''),
            'is_datacenter_current': is_dc_current,
            'last_change_relevant_timestamp': last_rel_ts,
            'dc_first_seen_explicit_timestamp': first_dc_explicit_ts or '',
            'dc_first_seen_explicit_year': first_dc_explicit_year,
            'dc_first_seen_like_timestamp': first_dc_like_ts or '',
            'dc_first_seen_like_year': first_dc_like_year,
            'operational_year_inferred': op_year,
            'operational_year_source': op_src,
        }

    except Exception as e:
        print(f"Error getting history for {element_type} {element_id}: {e}")
        return None

# ---------- Pipeline ----------
def process_csv_coordinates(input_csv, output_csv, radius_steps, generic_allow, require_signal_for_generic: bool):
    results = []
    with open(input_csv, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    print(f"Input : {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Radius steps: {radius_steps}")
    print(f"Generic fallback allow-list: {sorted(generic_allow)} (require_signal_for_generic={require_signal_for_generic})")
    print(f"Processing {len(rows)} locations...")

    for i, row in enumerate(rows, 1):
        print(f"\n[{i}/{len(rows)}] {row.get('name', 'Unknown')}")
        try:
            lat = float(row['best_latitude'])
            lon = float(row['best_longitude'])

            selected = None
            used_radius = None
            rule_used = None

            # Pass A: CURRENT explicit DC only
            for r in radius_steps:
                elements = find_buildings_at_coordinates(lat, lon, radius=r)
                if not elements:
                    print(f"  - No buildings @ {r}m")
                    continue

                dc_elems = [e for e in elements if has_explicit_dc_tag(e.get('tags', {}))]
                if not dc_elems:
                    print(f"  - No CURRENT explicit DC @ {r}m")
                    continue

                candidates = []
                for e in dc_elems:
                    elem_type = e['type']; elem_id = e['id']
                    print(f"    • Candidate {elem_type}/{elem_id} (current DC) → analyze history...")
                    info = analyze_building_history(elem_type, elem_id)
                    if info:
                        candidates.append(info)

                if candidates:
                    selected = max(candidates, key=lambda x: x.get('last_change_relevant_timestamp',''))
                    used_radius = r
                    rule_used = 'current_explicit_dc_tag'
                    print(f"  ✓ Selected {selected['building_id']} @ radius {r}m [current_explicit_dc_tag]")
                    break

            # Pass B: Fallback to GENERIC shells (building=yes or user-provided list)
            if selected is None:
                for r in radius_steps:
                    elements = find_buildings_at_coordinates(lat, lon, radius=r)
                    if not elements:
                        print(f"  - No buildings @ {r}m")
                        continue

                    generic_elems = []
                    for e in elements:
                        tags = {k.lower(): str(v).lower() for k,v in (e.get('tags', {}) or {}).items()}
                        b = tags.get('building')
                        if b in generic_allow:
                            generic_elems.append(e)

                    if not generic_elems:
                        print(f"  - No GENERIC (building in {generic_allow}) @ {r}m")
                        continue

                    candidates = []
                    for e in generic_elems:
                        elem_type = e['type']; elem_id = e['id']
                        print(f"    • Generic candidate {elem_type}/{elem_id} (building in {generic_allow}) → analyze history...")
                        info = analyze_building_history(elem_type, elem_id)
                        if not info:
                            continue
                        has_signal = False
                        if info.get('start_date_year'): has_signal = True
                        if info.get('dc_first_seen_explicit_year'): has_signal = True
                        if info.get('dc_first_seen_like_year'): has_signal = True
                        if require_signal_for_generic and not has_signal:
                            print("      ↳ Skipped (no start_date/first-DC-ever/first-DC-like signal)")
                            continue
                        candidates.append(info)

                    if candidates:
                        selected = max(candidates, key=lambda x: x.get('last_change_relevant_timestamp',''))
                        used_radius = r
                        rule_used = 'fallback_generic_building'
                        print(f"  ✓ Selected {selected['building_id']} @ radius {r}m [fallback_generic_building]")
                        break

            # No candidate found
            if selected is None:
                print("  ✗ No acceptable candidate within max radius")
                result = _empty_result(row, status="No acceptable candidate within max radius")
                results.append(result)
                continue

            # Build output record
            result = row.copy()
            result.update(selected)
            result['selection_rule_used'] = rule_used
            result['search_radius_used'] = used_radius
            result['status'] = 'Success'
            results.append(result)

        except Exception as e:
            print(f"  Error processing row: {e}")
            result = _empty_result(row, status=f"Error: {e}")
            results.append(result)

    # Write results
    if results:
        with open(output_csv, 'w', newline='', encoding='utf-8') as file:
            fieldnames = list(results[0].keys())
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"\n✓ Results saved to {output_csv}")

def _empty_result(row, status=""):
    base = {
        'building_id': '',
        'last_change_timestamp': '',
        'last_change_readable': '',
        'last_change_year': '',
        'last_change_user': '',
        'last_change_changeset': '',
        'last_change_version': '',
        'tags_before_change': '',
        'tags_after_change': '',
        'is_datacenter_now': False,
        'total_versions': 0,
        'first_timestamp': '',
        'first_timestamp_readable': '',
        'start_date_raw': '',
        'start_date_standardized': '',
        'start_date_year': '',
        'start_date_source_tag': '',
        'current_name': '',
        'current_operator': '',
        'current_building_type': '',
        'is_datacenter_current': False,
        'last_change_relevant_timestamp': '',
        'dc_first_seen_explicit_timestamp': '',
        'dc_first_seen_explicit_year': '',
        'dc_first_seen_like_timestamp': '',
        'dc_first_seen_like_year': '',
        'operational_year_inferred': '',
        'operational_year_source': '',
        'selection_rule_used': '',
        'search_radius_used': '',
        'status': status
    }
    r = row.copy()
    r.update(base)
    return r

# ---------- CLI ----------
def _parse_args():
    p = argparse.ArgumentParser(description="V5.2 generic fallback: current DC first, then building=yes at larger radius, history-based dates.")
    # YOUR EXACT PATHS:
    p.add_argument("-i", "--input", default=r"C:\Users\Bova\Downloads\RAproject\cleaning\datacentermap_clean.csv",
                   help="Input CSV with 'best_latitude' and 'best_longitude' columns")
    p.add_argument("-o", "--output", default=r"C:\Users\Bova\Downloads\RAproject\cleaning\historyV5.2.csv",
                   help="Output CSV filename")
    p.add_argument("--radius-steps", default=DEFAULT_RADIUS_STEPS,
                   help="Comma-separated search radii in meters, e.g. '50,100,200'")
    p.add_argument("--generic-allow", default="yes",
                   help="Comma-separated building values to allow in generic fallback (default: 'yes')")
    p.add_argument("--require-signal-for-generic", action="store_true",
                   help="If set, generic fallback will only accept candidates with start_date or DC-ever signals")
    p.add_argument("--overpass-url", default=DEFAULT_OVERPASS_URL, help="Overpass API URL")
    p.add_argument("--osm-api-base", default=DEFAULT_OSM_API_BASE, help="OSM API base URL")
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds")
    p.add_argument("--sleep-overpass", type=float, default=DEFAULT_SLEEP_OVERPASS, help="Sleep seconds before Overpass calls")
    p.add_argument("--sleep-history", type=float, default=DEFAULT_SLEEP_HISTORY, help="Sleep seconds before history calls")
    return p.parse_args()

def _apply_overrides_from_args(args):
    global DEFAULT_OVERPASS_URL, DEFAULT_OSM_API_BASE, DEFAULT_TIMEOUT, DEFAULT_SLEEP_OVERPASS, DEFAULT_SLEEP_HISTORY
    DEFAULT_OVERPASS_URL = args.overpass_url
    DEFAULT_OSM_API_BASE = args.osm_api_base
    DEFAULT_TIMEOUT = args.timeout
    DEFAULT_SLEEP_OVERPASS = args.sleep_overpass
    DEFAULT_SLEEP_HISTORY = args.sleep_history

def _parse_radius_steps(s: str):
    steps = []
    for part in str(s).split(','):
        part = part.strip()
        if not part: continue
        try: steps.append(int(part))
        except ValueError: pass
    return steps or [50, 100, 200]

def _parse_generic_allow(s: str):
    vals = []
    for part in str(s).split(','):
        part = part.strip().lower()
        if part:
            vals.append(part)
    return set(vals or ["yes"])

if __name__ == "__main__":
    args = _parse_args()
    _apply_overrides_from_args(args)
    radius_steps = _parse_radius_steps(args.radius_steps)
    generic_allow = _parse_generic_allow(args.generic_allow)
    process_csv_coordinates(
        args.input, args.output, radius_steps,
        generic_allow=generic_allow,
        require_signal_for_generic=args.require_signal_for_generic
    )
