# volland_test/analyze_captures.py
# Analyze previously captured JSON files

import json
import sys
from pathlib import Path
from collections import defaultdict

CAPTURES_DIR = Path(__file__).parent / "captures"


def analyze_file(filepath: Path):
    """Analyze a raw captures JSON file."""
    print(f"\n{'='*60}")
    print(f"Analyzing: {filepath.name}")
    print('='*60)

    with open(filepath) as f:
        data = json.load(f)

    fetch = data.get("fetch", [])
    xhr = data.get("xhr", [])
    ws = data.get("ws", [])

    print(f"\nTotal requests: {len(fetch)} fetch, {len(xhr)} xhr, {len(ws)} ws")

    # Categorize by URL patterns
    categories = defaultdict(list)
    keywords = ["exposure", "gamma", "vanna", "charm", "delta", "theta", "volbeta", "spot-vol", "dealer", "statistic"]

    for item in fetch + xhr:
        url = (item.get("url") or "").lower()
        body = item.get("body") or ""

        # Skip noise
        if any(x in url for x in ("sentry", "gleap", "google", "intercom", "analytics")):
            continue

        # Find matching keywords
        combined = url + body[:1000].lower()
        matches = [k for k in keywords if k in combined]

        if matches:
            key = ", ".join(sorted(set(matches)))
            categories[key].append({
                "url": item.get("url", ""),
                "body_len": len(body),
                "body": body
            })

    print(f"\nCategorized endpoints:")
    for cat, items in sorted(categories.items(), key=lambda x: -len(x[1])):
        print(f"\n  [{cat}] - {len(items)} request(s)")
        for i, item in enumerate(items[:3]):
            print(f"    URL: {item['url'][:100]}...")

            # Try to parse body
            try:
                parsed = json.loads(item['body'])
                if isinstance(parsed, dict):
                    keys = list(parsed.keys())
                    print(f"    Keys: {keys[:8]}")

                    # Check for identifying fields
                    if "greek" in parsed:
                        print(f"    → greek: {parsed['greek']}")
                    if "type" in parsed:
                        print(f"    → type: {parsed['type']}")
                    if "expiration" in str(parsed).lower():
                        # Try to find expiration info
                        for k, v in parsed.items():
                            if "expir" in k.lower():
                                print(f"    → {k}: {v}")
                    if "items" in parsed:
                        print(f"    → items count: {len(parsed['items'])}")
                        if parsed['items']:
                            print(f"    → sample item: {str(parsed['items'][0])[:80]}")
                    if "currentPrice" in parsed:
                        print(f"    → currentPrice: {parsed['currentPrice']}")
            except:
                pass


def find_widget_identifiers(filepath: Path):
    """Look for fields that could identify different widgets."""
    print(f"\n{'='*60}")
    print("LOOKING FOR WIDGET IDENTIFIERS")
    print('='*60)

    with open(filepath) as f:
        data = json.load(f)

    potential_ids = defaultdict(set)

    for item in data.get("fetch", []) + data.get("xhr", []):
        url = item.get("url") or ""
        body = item.get("body") or ""

        if "exposure" not in url.lower():
            continue

        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                # Look for any field that might identify the widget type
                for key in ["greek", "type", "kind", "expiration", "expirations", "name", "widget", "chart"]:
                    if key in parsed:
                        potential_ids[key].add(str(parsed[key])[:100])

                # Check URL parameters
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(url).query)
                for k, v in qs.items():
                    potential_ids[f"url:{k}"].add(str(v[0]) if v else "")
        except:
            pass

    print("\nPotential identifying fields found:")
    for field, values in sorted(potential_ids.items()):
        print(f"  {field}: {values}")


def main():
    # Find the most recent capture file
    files = sorted(CAPTURES_DIR.glob("raw_captures_*.json"), reverse=True)

    if not files:
        print("No capture files found in", CAPTURES_DIR)
        print("Run test_scraper.py first to capture data.")
        return

    # Analyze most recent or specified file
    if len(sys.argv) > 1:
        filepath = Path(sys.argv[1])
    else:
        filepath = files[0]
        print(f"Analyzing most recent file: {filepath.name}")
        if len(files) > 1:
            print(f"(Other files available: {[f.name for f in files[1:5]]})")

    if not filepath.exists():
        print(f"File not found: {filepath}")
        return

    analyze_file(filepath)
    find_widget_identifiers(filepath)


if __name__ == "__main__":
    main()
