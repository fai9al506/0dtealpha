"""
Search Discord JSON exports for overvixing, vol events, and bottom-catching rally messages.
"""
import json
import os
import re
import sys
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Fix encoding for Windows console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

OUTPUT_DIR = Path(r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output")

# Keywords to search for (case-insensitive)
KEYWORDS = [
    r"overvix",
    r"over[\s-]?vix",
    r"overvixxed",
    r"vol\s*event",
    r"vol\s*signal",
    r"vol\s*crush",
    r"beach\s*ball",
    r"bottom",
    r"bounce",
    r"snap\s*back",
    r"snapback",
    r"rally",
    r"mean\s*revert",
    r"mean\s*reversion",
    r"compression",
    r"capitulat",
    r"flush",
    r"washout",
    r"wash\s*out",
    r"reversal",
    r"vol\s*spike",
    r"vix\s*spike",
    r"vix\s*crush",
    r"vol\s*unwind",
    r"gamma\s*unwind",
    r"short\s*covering",
    r"short\s*squeeze",
    r"real\s*bottom",
    r"fake\s*bounce",
    r"dead\s*cat",
    r"catching\s*kniv",
    r"knife\s*catch",
    r"buy\s*the\s*dip",
    r"btfd",
    r"sell[\s-]?off",
    r"liquidat",
    r"panic",
    r"fear",
    r"capitul",
    r"squeeze",
    r"recovery",
    r"inflect",
    r"pivot",
    r"turn",         # too generic alone, but combined with context useful
    r"long\s*here",
    r"going\s*long",
    r"loaded\s*(calls|longs|puts)",
    r"vol\s*term\s*structure",
    r"contango",
    r"backwardation",
    r"vix3m",
    r"vvix",
]

# Expert users to flag
EXPERTS = {"Apollo", "Wizard", "LordHelmet", "Phoenix", "Zack", "TheEdge",
           "apollo", "wizard", "lordhelmet", "phoenix", "zack", "theedge",
           "VollandDev", "vollanddev"}

# Compile a single mega-pattern for efficiency
PATTERN = re.compile("|".join(KEYWORDS), re.IGNORECASE)

# More specific patterns that are highly relevant (to flag separately)
HIGH_RELEVANCE = re.compile(
    r"overvix|over[\s-]?vix|vol\s*event|vol\s*signal|vol\s*crush|beach\s*ball|"
    r"snap\s*back|snapback|mean\s*revert|capitulat|flush|washout|compression|"
    r"real\s*bottom|fake\s*bounce|dead\s*cat|btfd|vix3m|vvix|contango|backwardation|"
    r"vol\s*term|short\s*squeeze|gamma\s*unwind|vol\s*unwind",
    re.IGNORECASE
)

KSA = timezone(timedelta(hours=3))
ET_OFFSET = timedelta(hours=7)  # KSA - 7 = ET

def parse_ts(ts_str):
    """Parse ISO timestamp, return (KSA datetime, ET datetime)"""
    try:
        dt = datetime.fromisoformat(ts_str)
        # Ensure it's in KSA
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KSA)
        et = dt - ET_OFFSET
        return dt, et
    except:
        return None, None

def get_author(msg):
    """Get nickname or username"""
    author = msg.get("author", {})
    return author.get("nickname") or author.get("name") or "Unknown"

def is_expert(author_name):
    """Check if author is a known expert"""
    for e in EXPERTS:
        if e.lower() in author_name.lower():
            return True
    return False

def search_file(filepath):
    """Search a single JSON file for relevant messages"""
    results = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"  ERROR reading {filepath}: {e}")
        return results

    messages = data.get("messages", [])
    channel = data.get("channel", {}).get("name", "unknown")

    for msg in messages:
        content = msg.get("content", "")
        if not content or len(content.strip()) < 5:
            continue

        # Check embeds too (bot messages often have content in embeds)
        embed_text = ""
        for emb in msg.get("embeds", []):
            embed_text += " " + emb.get("description", "") + " " + emb.get("title", "")

        full_text = content + embed_text

        match = PATTERN.search(full_text)
        if not match:
            continue

        author = get_author(msg)
        ts_str = msg.get("timestamp", "")
        ksa_dt, et_dt = parse_ts(ts_str)

        is_high = bool(HIGH_RELEVANCE.search(full_text))
        expert = is_expert(author)

        # Filter out very generic matches (e.g., just "bottom" or "turn" in casual context)
        # Keep ALL matches - user wants exhaustive

        results.append({
            "channel": channel,
            "date": et_dt.strftime("%Y-%m-%d") if et_dt else "unknown",
            "et_time": et_dt.strftime("%H:%M:%S ET") if et_dt else "unknown",
            "ksa_time": ksa_dt.strftime("%H:%M:%S KSA") if ksa_dt else "unknown",
            "author": author,
            "expert": expert,
            "high_relevance": is_high,
            "content": full_text[:400],
            "matched": match.group(0),
            "file": os.path.basename(filepath),
        })

    return results

def main():
    # Find all matching JSON files
    json_files = []
    for f in OUTPUT_DIR.iterdir():
        if not f.suffix == ".json":
            continue
        fname = f.name.lower()
        if "volland-daytrading-central" in fname or "0dte-alerts" in fname or "daytrading" in fname:
            json_files.append(f)

    # Also include beginners-chatter - experts sometimes post there
    for f in OUTPUT_DIR.iterdir():
        if f.suffix == ".json" and "beginners" in f.name.lower():
            json_files.append(f)

    json_files.sort(key=lambda x: x.name)

    print(f"Found {len(json_files)} JSON files to search\n")

    all_results = []
    for jf in json_files:
        print(f"Searching: {jf.name}")
        results = search_file(jf)
        print(f"  -> {len(results)} matches")
        all_results.extend(results)

    # Deduplicate by message content + author + time (overlapping exports)
    seen = set()
    unique_results = []
    for r in all_results:
        key = (r["author"], r["et_time"], r["content"][:100])
        if key not in seen:
            seen.add(key)
            unique_results.append(r)

    # Sort by date/time
    unique_results.sort(key=lambda x: (x["date"], x["et_time"]))

    print(f"\n{'='*120}")
    print(f"TOTAL UNIQUE MATCHES: {len(unique_results)}")
    print(f"{'='*120}\n")

    # === SECTION 1: EXPERT MESSAGES (highest priority) ===
    expert_msgs = [r for r in unique_results if r["expert"]]
    print(f"\n{'#'*120}")
    print(f"## SECTION 1: EXPERT MESSAGES ({len(expert_msgs)} messages)")
    print(f"## (Apollo, Wizard, LordHelmet, Phoenix, Zack, TheEdge, VollandDev)")
    print(f"{'#'*120}\n")

    for r in expert_msgs:
        tag = "[HIGH]" if r["high_relevance"] else "[    ]"
        print(f"{tag} {r['date']} {r['et_time']} | {r['channel']}")
        print(f"       Author: ** {r['author']} ** (EXPERT)")
        print(f"       Matched: '{r['matched']}'")
        print(f"       Content: {r['content']}")
        print(f"       ---")

    # === SECTION 2: HIGH RELEVANCE (overvix, vol event, etc.) ===
    high_rel = [r for r in unique_results if r["high_relevance"] and not r["expert"]]
    print(f"\n{'#'*120}")
    print(f"## SECTION 2: HIGH RELEVANCE NON-EXPERT ({len(high_rel)} messages)")
    print(f"## (overvix, vol event, vol crush, beach ball, snapback, mean revert, etc.)")
    print(f"{'#'*120}\n")

    for r in high_rel:
        print(f"[HIGH] {r['date']} {r['et_time']} | {r['channel']}")
        print(f"       Author: {r['author']}")
        print(f"       Matched: '{r['matched']}'")
        print(f"       Content: {r['content']}")
        print(f"       ---")

    # === SECTION 3: OTHER MATCHES (bounce, rally, bottom, reversal, etc.) ===
    other = [r for r in unique_results if not r["high_relevance"] and not r["expert"]]
    print(f"\n{'#'*120}")
    print(f"## SECTION 3: OTHER MATCHES ({len(other)} messages)")
    print(f"## (bounce, rally, bottom, reversal, recovery, squeeze, panic, etc.)")
    print(f"{'#'*120}\n")

    for r in other:
        print(f"[    ] {r['date']} {r['et_time']} | {r['channel']}")
        print(f"       Author: {r['author']}")
        print(f"       Matched: '{r['matched']}'")
        print(f"       Content: {r['content']}")
        print(f"       ---")

    # === SUMMARY STATS ===
    print(f"\n{'='*120}")
    print("SUMMARY")
    print(f"{'='*120}")
    print(f"Total unique matches: {len(unique_results)}")
    print(f"Expert messages: {len(expert_msgs)}")
    print(f"High relevance: {len(high_rel)}")
    print(f"Other matches: {len(other)}")
    print()

    # Keyword frequency
    from collections import Counter
    keyword_counts = Counter()
    for r in unique_results:
        keyword_counts[r["matched"].lower()] += 1
    print("Keyword frequency:")
    for kw, cnt in keyword_counts.most_common(30):
        print(f"  {kw}: {cnt}")

    # Expert author frequency
    expert_author_counts = Counter()
    for r in expert_msgs:
        expert_author_counts[r["author"]] += 1
    if expert_author_counts:
        print("\nExpert author frequency:")
        for a, cnt in expert_author_counts.most_common():
            print(f"  {a}: {cnt}")

    # Date frequency (to see which days had most vol discussion)
    date_counts = Counter()
    for r in unique_results:
        if r["high_relevance"]:
            date_counts[r["date"]] += 1
    if date_counts:
        print("\nDates with most HIGH RELEVANCE messages:")
        for d, cnt in date_counts.most_common(15):
            print(f"  {d}: {cnt}")

if __name__ == "__main__":
    main()
