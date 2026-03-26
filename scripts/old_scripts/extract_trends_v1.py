import feedparser, csv, os, time
from datetime import datetime, timezone
from deep_translator import GoogleTranslator
import pandas as pd  

FEEDS = [
    ("LB", "ar", "Lebanon"),
    ("IL", "he", "Israel"),
    ("PS", "ar", "Gaza"),
    ("SY", "ar", "Syria"),
    ("JO", "ar", "Jordan"),
    ("EG", "ar", "Egypt"),
    ("IR", "fa", "Iran"),
    ("YE", "ar", "Yemen"),
    ("SA", "ar", "Saudi Arabia"),
    ("IQ", "ar", "Iraq"),
    ("AE", "ar", "United Arab Emirates"),
    ("QA", "ar", "Qatar"),
]

FIELDNAMES = [
    "rank",
    "pulled_at_utc",
    "country_en",
    "title_original",
    "title_english",
    "link",
    "published"
]

now = datetime.now(timezone.utc).isoformat()
rows = []
translator = GoogleTranslator(source='auto', target='en')

def safe_translate(text, retries=3):
    for _ in range(retries):
        try:
            if not text.strip():
                return ""
            return translator.translate(text)
        except Exception:
            time.sleep(1)
    return ""

for geo, lang, country_en in FEEDS:
    url = f"https://trends.google.com/trending/rss?geo={geo}&hl={lang}"
    d = feedparser.parse(url)
    for e in d.entries:
        title_raw = e.title.strip()
        title_translated = safe_translate(title_raw)
        rows.append({
            "pulled_at_utc": now,              
            "country_en": country_en,
            "title_original": title_raw,
            "title_english": title_translated,
            "link": getattr(e, "link", ""),
            "published": getattr(e, "published", ""),
        })

os.makedirs("data", exist_ok=True)
out_file = os.path.join("data", "trending_now_snapshot.csv")


if os.path.exists(out_file):
    old_df = pd.read_csv(out_file, encoding="utf-8-sig")
    old_df["rank"] = old_df["rank"] + 1 
    rank = 1
    new_df = pd.DataFrame(rows)
    new_df.insert(0, "rank", rank)
    combined = pd.concat([new_df, old_df], ignore_index=True)
else:
    new_df = pd.DataFrame(rows)
    new_df.insert(0, "rank", 1)
    combined = new_df

combined.to_csv(out_file, index=False, encoding="utf-8-sig")

print(f"Saved {len(rows)} rows with rank=1 to {os.path.abspath(out_file)}")
print(f"Total rows in file: {len(combined)}")
