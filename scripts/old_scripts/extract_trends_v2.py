import os, time, re, requests, feedparser, pandas as pd
from datetime import datetime, timezone
from deep_translator import GoogleTranslator

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
OUT_DIR = "data"
OUT_FILE = os.path.join(OUT_DIR, "trending_now_snapshot.csv")

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
    "rank", "pulled_at_utc", "country_en",
    "title_original", "title_english",
    "summary_hebrew", "link", "published"
]

translator = GoogleTranslator(source="auto", target="en")
now = datetime.now(timezone.utc).isoformat()

def is_english(text):
    return bool(re.fullmatch(r"[A-Za-z0-9\s.,!?'\-]+", text.strip()))

def preserve_numbers(text):
    return re.sub(r"(\d+)", r"¤\1¤", text)
  
def restore_numbers(translated_text):
    return re.sub(r"¤(\d+)¤", r"\1", translated_text)

def safe_translate(text, retries=3):
    if not text or not text.strip():
        return ""
    if is_english(text):
        return text

    text = preserve_numbers(text)
    for _ in range(retries):
        try:
            translated = translator.translate(text)
            return restore_numbers(translated)
        except Exception:
            time.sleep(1)
    return restore_numbers(text)


def summarize_with_gemini(trend, country):
    if not GEMINI_API_KEY:
        return "⚠️ Missing GEMINI_API_KEY"
    prompt = f"""
אתה כעת עובר על הטרנד "{trend}" שעלה בשעות האחרונות במדינת "{country}".
אני מבקש ממך ב-3 שורות קצרות בעברית לסכם בצורה טבעית וברורה:
1. מה נושא הטרנד (למשל משחק כדורגל, שחקן, נושא פוליטי וכו').
2. מה קרה במדינה בנושא הזה.
"""
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    headers = {"x-goog-api-key": GEMINI_API_KEY}
    try:
        r = requests.post(GEMINI_URL, headers=headers, json=payload, timeout=30)
        data = r.json()
        return data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "❌ No response")
    except Exception as e:
        return f"⚠️ Error: {e}"

os.makedirs(OUT_DIR, exist_ok=True)
rows = []

for geo, lang, country_en in FEEDS:
    print(f"🌍 Fetching {country_en} ({geo})...")
    url = f"https://trends.google.com/trending/rss?geo={geo}&hl={lang}"
    d = feedparser.parse(url)

    for e in d.entries:
        title_raw = e.title.strip()
        title_translated = safe_translate(title_raw)
        summary = summarize_with_gemini(title_translated, country_en)
        rows.append({
            "pulled_at_utc": now,
            "country_en": country_en,
            "title_original": title_raw,
            "title_english": title_translated,
            "summary_hebrew": summary,
            "link": getattr(e, "link", ""),
            "published": getattr(e, "published", ""),
        })
        time.sleep(1)

if os.path.exists(OUT_FILE):
    old_df = pd.read_csv(OUT_FILE, encoding="utf-8-sig")
    old_df["rank"] = old_df["rank"] + 1
    new_df = pd.DataFrame(rows)
    new_df.insert(0, "rank", 1)
    combined = pd.concat([new_df, old_df], ignore_index=True)
else:
    new_df = pd.DataFrame(rows)
    new_df.insert(0, "rank", 1)
    combined = new_df

combined.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

print(f"\nSaved {len(rows)} new trends with summaries.")
print(f"Output file: {os.path.abspath(OUT_FILE)}")
print(f"Total rows: {len(combined)}")
