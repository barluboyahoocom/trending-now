import os, time, re, requests, feedparser, pandas as pd
from datetime import datetime, timezone
from deep_translator import GoogleTranslator

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

GEMINI_20_FLASH_LIMITS = {
    "MAX_RPM": 14,        # Requests per minute
    "MAX_TPM": 1_000_000, # Tokens per minute
    "MAX_RPD": 200,       # Requests per day
    "TOKEN_ESTIMATE": 350 # Tokens used per request (approx)
}

GEMINI_25_FLASH_LIMITS = {
    "MAX_RPM": 9,         # Requests per minute
    "MAX_TPM": 250_000,    # Tokens per minute
    "MAX_RPD": 250,        # Requests per day
    "TOKEN_ESTIMATE": 350  # Tokens used per request (approx)
}


if "2.5" in GEMINI_URL:
    LIMITS = GEMINI_25_FLASH_LIMITS
    print("🤖 Using Gemini model: gemini-2.5-flash (⚡ higher accuracy)")
else:
    LIMITS = GEMINI_20_FLASH_LIMITS
    print("🤖 Using Gemini model: gemini-2.0-flash (⚙️ faster, lower limits)")

MAX_RPM = LIMITS["MAX_RPM"] # Requests per minute
MAX_TPM = LIMITS["MAX_TPM"] # Tokens per minute
MAX_RPD = LIMITS["MAX_RPD"] # Requests per day
TOKEN_ESTIMATE = LIMITS["TOKEN_ESTIMATE"]  # Tokens used per request (approx)

OUT_DIR = "data"
OUT_FILE = os.path.join(OUT_DIR, "trending_now_snapshot.csv")

if os.path.exists(OUT_FILE):
    print("🧹 Cleaning existing duplicate rows before run...")
    df = pd.read_csv(OUT_FILE, encoding="utf-8-sig")

    if not df.empty and all(col in df.columns for col in ["country_en", "title_original", "published"]):
        # חילוץ תאריך בלבד מתוך ה־published
        df["published_date"] = pd.to_datetime(df["published"], errors="coerce").dt.date.astype(str)
        df["dup_key"] = (
            df["country_en"].astype(str).str.strip() + "_" +
            df["title_original"].astype(str).str.strip() + "_" +
            df["published_date"].astype(str).str.strip()
        )

        before = len(df)
        df = df.sort_values(by=["rank"]).drop_duplicates(subset=["dup_key"], keep="first")
        df = df.drop(columns=["dup_key", "published_date"])
        after = len(df)

        if before != after:
            print(f"✅ Removed {before - after} old duplicate rows (same trend & date).")
            df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
        else:
            print("✅ No duplicates found in existing file.")
    else:
        print("⚠️ Skipped cleanup — missing expected columns in existing file.")

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
    "summary_hebrew", "link", "published", "search_volume"
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

last_requests = []
total_tokens_this_min = 0

def respect_rate_limits(tokens_used):
    global last_requests, total_tokens_this_min

    now = time.time()
    last_requests = [(t, tok) for t, tok in last_requests if now - t < 60]
    total_tokens_this_min = sum(tok for _, tok in last_requests)

    if len(last_requests) >= MAX_RPM:
        sleep_time = 60 - (now - last_requests[0][0])
        print(f"⏸️ Reached RPM limit ({MAX_RPM}). Sleeping {sleep_time:.1f}s...")
        time.sleep(max(1, sleep_time))

    if total_tokens_this_min + tokens_used > MAX_TPM:
        print("⏸️ Reached TPM limit. Waiting 60s...")
        time.sleep(60)
        total_tokens_this_min = 0
        last_requests = []

    last_requests.append((now, tokens_used))

def summarize_with_gemini(trend, country, retries=1):
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

    for attempt in range(1, retries + 1):
        respect_rate_limits(TOKEN_ESTIMATE)
        try:
            r = requests.post(GEMINI_URL, headers=headers, json=payload, timeout=30)
            data = r.json()
            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if text and "No response" not in text:
                return text.strip()
        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for '{trend}': {e}")
        wait_time = min(30, attempt * 10)
        time.sleep(wait_time)

    return "❌ No response"

os.makedirs(OUT_DIR, exist_ok=True)
rows = []

for geo, lang, country_en in FEEDS:
    print(f"🌍 Fetching {country_en} ({geo})...")
    url = f"https://trends.google.com/trending/rss?geo={geo}&hl={lang}"
    d = feedparser.parse(url)

    for e in d.entries:
        title_raw = e.title.strip()
        title_translated = safe_translate(title_raw)
        traffic_raw = getattr(e, "ht_approx_traffic", "")
        if traffic_raw:
            traffic_clean = re.sub(r"[^\d]", "", traffic_raw)
        else:
            traffic_clean = ""
        summary = summarize_with_gemini(title_translated, country_en)
        rows.append({
            "pulled_at_utc": now,
            "country_en": country_en,
            "title_original": title_raw,
            "title_english": title_translated,
            "summary_hebrew": summary,
            "link": getattr(e, "link", ""),
            "published": getattr(e, "published", ""),
            "search_volume": traffic_clean,
        })
        time.sleep(1)

if os.path.exists(OUT_FILE):
    old_df = pd.read_csv(OUT_FILE, encoding="utf-8-sig")
    old_df["rank"] = old_df["rank"] + 1
    new_df = pd.DataFrame(rows)
    new_df.insert(0, "rank", 1)
    combined = pd.concat([new_df, old_df], ignore_index=True)
    combined["published_date"] = pd.to_datetime(combined["published"], errors="coerce").dt.date.astype(str)
    combined["dup_key"] = (
        combined["country_en"].astype(str).str.strip() + "_" +
        combined["title_original"].astype(str).str.strip() + "_" +
        combined["published_date"].astype(str).str.strip()
        )
    combined = combined.sort_values(by=["rank"]).drop_duplicates(subset=["dup_key"], keep="first")
    combined = combined.drop(columns=["dup_key", "published_date"])

else:
    new_df = pd.DataFrame(rows)
    new_df.insert(0, "rank", 1)
    combined = new_df

print("\n🔁 Checking for previous 'No response' summaries...")
retry_mask = (combined["summary_hebrew"].astype(str).str.contains("No response")) | \
             (combined["summary_hebrew"].astype(str).str.startswith("⚠️"))
no_response_df = combined[retry_mask].copy()

if not no_response_df.empty:
    print(f"Found {len(no_response_df)} items without summary. Retrying Gemini up to 3 times...")
    for idx, row in no_response_df.iterrows():
        trend = row["title_english"]
        country = row["country_en"]
        new_summary = summarize_with_gemini(trend, country, retries=3)
        combined.at[idx, "summary_hebrew"] = new_summary
        print(f"↻ Retried: {trend[:50]} → {new_summary[:40]}")
else:
    print("✅ No missing summaries found.")

combined.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

print(f"\n✅ Saved {len(rows)} new trends with summaries.")
print(f"📁 Output file: {os.path.abspath(OUT_FILE)}")
print(f"📊 Total rows: {len(combined)}")
