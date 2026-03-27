import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
from deep_translator import GoogleTranslator

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

translator = GoogleTranslator(source="auto", target="en")

def is_english(text):
    return all(ord(c) < 128 for c in text)

ns = {"ht": "https://trends.google.com/trending/rss"}
snapshot = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
rows = []

for geo, lang, country in FEEDS:
    url = f"https://trends.google.com/trending/rss?geo={geo}"
    response = requests.get(url, timeout=30)
    root = ET.fromstring(response.text)

    for item in root.findall(".//item"):
        title = item.findtext("title")
        traffic = item.findtext("ht:approx_traffic", namespaces=ns)

        if traffic:
            traffic = traffic.replace("+", "").strip()
            if "K" in traffic:
                traffic = int(float(traffic.replace("K", "")) * 1000)
            elif "M" in traffic:
                traffic = int(float(traffic.replace("M", "")) * 1000000)
            else:
                traffic = int("".join(filter(str.isdigit, traffic)))
        else:
            traffic = None

        pub_date = item.findtext("pubDate")
        if pub_date:
            dt = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
            date = dt.strftime("%Y-%m-%d")
            end_time = dt.strftime("%H:%M:%S")
            start_time = dt.strftime("%z")
            start_time = start_time[:3].replace("-", "") + ":" + start_time[3:]
            start_time = f"{start_time}:00"
        else:
            date, start_time, end_time = None, None, None

        url_pic = item.findtext("ht:picture", namespaces=ns)
        news_items = item.findall("ht:news_item", ns)

        news_titles = [None, None, None]
        news_urls = [None, None, None]
        news_pictures = [None, None, None]
        news_sources = [None, None, None]

        for i, news in enumerate(news_items[:3]):
            news_titles[i] = news.findtext("ht:news_item_title", namespaces=ns)
            news_urls[i] = news.findtext("ht:news_item_url", namespaces=ns)
            news_pictures[i] = news.findtext("ht:news_item_picture", namespaces=ns)
            news_sources[i] = news.findtext("ht:news_item_source", namespaces=ns)

        rows.append({
            "geo": geo,
            "language": lang,
            "country": country,
            "trend_title": title,
            "traffic": traffic,
            "date": date,
            "start_time": start_time,
            "end_time": end_time,
            "picture_url": url_pic,
            "news_item_title_1": news_titles[0],
            "news_item_url_1": news_urls[0],
            "news_item_picture_1": news_pictures[0],
            "news_item_source_1": news_sources[0],
            "news_item_title_2": news_titles[1],
            "news_item_url_2": news_urls[1],
            "news_item_picture_2": news_pictures[1],
            "news_item_source_2": news_sources[1],
            "news_item_title_3": news_titles[2],
            "news_item_url_3": news_urls[2],
            "news_item_picture_3": news_pictures[2],
            "news_item_source_3": news_sources[2],
            "snapshot": snapshot
        })

df = pd.DataFrame(rows)

for col in df.columns:
    if any(skip in col.lower() for skip in ["url", "traffic", "date", "time", "snapshot"]):
        continue
    df[col] = df[col].apply(
        lambda x: translator.translate(x) if isinstance(x, str) and not is_english(x) else x
    )

os.makedirs("data", exist_ok=True)
file_path = os.path.join("data", "trending_now_snapshot.csv")

if os.path.exists(file_path):
    df_existing = pd.read_csv(file_path)
    df = pd.concat([df_existing, df], ignore_index=True)

cols_to_check = [col for col in df.columns if col != "snapshot"]
df = df.drop_duplicates(subset=cols_to_check)

df.to_csv(file_path, index=False, encoding="utf-8-sig")
