#!/usr/bin/env python3
"""
Amazon SERP Scraper — /serpscrape Slack Command
=================================================
Slash command: /serpscrape <marketplace>
Example:        /serpscrape com

Flow:
  1. /serpscrape com  →  lookup Slack channel name in Airtable (Clients table)
  2. Find Google Sheet URL from 'Master Sheets' field
  3. Pull product names (col A) and keywords (col E) from that sheet
  4. Show a Slack dropdown modal with product names
  5. User picks a product → grab keyword from col E
  6. Scrape Amazon with that keyword + marketplace
  7. Post results back to Slack channel
"""

import hashlib
import hmac
import json
import os
import random
import re
import threading
import time

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ── Environment variables (set in Railway dashboard) ──────────────────────────
PROXY_USER              = os.environ.get("PROXY_USER", "")
PROXY_PASS              = os.environ.get("PROXY_PASS", "")
PROXY_HOST              = os.environ.get("PROXY_HOST", "core-residential.evomi.com")
PROXY_PORT              = os.environ.get("PROXY_PORT", "1000")
SLACK_SIGNING_SECRET    = os.environ.get("SLACK_SIGNING_SECRET", "")
SLACK_BOT_TOKEN         = os.environ.get("SLACK_BOT_TOKEN", "")
AIRTABLE_API_KEY        = os.environ.get("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID        = os.environ.get("AIRTABLE_BASE_ID", "appT17nLtUXKnxbMQ")
AIRTABLE_TABLE_ID       = os.environ.get("AIRTABLE_TABLE_ID", "tblnuwAaodLKAk3nC")
GOOGLE_CREDS_JSON       = os.environ.get("GOOGLE_CREDS_JSON", "")

# ── Scraper config ─────────────────────────────────────────────────────────────
TARGET_ASINS = 20
MAX_PAGES    = 5

MARKETPLACES = {
    "com":    "https://www.amazon.com/s",
    "co.uk":  "https://www.amazon.co.uk/s",
    "de":     "https://www.amazon.de/s",
    "fr":     "https://www.amazon.fr/s",
    "es":     "https://www.amazon.es/s",
    "it":     "https://www.amazon.it/s",
    "ca":     "https://www.amazon.ca/s",
    "com.au": "https://www.amazon.com.au/s",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]


# ── Slack verification ─────────────────────────────────────────────────────────

def verify_slack_request(req) -> bool:
    if not SLACK_SIGNING_SECRET:
        return True
    timestamp = req.headers.get("X-Slack-Request-Timestamp", "")
    signature = req.headers.get("X-Slack-Signature", "")
    if abs(time.time() - int(timestamp)) > 300:
        return False
    sig_basestring = f"v0:{timestamp}:{req.get_data(as_text=True)}"
    computed = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(computed, signature)


# ── Proxy ──────────────────────────────────────────────────────────────────────

def get_proxies():
    if PROXY_USER and PROXY_PASS:
        url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        return {"http": url, "https": url}
    return None


# ── Airtable ───────────────────────────────────────────────────────────────────

def get_client_record(channel_name: str) -> dict | None:
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    params  = {
        "filterByFormula": f"{{Slack Internal Channel}}='{channel_name}'",
        "maxRecords": 1,
    }
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    records = resp.json().get("records", [])
    return records[0] if records else None


# ── Google Sheets ──────────────────────────────────────────────────────────────

def get_sheets_service():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=creds)


def extract_sheet_id(url_or_id: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_id)
    return match.group(1) if match else url_or_id


def get_products_from_sheet(sheet_url: str) -> list[dict]:
    sheet_id = extract_sheet_id(sheet_url)
    service  = get_sheets_service()
    result   = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="A:E",
    ).execute()
    rows     = result.get("values", [])
    products = []
    for i, row in enumerate(rows):
        if i == 0:
            continue  # skip header row
        name    = row[0].strip() if len(row) > 0 and row[0].strip() else None
        keyword = row[4].strip() if len(row) > 4 and row[4].strip() else None
        if name and keyword:
            products.append({"name": name, "keyword": keyword, "row": i + 1})
    return products


# ── Scraper ────────────────────────────────────────────────────────────────────

def build_headers():
    return {
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-GB,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Cache-Control":             "max-age=0",
    }


def is_sponsored(card) -> bool:
    if "sp-sponsored-result" in card.get("data-component-type", ""):
        return True
    html = str(card)
    if re.search(r'data-component-type=["\']sp-', html):
        return True
    if re.search(r'sponsored-products|sp_sponsored', html, re.IGNORECASE):
        return True
    for tag in card.find_all(["span", "div", "a"]):
        if tag.get_text(strip=True).lower() in ("sponsored", "sponsorisé", "patrocinado", "gesponsert"):
            return True
    return False


def extract_asin(card):
    asin = card.get("data-asin", "").strip()
    if asin and len(asin) == 10:
        return asin
    link = card.find("a", href=re.compile(r"/dp/[A-Z0-9]{10}"))
    if link:
        m = re.search(r"/dp/([A-Z0-9]{10})", link["href"])
        if m:
            return m.group(1)
    return None


def extract_product(card):
    asin = extract_asin(card)
    if not asin:
        return None
    title_tag   = card.find("span", {"class": re.compile(r"a-text-normal")})
    title       = title_tag.get_text(strip=True) if title_tag else ""
    price_whole = card.find("span", {"class": "a-price-whole"})
    price_frac  = card.find("span", {"class": "a-price-fraction"})
    if price_whole:
        whole = price_whole.get_text(strip=True).rstrip(".")
        frac  = price_frac.get_text(strip=True) if price_frac else "00"
        price = f"{whole}.{frac}"
    else:
        price = ""
    rating_tag  = card.find("span", {"class": "a-icon-alt"})
    rating      = rating_tag.get_text(strip=True).split(" ")[0] if rating_tag else ""
    review_tag  = card.find("a", href=re.compile(r"#customerReviews"))
    if not review_tag:
        review_tag = card.find("span", {"class": re.compile(r"a-size-base s-underline")})
    reviews = review_tag.get_text(strip=True) if review_tag else ""
    return {"asin": asin, "title": title, "price": price, "rating": rating, "reviews": reviews}


def fetch_page(base_url, keyword, page):
    params = {"k": keyword, "page": page, "ref": f"sr_pg_{page}"}
    try:
        r = requests.get(
            base_url,
            params=params,
            headers=build_headers(),
            proxies=get_proxies(),
            timeout=30,
        )
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
        if r.status_code == 503:
            time.sleep(10)
    except requests.exceptions.RequestException:
        pass
    return None


def scrape(keyword, marketplace):
    base_url = MARKETPLACES[marketplace]
    results, seen = [], set()
    for page in range(1, MAX_PAGES + 1):
        if len(results) >= TARGET_ASINS:
            break
        soup = fetch_page(base_url, keyword, page)
        if soup is None:
            continue
        cards = soup.find_all(
            "div",
            attrs={"data-component-type": re.compile(r"s-search-result|sp-sponsored-result")},
        )
        if not cards:
            cards = soup.find_all("div", attrs={"data-asin": re.compile(r"^[A-Z0-9]{10}$")})
        for card in cards:
            if len(results) >= TARGET_ASINS:
                break
            if is_sponsored(card):
                continue
            product = extract_product(card)
            if not product or product["asin"] in seen:
                continue
            seen.add(product["asin"])
            product["position"]    = len(results) + 1
            product["page"]        = page
            product["marketplace"] = marketplace
            results.append(product)
        if page < MAX_PAGES and len(results) < TARGET_ASINS:
            time.sleep(random.uniform(3, 6))
    return results


# ── Slack helpers ──────────────────────────────────────────────────────────────

def open_product_modal(trigger_id, products, marketplace, channel_id):
    options = [
        {
            "text":  {"type": "plain_text", "text": p["name"][:75]},
            "value": json.dumps({"keyword": p["keyword"], "marketplace": marketplace, "channel_id": channel_id}),
        }
        for p in products[:100]
    ]
    modal = {
        "type":        "modal",
        "callback_id": "product_selected",
        "title":       {"type": "plain_text", "text": "Pick a Product"},
        "submit":      {"type": "plain_text", "text": "Search Amazon"},
        "close":       {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"Searching *amazon.{marketplace}* — select a product:"},
            },
            {
                "type":     "input",
                "block_id": "product_block",
                "label":    {"type": "plain_text", "text": "Product"},
                "element":  {
                    "type":        "static_select",
                    "action_id":   "product_select",
                    "placeholder": {"type": "plain_text", "text": "Choose a product..."},
                    "options":     options,
                },
            },
        ],
    }
    requests.post(
        "https://slack.com/api/views.open",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"trigger_id": trigger_id, "view": modal},
        timeout=10,
    )


def post_to_channel(channel_id, keyword, marketplace, asins):
    domain  = f"amazon.{marketplace}"
    currency = "$" if marketplace == "com" else "£"
    blocks  = [
        {"type": "header", "text": {"type": "plain_text", "text": f"🔍 Top {len(asins)} Organic ASINs"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Keyword: *{keyword}*  ·  Marketplace: *{domain}*"}]},
        {"type": "divider"},
    ]
    for item in asins:
        url     = f"https://www.{domain}/dp/{item['asin']}"
        price   = f"{currency}{item['price']}" if item["price"] else "N/A"
        stars   = f"★ {item['rating']}" if item["rating"] else ""
        reviews = f"({item['reviews']} reviews)" if item["reviews"] else ""
        title   = item["title"][:80] + "..." if len(item["title"]) > 80 else item["title"]
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{item['position']}.* <{url}|{item['asin']}>\n"
                    f"{title}\n"
                    f"`{price}`  {stars}  {reviews}"
                ),
            },
        })
    blocks.append({"type": "divider"})
    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"channel": channel_id, "blocks": blocks},
        timeout=10,
    )


def post_error_to_channel(channel_id, message):
    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"channel": channel_id, "text": f"❌ {message}"},
        timeout=10,
    )


# ── Background worker ──────────────────────────────────────────────────────────

def run_scrape_and_post(keyword, marketplace, channel_id):
    try:
        asins = scrape(keyword, marketplace)
        if asins:
            post_to_channel(channel_id, keyword, marketplace, asins)
        else:
            post_error_to_channel(channel_id, f"No organic results found for *{keyword}* on amazon.{marketplace}. Try again in a moment.")
    except Exception as e:
        post_error_to_channel(channel_id, f"Scraper error: {str(e)}")


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "serpscrape"})


@app.route("/serpscrape", methods=["POST"])
def slack_serpscrape():
    if not verify_slack_request(request):
        return jsonify({"error": "Unauthorized"}), 401

    text         = request.form.get("text", "").strip().lower()
    trigger_id   = request.form.get("trigger_id", "")
    channel_id   = request.form.get("channel_id", "")
    channel_name = request.form.get("channel_name", "")
    marketplace  = text if text in MARKETPLACES else "com"

    def load_and_open_modal():
        try:
            record = get_client_record(channel_name)
            if not record:
                post_error_to_channel(
                    channel_id,
                    f"No Airtable record found for channel *#{channel_name}*. Make sure the `Slack Internal Channel` field matches exactly."
                )
                return

            sheet_url = record.get("fields", {}).get("Master Sheets", "")
            if not sheet_url:
                post_error_to_channel(channel_id, "No Google Sheet URL found in the `Master Sheets` field for this client.")
                return

            products = get_products_from_sheet(sheet_url)
            if not products:
                post_error_to_channel(channel_id, "No products found in the master sheet. Make sure column A (name) and column E (keyword) are filled in.")
                return

            open_product_modal(trigger_id, products, marketplace, channel_id)

        except Exception as e:
            post_error_to_channel(channel_id, f"Error loading products: {str(e)}")

    threading.Thread(target=load_and_open_modal, daemon=True).start()

    return jsonify({"response_type": "ephemeral", "text": "⏳ Loading products for this channel..."})


@app.route("/interact", methods=["POST"])
def slack_interact():
    payload = json.loads(request.form.get("payload", "{}"))

    if payload.get("type") == "view_submission":
        values   = payload["view"]["state"]["values"]
        selected = values["product_block"]["product_select"]["selected_option"]
        data     = json.loads(selected["value"])

        keyword     = data["keyword"]
        marketplace = data["marketplace"]
        channel_id  = data["channel_id"]

        requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
            json={"channel": channel_id, "text": f"⏳ Searching *amazon.{marketplace}* for *{keyword}*... Results coming shortly."},
            timeout=10,
        )

        threading.Thread(
            target=run_scrape_and_post,
            args=(keyword, marketplace, channel_id),
            daemon=True,
        ).start()

        return "", 200

    return "", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
