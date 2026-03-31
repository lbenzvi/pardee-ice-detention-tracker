import requests
import pandas as pd
import time
from datetime import datetime, timezone
import psycopg2
import os

# ---------- DB ----------
def get_db_connection():
    return psycopg2.connect(os.getenv("DB_URL"))

def get_latest_date():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT MAX(date) FROM contracts;")
    result = cur.fetchone()[0]

    cur.close()
    conn.close()

    return result


# ---------- CONFIG ----------
BASE_URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json"
}


# ---------- KEYWORDS ----------
TIER1 = [
    "detention", "detainee", "detainees",
    "influx care facility", "influx care",
    "processing center", "processing facility",
    "immigration custody", "holding facility",
    "immigration enforcement",
    "alien housing", "alien detention",
    "family residential", "family detention"
]

TIER2 = [
    "facility operations", "facility management",
    "residential services", "shelter services",
    "housing services", "dormitory",
    "transportation services", "escort services",
    "removal", "repatriation", "alien transport",
    "case management", "medical services",
    "mental health services", "guard services",
    "bed", "beds"
]

NEGATIVE = [
    "janitorial", "landscaping", "lawn",
    "food service", "cafeteria", "catering",
    "it support", "information technology",
    "printing", "copier", "furniture",
    "license", "software", "subscription",
    "warranty"
]


# ---------- FETCH ----------
def fetch_page(page):
    payload = {
        "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "awarding_sub_agency_names": [
                "U.S. Immigration and Customs Enforcement"
            ],
            "keywords": [
                "detention",
                "detainee",
                "influx",
                "processing center",
                "alien",
                "custody"
            ]
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Start Date",
            "Place of Performance State Code",
            "Description",
            "Awarding Agency Name"
        ],
        "page": page,
        "limit": 50,
        "sort": "Start Date",
        "order": "desc"
    }

    for attempt in range(5):
        try:
            res = requests.post(
                BASE_URL,
                json=payload,
                headers=HEADERS,
                timeout=30
            )

            if res.status_code == 200:
                return res.json().get("results", [])

            print(f"Bad status code: {res.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)

    print("Failed to fetch page:", page)
    return []


# ---------- FILTER ----------
def is_relevant(row):
    desc = (row.get("description") or "").lower()

    if any(k in desc for k in NEGATIVE):
        return False

    if any(x in desc for x in [
        "navmc", "ordinance", "weapon",
        "ammunition", "army", "air force"
    ]):
        return False

    if any(x in desc for x in [
        "license", "software", "subscription",
        "printer", "copier", "equipment",
        "warranty", "hardware"
    ]):
        return False

    if any(k in desc for k in TIER1):
        return True

    tier2_match = any(k in desc for k in TIER2)

    context_words = [
        "immigration", "alien", "border",
        "detention", "transport", "housing", "facility"
    ]

    context_match = any(k in desc for k in context_words)

    return tier2_match and context_match


# ---------- SCORE ----------
def relevance_score(row):
    desc = (row.get("description") or "").lower()

    if any(x in desc for x in [
        "review", "assessment", "consulting",
        "analysis", "inspection", "audit"
    ]):
        return 0

    score = 0

    score += sum(k in desc for k in TIER1) * 3
    score += sum(k in desc for k in TIER2) * 2

    if "detention" in desc:
        score += 3
    if "detainee" in desc:
        score += 3
    if "influx care" in desc:
        score += 4
    if "detention" in desc and "transport" in desc:
        score += 4
    if "detainee" in desc and "service" in desc:
        score += 3

    if any(x in desc for x in [
        "review", "assessment", "consulting",
        "analysis", "inspection", "audit"
    ]):
        score -= 6

    return int(score)


# ---------- DB INSERT ----------
def insert_contracts(df):
    conn = get_db_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO contracts (
                award_id,
                recipient,
                amount,
                date,
                state,
                description,
                awarding_agency,
                relevance_score,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (award_id) DO NOTHING;
        """, (
            row["award_id"],
            row["recipient"],
            row["amount"],
            row["date"],
            row["state"],
            row["description"],
            row["awarding_agency"],
            row["relevance_score"]
        ))

    conn.commit()
    cur.close()
    conn.close()


# ---------- MAIN ----------
def poll():
    print(f"\nPolling at {datetime.now(timezone.utc)}")

    latest_date = get_latest_date()
    print("Latest date in DB:", latest_date)

    all_data = []

    for page in range(1, 25):
        results = fetch_page(page)
        if not results:
            break

        new_batch = []

        for r in results:
            contract_date = r.get("Start Date")

            if not contract_date:
                continue

            # 🔥 THIS IS WHERE YOUR CODE GOES
            contract_dt = datetime.fromisoformat(contract_date).date()

            latest_dt = latest_date  # already a date from DB

            if latest_dt and contract_dt <= latest_dt:
                print("Hit existing data → stopping early")
                break

            new_batch.append(r)

        # 🚨 IMPORTANT: stop outer loop too
        if not new_batch:
            break

        all_data.extend(new_batch)
        time.sleep(1)

    if not all_data:
        print("No new data")
        return

    df = pd.DataFrame([{
        "award_id": r.get("Award ID"),
        "recipient": r.get("Recipient Name"),
        "amount": r.get("Award Amount"),
        "date": r.get("Start Date"),
        "state": r.get("Place of Performance State Code"),
        "description": r.get("Description"),
        "awarding_agency": r.get("Awarding Agency Name")
    } for r in all_data])

    print("Fetched NEW:", len(df))

    if df.empty:
        return

    # filtering
    df = df[df.apply(is_relevant, axis=1)]

    if df.empty:
        print(f"New contracts fetched: {len(all_data)}")
        print(f"Relevant after filtering: {len(df)}")
        print("No inserts this run")
        return

    df.loc[:, "relevance_score"] = df.apply(
        lambda row: relevance_score(row), axis=1
    )

    df = df[df["relevance_score"] >= 10]

    print("Relevant NEW:", len(df))

    if df.empty:
        return

    df = df.sort_values("relevance_score", ascending=False)

    print("\nTop matches:")
    print(df[["recipient", "relevance_score", "description"]].head(5))

    insert_contracts(df)
    print("Inserted new contracts")


if __name__ == "__main__":
    try:
        poll()
    except Exception as e:
        print("Error:", e)
