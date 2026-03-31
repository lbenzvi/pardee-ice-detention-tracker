from contracts_pipeline import fetch_page, is_relevant, relevance_score, insert_contracts
import pandas as pd
import time

def backfill():
    print("Starting backfill...")

    all_data = []

    for page in range(1, 200):   # go deep
        print(f"Fetching page {page}")

        results = fetch_page(page)
        if not results:
            break

        all_data.extend(results)
        time.sleep(1)

    df = pd.DataFrame([{
        "award_id": r.get("Award ID"),
        "recipient": r.get("Recipient Name"),
        "amount": r.get("Award Amount"),
        "date": r.get("Start Date"),
        "state": r.get("Place of Performance State Code"),
        "description": r.get("Description"),
        "awarding_agency": r.get("Awarding Agency Name")
    } for r in all_data])

    print("Total fetched:", len(df))

    df = df[df.apply(is_relevant, axis=1)]
    df["relevance_score"] = df.apply(relevance_score, axis=1)
    df = df[df["relevance_score"] >= 10]

    print("Relevant:", len(df))

    insert_contracts(df)
    print("Backfill complete")

if __name__ == "__main__":
    backfill()
