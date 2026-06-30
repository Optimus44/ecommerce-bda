import json
from pathlib import Path

from pymongo import MongoClient
from pymongo import UpdateOne

MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "ecommerce"

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_purchase_summary(db):
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "total_spend": {"$sum": "$total"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total"},
                "last_order_date": {"$max": "$timestamp"},
                "first_order_date": {"$min": "$timestamp"},
            }
        }
    ]

    summary_map = {}
    for doc in db.transactions.aggregate(pipeline):
        summary_map[doc["_id"]] = {
            "total_spend": round(doc["total_spend"], 2),
            "order_count": int(doc["order_count"]),
            "avg_order_value": round(doc["avg_order_value"], 2),
            "last_order_date": doc["last_order_date"],
            "first_order_date": doc["first_order_date"],
        }

    return summary_map

def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[DB_NAME]

    # Reset target collections to keep the load idempotent.
    db.products.delete_many({})
    db.users.delete_many({})
    db.transactions.delete_many({})
    db.categories.delete_many({})

    products = load_json("products.json")
    users = load_json("users.json")
    transactions = load_json("transactions.json")
    categories = load_json("categories.json")

    db.products.insert_many(products)
    db.categories.insert_many(categories)
    db.transactions.insert_many(transactions)
    db.users.insert_many(users)

    summary_map = build_purchase_summary(db)

    updates = []
    for user in users:
        updates.append(
            UpdateOne(
                {"user_id": user["user_id"]},
                {"$set": {"purchase_summary": summary_map.get(user["user_id"], {
                    "total_spend": 0.0,
                    "order_count": 0,
                    "avg_order_value": 0.0,
                    "last_order_date": None,
                    "first_order_date": None
                })}}
            )
        )

    if updates:
        db.users.bulk_write(updates)

    print("Mongo load complete")
    print(f"products={db.products.count_documents({})}")
    print(f"users={db.users.count_documents({})}")
    print(f"transactions={db.transactions.count_documents({})}")
    print(f"categories={db.categories.count_documents({})}")
    print(f"users_with_purchase_summary={db.users.count_documents({'purchase_summary': {'$exists': True}})}")

if __name__ == "__main__":
    main()