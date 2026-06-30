"""
HBase Data Loader — E-Commerce Analytics
Loads two tables into HBase:
  1. sessions          — user browsing sessions (row key: user_id#reverse_timestamp)
  2. product_performance — product view counts per day (row key: product_id#date)

Prerequisites:
    pip install happybase
    HBase must be running with the Thrift server on port 9090.
    Both tables must already exist (see create_table.hbase).

Usage:
    python load_data.py
"""

import json
from datetime import datetime
from glob import glob

import happybase

HBASE_HOST = "localhost"
HBASE_PORT = 9090
BATCH_SIZE = 1000


def reverse_timestamp(dt_string):
    """
    Convert an ISO timestamp to a reverse millisecond timestamp.

    Subtracting from a large constant means more recent timestamps
    produce smaller values — so when HBase sorts rows by key,
    the most recent session for a user appears first.
    """
    dt = datetime.fromisoformat(dt_string)
    ts = int(dt.timestamp() * 1000)
    return 9999999999999 - ts


def load_sessions(json_file, table):
    """
    Load all sessions from a single JSON file into the sessions table.

    Row key: user_id#reverse_timestamp
    Column families: info, device, geo, activity
    """
    with open(json_file, encoding="utf-8") as f:
        sessions = json.load(f)

    batch = table.batch(batch_size=BATCH_SIZE)

    for session in sessions:
        user_id = session["user_id"]
        rev_ts  = reverse_timestamp(session["start_time"])
        row_key = f"{user_id}#{rev_ts}"

        data = {
            # info — core session metadata
            b"info:session_id":        session["session_id"].encode(),
            b"info:duration_seconds":  str(session["duration_seconds"]).encode(),
            b"info:conversion_status": session["conversion_status"].encode(),
            b"info:referrer":          session["referrer"].encode(),

            # device — client device and browser
            b"device:type":    session["device_profile"]["type"].encode(),
            b"device:os":      session["device_profile"]["os"].encode(),
            b"device:browser": session["device_profile"]["browser"].encode(),

            # geo — geographic location of the session
            b"geo:city":       session["geo_data"]["city"].encode(),
            b"geo:state":      session["geo_data"]["state"].encode(),
            b"geo:country":    session["geo_data"]["country"].encode(),
            b"geo:ip_address": session["geo_data"]["ip_address"].encode(),

            # activity — what the user did during the session
            b"activity:viewed_products": ",".join(session["viewed_products"]).encode(),
            b"activity:page_view_count": str(len(session["page_views"])).encode(),
        }

        batch.put(row_key.encode(), data)

    batch.send()
    print(f"  Loaded {len(sessions):,} sessions from {json_file}")
    return len(sessions)


def load_product_performance(json_file, table):
    """
    Derive product-level view counts per day from session data and load
    into the product_performance table.

    Row key: product_id#date (e.g. prod_00950#2025-03-12)
    Column family: metrics
      - metrics:view_count — number of times this product was viewed on this date

    This aggregates across all sessions in the file before writing,
    so each product-day combination becomes exactly one row in HBase.
    """
    with open(json_file, encoding="utf-8") as f:
        sessions = json.load(f)

    # Accumulate view counts per product per day in memory before writing
    product_day_views = {}

    for session in sessions:
        date = session["start_time"][:10]  # Extract YYYY-MM-DD from ISO timestamp
        for product_id in session["viewed_products"]:
            row_key = f"{product_id}#{date}"
            product_day_views[row_key] = product_day_views.get(row_key, 0) + 1

    # Write aggregated counts to HBase in batches
    batch = table.batch(batch_size=BATCH_SIZE)

    for row_key, view_count in product_day_views.items():
        batch.put(row_key.encode(), {
            b"metrics:view_count": str(view_count).encode()
        })

    batch.send()
    print(f"  Loaded product performance from {json_file}: "
          f"{len(product_day_views):,} product-day combinations")
    return len(product_day_views)


def main():
    print("Connecting to HBase...")
    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)

    sessions_table     = conn.table("sessions")
    performance_table  = conn.table("product_performance")

    # Load all session chunk files matching the wildcard pattern
    session_files = sorted(glob("sessions_*.json"))

    if not session_files:
        print("No session files found. Make sure sessions_*.json files are present.")
        conn.close()
        return

    print(f"Found {len(session_files)} session file(s): {session_files}\n")

    # --- Sessions ---
    print("Loading sessions table...")
    total_sessions = 0
    for json_file in session_files:
        total_sessions += load_sessions(json_file, sessions_table)

    # --- Product Performance ---
    print("\nLoading product_performance table...")
    total_product_days = 0
    for json_file in session_files:
        total_product_days += load_product_performance(json_file, performance_table)

    conn.close()

    print(f"\nDone.")
    print(f"  Sessions loaded:              {total_sessions:,}")
    print(f"  Product-day rows loaded:      {total_product_days:,}")


if __name__ == "__main__":
    main()