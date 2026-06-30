# ecommerce-bda
### Big Data Analytics — E-Commerce Platform

A multi-technology big data analytics system built on MongoDB, HBase, and Apache Spark. The system ingests a synthetically generated e-commerce dataset and supports document storage, time-series session analytics, batch processing, cohort analysis, and Customer Lifetime Value estimation.

---

## Repository Structure

```
ecommerce-bda/
│
├── data/
│   └── dataset_generator.py         # Generates all source JSON files
│
├── mongodb/
│   ├── load_data.py                 # Loads products, users, transactions into MongoDB
│   └── queries.js                   # Aggregation pipelines (product popularity + user segmentation)
│
├── hbase/
│   ├── create_table.hbase           # HBase shell commands to create both tables
│   ├── load_data.py                 # Loads sessions + product performance into HBase
│   └── sample_queries.hbase         # HBase shell queries for verification and analysis
│
├── spark/
│   └── ecommerce_analytics.ipynb    # Full PySpark notebook (cleaning, cohort, CLV, visualisations)
│
├── visualizations/
│   ├── revenue_over_time.png
│   ├── cohort_heatmap.png
│   ├── top_products.png
│   └── clv_segments.png
│
├── docker-compose.yml               # MongoDB container definition
├── scripts/
│   └── load_data.sh                 # Shell script to import JSON files into MongoDB
└── README.md
```

---

## Architecture Overview

```
dataset_generator.py
        │
        ▼
  JSON files (users, products, transactions, sessions)
        │
        ├──────────────────────────────────────────────────────────┐
        │                                                          │
        ▼                                                          ▼
┌───────────────┐                                      ┌──────────────────┐
│   MongoDB     │                                      │     HBase        │
│  (Docker)     │                                      │    (Docker)      │
│               │                                      │                  │
│  - products   │                                      │  - sessions      │
│  - users      │                                      │  - product_      │
│  - transactions│                                     │    performance   │
│  - categories │                                      └──────────────────┘
└───────────────┘
        │                                                          │
        └──────────────────────┬───────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Spark      │
                    │   (Docker)          │
                    │                     │
                    │  Reads JSON files   │
                    │  directly — no DB   │
                    │  connection needed  │
                    │                     │
                    │  - Cleaning         │
                    │  - Cohort analysis  │
                    │  - CLV estimation   │
                    │  - Visualisations   │
                    └─────────────────────┘
```

> **Note:** Spark reads data directly from the JSON source files. It does not connect to MongoDB or HBase at runtime. The integration happens at the data level — all three technologies consume the same generated dataset.

---

## Prerequisites

Install these on your machine before doing anything else.

| Requirement | Purpose |
|---|---|
| Docker Desktop | Runs MongoDB, HBase, and Spark containers |
| Python 3.9+ | Runs the dataset generator and loading scripts |
| pip | Installs Python packages |

Verify everything is present:

```bash
docker --version
docker compose version
python3 --version
```

---

## Step 0 — Generate the Dataset

All three components consume JSON files produced by `dataset_generator.py`. Run this once from the `data/` folder before setting up any component.

```bash
cd data/
pip install faker numpy
python3 dataset_generator.py
```

Expected output:
```
Initializing dataset generation...
Generated 25 categories
Generated 1500 products
Generated 2000 users
Generating sessions and transactions...

Dataset generation complete!
- Sessions: 150,000 (target: 150,000)
- Transactions: 40,000 (target: 40,000)
- Remaining products: ...
```

This produces six files in `data/`:
```
users.json          (2,000 records)
products.json       (1,500 records)
categories.json     (25 records)
transactions.json   (40,000 records)
sessions_0.json     (100,000 records)
sessions_1.json     (50,000 records)
```

**The generation is fully reproducible.** Fixed random seeds (`np.random.seed(42)`, `random.seed(42)`, `Faker.seed(42)`) mean re-running the script on any machine produces byte-for-byte identical output.

---

## Part 1 — MongoDB

MongoDB stores the product catalogue, user profiles, and transaction history as documents.

### Setup

**Step 1 — Start the MongoDB container:**
```bash
docker compose up -d
```

Expected output:
```
[+] Running 2/2
 ✔ Network ecommerce-bda_default   Created
 ✔ Container mongo-ecommerce       Started
```

**Step 2 — Verify the container is running:**
```bash
docker ps
```
Expected: one row with container name `mongo-ecommerce`, status `Up`.

**Step 3 — Install Python dependencies:**
```bash
pip install pymongo
```

### Load the Data

From the **repo root** (the directory containing `docker-compose.yml`):

```bash
chmod +x scripts/load_data.sh
./scripts/load_data.sh
```

Expected output:
```
Target container: mongo-ecommerce, database: ecommerce

Importing products.json -> products (--drop, fresh load)...
1500 document(s) imported successfully.
Importing users.json -> users (--drop, fresh load)...
2000 document(s) imported successfully.
Importing transactions.json -> transactions (--drop, fresh load)...
40000 document(s) imported successfully.
Importing sessions_0.json -> sessions (--drop, fresh load)...
100000 document(s) imported successfully.
Importing sessions_1.json -> sessions (append)...
50000 document(s) imported successfully.

Verifying document counts...
products:     1500
users:        2000
transactions: 40000
sessions:     150000

Done.
```

The script is safe to re-run — each collection is dropped and rebuilt fresh on every run.

### Verify

```bash
docker exec -it mongo-ecommerce mongosh ecommerce --eval '
  print("products:     " + db.products.countDocuments());
  print("users:        " + db.users.countDocuments());
  print("transactions: " + db.transactions.countDocuments());
  print("sessions:     " + db.sessions.countDocuments());
'
```

Expected counts: `products: 1500 | users: 2000 | transactions: 40000 | sessions: 150000`

### Run the Aggregation Pipelines

Open `mongodb/queries.js` in VS Code with the MongoDB extension connected, or paste its contents into mongosh:

```bash
docker exec -it mongo-ecommerce mongosh ecommerce
```

The file contains two pipelines:
- **Pipeline 1:** Top 10 products by revenue (unwinds line items, groups by product, joins product names)
- **Pipeline 2:** User segmentation by purchase frequency (High / Mid / Low frequency tiers)

### Connection Details

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `27017` |
| Database | `ecommerce` |
| Auth | None (local development) |
| Connection string | `mongodb://localhost:27017/ecommerce` |

---

## Part 2 — HBase

HBase stores time-series user session data and product performance metrics.

### Setup

**Step 1 — Start the HBase container:**
```bash
docker run -d \
  --name hbase-project \
  -p 2181:2181 \
  -p 9090:9090 \
  -p 16000:16000 \
  -p 16010:16010 \
  -p 16020:16020 \
  harisekhon/hbase:latest
```

> **Apple Silicon (M1/M2/M3):** You will see a platform mismatch warning — this is expected and harmless. The container runs via Docker's built-in emulation layer and works correctly.

**Step 2 — Wait for HBase to initialise (20–30 seconds), then verify:**
```bash
docker exec -it hbase-project hbase shell <<< "status"
```
Expected: `1 active master, 0 backup masters, 1 servers, 0 dead`

**Step 3 — Install Python dependencies:**
```bash
pip install happybase
```

**Step 4 — Verify Python can connect:**
```bash
python3 -c "
import happybase
conn = happybase.Connection('localhost', port=9090)
print('Connected. Tables:', conn.tables())
"
```
Expected: `Connected. Tables: []`

### Create the Tables

Open the HBase shell and paste the contents of `hbase/create_table.hbase`:

```bash
docker exec -it hbase-project hbase shell
```

This creates two tables:
- `sessions` — with column families `info`, `device`, `geo`, `activity`
- `product_performance` — with column family `metrics`

### Load the Data

From the `data/` directory (where the JSON files are):

```bash
cd data/
python3 ../hbase/load_data.py
```

Expected output:
```
Connecting to HBase...
Found 2 session file(s): ['sessions_0.json', 'sessions_1.json']

Loading sessions table...
  Loaded 100,000 sessions from sessions_0.json
  Loaded 50,000 sessions from sessions_1.json

Loading product_performance table...
  Loaded product performance from sessions_0.json: 118,491 product-day combinations
  Loaded product performance from sessions_1.json: 89,795 product-day combinations

Done.
  Sessions loaded:         150,000
  Product-day rows loaded: 208,286
```

### Verify

Open the HBase shell and run:

```
# Both tables exist
list

# Sessions row count
count 'sessions'

# All sessions for a specific user
scan 'sessions', {STARTROW => 'user_000603#', STOPROW => 'user_000603$'}

# Product performance across all dates
scan 'product_performance', {STARTROW => 'prod_00950#', STOPROW => 'prod_00950$'}
```

See `hbase/sample_queries.hbase` for the full set of verified queries.

### Connection Details

| Setting | Value |
|---|---|
| Host | `localhost` |
| Thrift port | `9090` |
| ZooKeeper port | `2181` |
| Web UI | `http://localhost:16010` |
| Auth | None |
| Container name | `hbase-project` |

---

## Part 3 — Spark (Batch Processing, Analytics, Visualisations)

Spark reads the JSON files directly — no database connection needed. All cleaning, cohort analysis, CLV estimation, and visualisations are in a single notebook.

### Setup

**Step 1 — Start the PySpark + Jupyter container** from the `data/` directory (where the JSON files are):

```bash
cd data/

# macOS / Linux
docker run -d \
  --name spark-jupyter \
  -p 8888:8888 \
  -p 4040:4040 \
  -v "$(pwd)":/home/jovyan/data \
  jupyter/pyspark-notebook

# Windows (PowerShell)
docker run -d --name spark-jupyter -p 8888:8888 -p 4040:4040 -v "${PWD}":/home/jovyan/data jupyter/pyspark-notebook
```

**Step 2 — Get the access token:**
```bash
docker logs spark-jupyter 2>&1 | grep "http://127"
```

Copy the full URL (e.g. `http://127.0.0.1:8888/lab?token=abc123...`).

**Step 3 — Open in browser or VS Code:**

*Browser:* Paste the URL directly.

*VS Code:*
1. Install the **Jupyter** extension
2. Open `spark/ecommerce_analytics.ipynb`
3. Click the kernel selector (top right) → **Select Kernel** → **Existing Jupyter Server**
4. Paste the token URL
5. Select **Python 3 (ipykernel)**

### Run the Notebook

Open `spark/ecommerce_analytics.ipynb` and run all cells top to bottom. The notebook covers:

| Section | Content |
|---|---|
| 1. Loading | Reads all JSON files into Spark DataFrames |
| 2. Exploration | Schema inspection, filtering, projections |
| 3. Cleaning | Timestamp parsing, null handling, explode items array, MapType fix for cart_contents |
| 4. Spark SQL | Cross-source joins, subqueries, top-5 countries by revenue |
| 5. Cohort Analysis | Users grouped by registration month, spending across subsequent months |
| 6. CLV Estimation | Integrates users + transactions + sessions; segments into High / Mid / Low Value |
| 7. Visualisations | Revenue over time, cohort heatmap, top 10 products, CLV segment distribution |

### Verify

After running the loading cells, confirm exact row counts:

```python
print("users:", users_df.count())         # 2000
print("products:", products_df.count())   # 1500
print("transactions:", transactions_df.count())  # 40000
print("sessions:", sessions_df.count())   # 150000
```

### Connection Details

No database connection — Spark reads files directly from `/home/jovyan/data/` inside the container (mounted from your local `data/` folder).

| Interface | URL |
|---|---|
| Jupyter Lab | `http://127.0.0.1:8888/lab?token=<your-token>` |
| Spark UI (while jobs run) | `http://localhost:4040` |

---

## Returning After a Break

Each container persists its data across restarts as long as it is not deleted.

```bash
# MongoDB
docker start mongo-ecommerce

# HBase
docker start hbase-project
# Wait 30 seconds for HBase to reinitialise, then verify:
docker exec -it hbase-project hbase shell <<< "status"

# Spark
docker start spark-jupyter
docker logs spark-jupyter 2>&1 | grep "http://127"
# Token rotates on each start — always re-fetch it
```

> If a container is **deleted** (`docker rm`), data inside it is lost. Re-run the appropriate loading script to restore it. The JSON source files live on your host machine and are never lost.

---

## Common Issues

| Symptom | Fix |
|---|---|
| `port is already allocated` | Another container or process is using that port. Run `docker ps -a` and remove the conflicting container, or change the host port in the run command. |
| `_corrupt_record` in Spark | Missing `.option("multiLine", True)` — every JSON file is a single array, not JSON Lines. |
| `TTransportException` from happybase | HBase Thrift server not ready. Wait 30 seconds after starting the container and retry. |
| `FileNotFoundError` in HBase loader | Run the loader from the `data/` directory, not from `hbase/`. |
| `KeeperErrorCode = NoNode for /hbase/master` | HBase still initialising. Wait and retry. |
| Container name conflict | Run `docker rm <name>` to remove the old container, then re-run the `docker run` command. |
| Spark token invalid | Token resets each time the container starts. Re-run `docker logs spark-jupyter 2>&1 | grep "http://127"`. |

---

## Dataset Summary

| Metric | Value |
|---|---|
| Users | 2,000 |
| Products | 1,500 |
| Categories | 25 |
| Transactions | 40,000 |
| Sessions | 150,000 |
| Observation window | 90 days |
| Random seeds | Fixed (42) — fully reproducible |