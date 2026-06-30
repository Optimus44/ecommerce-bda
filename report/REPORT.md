# Big Data Analytics System — E-Commerce Platform

This report documents a university big data systems project implementing three complementary technologies — MongoDB, HBase, and Apache Spark — over a single shared e-commerce dataset. Each technology is containerised with Docker and is responsible for a distinct part of the system: MongoDB for document storage, HBase for time-series session and product metrics, and Spark for batch processing and cross-source analytics.

## 1. System Architecture

The project is built around one principle: a single generated dataset, consumed independently by three different storage and processing technologies.

The `dataset_generator.py` script produces all source data with fixed random seeds (`np.random.seed(42)`, `random.seed(42)`, `Faker.seed(42)`), guaranteeing byte-for-byte reproducible output regardless of which machine runs it. This reproducibility was important for development, since it meant queries and figures could be verified against a known, stable dataset rather than data that changed between runs.

MongoDB stores the product catalogue, user profiles, and transaction history as JSON documents, running in a Docker container based on `mongo:7.0` (container name `mongo-ecommerce`, database `ecommerce`). HBase stores time-series user session data and product performance metrics, running in a Docker container based on `harisekhon/hbase` (container name `hbase-project`), accessed through the Thrift API on port 9090. Spark reads the same generated JSON files directly from disk; it does not connect to MongoDB or HBase at runtime. Integration across the three systems therefore happens at the data level rather than through live database connectors — all three technologies consume the same generated dataset independently. Spark itself runs inside a `jupyter/pyspark-notebook` Docker container.

The resulting data flow is linear: `dataset_generator.py` produces JSON files, which are then loaded independently by the MongoDB loader, the HBase loader, and the Spark notebook.

## 2. Data Generation

The generator produces a fixed, deterministic dataset summarised below.

| Entity | Count |
|---|---|
| Users | 2,000 |
| Products | 1,500 |
| Categories | 25 |
| Transactions | 40,000 |
| Sessions | 150,000 (sessions_0.json: 100,000; sessions_1.json: 50,000) |
| Observation window | 90 days |

Sessions break down by outcome as 20.3% converted, 44.4% abandoned, and 35.3% browsed without converting. Device usage is evenly split across mobile (33.4%), tablet (33.5%), and desktop (33.1%).

Each transaction contains an embedded array of line items, each carrying `product_id`, `quantity`, `unit_price`, and `subtotal`. A subset of transactions carries a null `session_id`, reflecting transactions generated independently of the session-browsing flow rather than as the endpoint of a tracked session.

## 3. MongoDB Schema Design and Implementation

### 3.1 Design Criteria

Every embed-versus-reference decision in the schema was made against four consistent criteria: ownership and locality (is the data only ever read together with its parent, or queried independently?), duplication risk (would embedding copy this data across many parent documents?), growth boundedness (can the embedded structure grow without limit, or is it naturally capped?), and, for transactional data specifically, historical accuracy — whether a value must stay frozen at the moment it was recorded even if the live underlying data later changes.

### 3.2 Products

Each product document holds `product_id`, `name`, `category_id`, `base_price`, `current_stock`, `is_active`, an embedded `price_history` array of `{price, date}` entries, and a `creation_date`.

`price_history` is embedded, while category is referenced through `category_id` alone. Price history belongs exclusively to its product, is never queried independently of it, and is naturally bounded — in practice between zero and three entries per product — so all three embedding criteria apply cleanly. Category, by contrast, is shared across roughly 60 products on average; embedding category data inside every product would duplicate it 1,500 times over and require updating every affected product whenever a category was renamed. Keeping `category_id` as a reference avoids this duplication.

Direct inspection of the generated `products.json` — all 1,500 records — confirmed that no `subcategory_id` field exists on products, only `category_id`. The schema and all subsequent queries reflect the dataset as actually generated, not an idealised version of it.

### 3.3 Users

Each user document holds `user_id`, an embedded `geo_data` object (city, state, country), `registration_date`, and `last_active`.

`geo_data` is embedded because it is small, owned exclusively by the user, and never queried independently of the user record. A cached purchase-history summary field — a commonly suggested optimisation — was deliberately rejected. Such a field introduces a synchronisation problem: every new transaction would need to correctly update the cached value, and any missed update produces a silently stale, incorrect figure with no built-in way to detect the drift. Since purchase metrics can be computed on demand via aggregation at acceptable latency once properly indexed, the simpler, always-correct design was chosen over a denormalised cache that trades correctness for marginal read speed.

### 3.4 Transactions

Each transaction document holds `transaction_id`, `session_id`, `user_id`, `timestamp`, an embedded `items` array, `subtotal`, `discount`, `total`, `payment_method`, and `status`. Each line item carries `product_id`, `quantity`, `unit_price`, and `subtotal`.

`items` is embedded as a hybrid structure: a `product_id` reference paired with a price-and-quantity snapshot, rather than a bare reference or a fully duplicated product document. This is the most consequential schema decision in the project. A transaction is a permanent record of what happened at checkout. If line items only referenced products and resolved price live at read time, a later price change — confirmed to happen routinely through the `price_history` array — would cause historical transactions to silently misreport what the customer actually paid. Storing `unit_price` and `subtotal` as a point-in-time snapshot is therefore required for correctness, not a stylistic preference. `product_id` is retained alongside the snapshot so current product metadata can still be resolved via `$lookup` for reporting purposes, without bloating every transaction with redundant mutable fields.

### 3.5 Sessions

Each session document holds `session_id`, `user_id`, `start_time`, `end_time`, `duration_seconds`, `geo_data`, `device_profile`, `viewed_products`, an embedded `page_views` array, `cart_contents`, `conversion_status`, and `referrer`.

`page_views` is embedded within each session document, while sessions themselves are referenced from users via `user_id`, not embedded inside the user document. A session's page views describe one continuous browsing journey, are never queried independently, and are naturally bounded — minutes to hours of browsing produces a handful to a few dozen events — so embedding allows a single read to return the full journey with no joins. The reverse relationship, however, is unbounded: one user generates many sessions over the lifetime of an account. Embedding all of a user's sessions inside the user document would risk approaching MongoDB's 16MB document size ceiling at scale, and would force every lightweight profile read to load the user's entire browsing history. Sessions are therefore kept as a top-level collection that references `user_id`, rather than embedded.

### 3.6 Known Gap: Categories

`categories.json` was not loaded into the working MongoDB instance for this implementation. Products correctly reference categories through `category_id`, but category-based aggregation — such as revenue grouped by category — can currently only group by the opaque `category_id` value, not a human-readable category name. This is documented here as a scoped limitation rather than corrected after the fact, since the queries and findings below reflect the system as actually built.

### 3.7 Data Loading

MongoDB was run via Docker Compose rather than a manual `docker run`, specifically for reproducibility — container `mongo-ecommerce`, image `mongo:7.0`. All collections were loaded using `mongoimport`, wrapped in `scripts/load_data.sh`, with `--drop` applied per collection so that reloading the dataset is safe and idempotent. All five source files imported with zero failed documents:

| Collection | Documents | Source |
|---|---|---|
| products | 1,500 | products.json |
| users | 2,000 | users.json |
| transactions | 40,000 | transactions.json |
| sessions | 150,000 | sessions_0.json (100,000) + sessions_1.json (50,000) |

This represents the complete generated dataset — approximately 280MB of raw JSON — rather than a sampled subset. Full ingestion completed in seconds and avoids introducing sampling bias into the analytics that follow. A PyMongo access layer was also implemented alongside the `mongosh` shell, to support downstream integration with the Spark component.

### 3.8 Pre-Analysis Data Validation

Before computing any revenue figures, the common assumption that only `status: "completed"` transactions represent valid revenue was tested directly against the data rather than assumed:

```javascript
db.transactions.aggregate([{ $group: { _id: "$status", count: { $sum: 1 } } }])
```

The result showed: processing 4,907, delivered 4,898, completed 25,239, shipped 4,956. The dataset defines no `cancelled` or `failed` status, meaning every recorded transaction represents a charge that genuinely occurred. Filtering to `completed` only would have silently excluded 14,761 transactions — 36.9% of the dataset. For this reason, all revenue figures in this report aggregate across all four statuses, not completed transactions alone.

### 3.9 Aggregation Pipeline 1 — Product Popularity by Revenue

This pipeline answers a direct business question: which products generate the most revenue, and how many units do they move?

```javascript
db.transactions.aggregate([
  { $unwind: "$items" },
  { $group: {
      _id: "$items.product_id",
      total_revenue: { $sum: "$items.subtotal" },
      units_sold:   { $sum: "$items.quantity" }
  }},
  { $sort: { total_revenue: -1 } },
  { $limit: 5 },
  { $lookup: { from: "products", localField: "_id", foreignField: "product_id", as: "product_info" }},
  { $unwind: "$product_info" },
  { $project: { _id: 0, product_id: "$_id", product_name: "$product_info.name", total_revenue: 1, units_sold: 1 }}
])
```

The top five products by revenue were:

| product_id | product_name | total_revenue ($) | units_sold |
|---|---|---|---|
| prod_00724 | Open-Source User-Facing Matrix | 82,913.04 | 168 |
| prod_00154 | Switchable Hybrid Focus Group | 80,210.25 | 135 |
| prod_00592 | Proactive Scalable Policy | 79,587.36 | 162 |
| prod_00967 | Streamlined 24Hour Complexity | 79,096.05 | 145 |
| prod_00074 | Configurable Disintermediate Success | 76,883.04 | 148 |

The `$unwind` stage expands the 40,000 transaction documents into 85,799 individual line items — an average of 2.14 distinct products per transaction — which is necessary because revenue must be attributed per product, and `product_id` only exists at the line-item level. `$group` then aggregates per product across every transaction in which it appeared. The subsequent `$lookup`, `$unwind`, and `$project` stages resolve the bare `product_id` back to a human-readable name. Taken together, this pipeline combines array deconstruction, cross-document aggregation, ranking, and a cross-collection join, exceeding the complexity of a single-stage filter or count.

### 3.10 Aggregation Pipeline 2 — User Segmentation by Purchase Frequency

This pipeline answers a second business question: how does the user base segment by purchase frequency, and which segment should a re-engagement campaign target?

```javascript
db.transactions.aggregate([
  { $group: { _id: "$user_id", num_transactions: { $sum: 1 } } },
  { $bucket: {
      groupBy: "$num_transactions",
      boundaries: [0, 5, 15, 30, 100],
      default: "other",
      output: { num_users: { $sum: 1 } }
  }}
])
```

| Segment (transaction count) | Users | % of user base |
|---|---|---|
| 5–14 (low frequency) | 244 | 12.2% |
| 15–29 (mid frequency) | 1,702 | 85.1% |
| 30–99 (high frequency) | 54 | 2.7% |

No segment exists for 0–4 transactions: every one of the 2,000 users made at least 5 purchases over the dataset's roughly 90-day window. This is a substantive finding in its own right — a "re-engage inactive users" campaign has no true zero- or low-engagement target audience in this dataset. The nearest viable target is the 5–14 group, at 244 users and 12.2% of the base. This distribution more plausibly reflects the synthetic generator's behaviour than a realistic e-commerce population, since real platforms typically show a heavier long tail of one-time or inactive users, and this should be caveated whenever the finding is used to support a business recommendation.

A secondary finding emerged during this analysis: an unbucketed ranking by raw transaction count surfaces extreme outliers first — the single most frequent buyer made 38 transactions — which can make high purchase frequency appear more typical than it actually is. The `$bucket` stage corrects for this by revealing the true population shape, with 85.1% of users clustering tightly in the 15–29 range.

### 3.11 Indexing Strategy and Performance

Index decisions were derived from the access patterns exercised above, and backed by `.explain("executionStats")` evidence rather than assumed.

With no index on `transactions.user_id`, a lookup for a single user's transactions scanned the entire collection:

```javascript
db.transactions.find({ user_id: "user_000215" }).explain("executionStats")
// totalDocsExamined: 40000, executionTimeMillis: 25
```

All 40,000 transaction documents were examined via a collection scan to locate this one user's records. After creating the index with `db.transactions.createIndex({ user_id: 1 })`, the same kind of query for a different user examined only the matching documents:

```javascript
db.transactions.find({ user_id: "user_000577" }).explain("executionStats")
// totalDocsExamined: 9, executionTimeMillis: 0
```

`totalDocsExamined` dropped to exactly 9 — the true number of matching transactions for that user, confirmed independently as the user with the fewest transactions in the dataset. An index narrows scanning to exactly the matching documents; it does not guarantee a count of 1 unless the indexed value is itself unique.

Three indexes were created, summarised below.

| Collection | Index | Rationale |
|---|---|---|
| transactions | `{ user_id: 1 }` | Supports per-user lookups underlying Pipeline 2 and any future user-level join |
| transactions | `{ status: 1 }` | Supports the status-distribution validation query and future filtering by fulfillment state |
| products | `{ product_id: 1 }` | Supports the `$lookup` join in Pipeline 1 — without it, every `$lookup` execution performs a full collection scan of products per matched document |

A compound index on `{ status: 1, timestamp: 1 }` is recommended as a near-term addition, with the equality field placed before the range field per standard compound index design, to support revenue-by-period analysis filtered jointly on status and date range — a pattern named explicitly in the project brief but not yet implemented as a third pipeline.

A deliberate write/read trade-off was applied across collections. The `sessions` collection — 150,000 documents, the highest-volume and most write-heavy collection, and the closest analogue here to a continuously appended event stream — was left with only its default `_id` index. The `products` collection, by contrast, is small (1,500 documents) but rarely written and constantly read and joined, so it was prioritised for indexing instead. This reflects the general principle that every index imposes a cost on every future write to its collection, a cost worth paying on read-heavy, write-light collections and avoided where writes dominate.

### 3.12 Schema Decisions Summary

| Relationship | Decision | Primary justification |
|---|---|---|
| Product → price_history | Embed | Owned, naturally bounded, never queried independently |
| Product → category | Reference (category_id) | Shared by many products; avoids duplicating category metadata 1,500 times |
| Transaction → line items | Embed, hybrid (snapshot + product_id) | Owned and historically accurate — recorded price must not retroactively change |
| Transaction → user | Reference (user_id) | One user generates many transactions; embedding would duplicate the user profile per transaction |
| Session → page_views | Embed | Owned, bounded to one browsing journey, always read together |
| User → sessions | Reference (user_id on session) | Unbounded growth over account lifetime; risks document-size limits and bloats lightweight profile reads if embedded |
| User → purchase summary | Not cached; computed on demand | Avoids a stale/desynchronised denormalised field; acceptable performance once indexed |

### 3.13 MongoDB-Specific Limitations

Several limitations were identified during MongoDB implementation. `categories.json` was not loaded, so category-based analytics is limited to the opaque `category_id` value rather than resolved category names. The synthetic data distribution skews engagement metrics — the segmentation result contains no low-engagement users at all, which is more likely a generator artifact than a realistic population. No cached user purchase summary exists by design, favouring correctness over a marginal read-latency improvement; a periodically refreshed cache would be a reasonable middle ground at larger scale. Aggregated revenue figures occasionally show floating-point artifacts (for example, 76883.04000000001) due to BSON's double type; `Decimal128` would be required for production-grade financial accuracy. Finally, the generated `products.json` omits the `subcategory_id` field present in the brief's illustrative sample data, so the implementation reflects the dataset as actually generated rather than the brief's example schema.

## 4. HBase Schema Design and Implementation

Every cell in HBase is identified by four coordinates: row key, column family, column qualifier, and timestamp. The row key is the only built-in index, and rows are stored in lexicographic order by row key, which makes prefix scans and range scans fast. Column families are the unit of physical storage — each family is stored in separate files on disk, so a query targeting one family never reads the others. These properties directly shaped the two schema designs below.

### 4.1 Sessions Table

The row key is `user_id#reverse_timestamp`, for example `user_000603#8224310960483`.

`user_id` is placed first because the primary query is "retrieve all sessions for a specific user." Placing it first ensures all sessions for the same user are stored physically adjacent on disk, so retrieval becomes a prefix scan: HBase jumps to the first row matching `user_000603#` and reads forward until the prefix no longer matches, without touching any other user's data.

The reverse timestamp is computed as `9999999999999 - actual_timestamp_ms`. Because HBase sorts ascending, a normal timestamp would surface the oldest sessions first. Reversing it corrects this — more recent sessions produce smaller reverse-timestamp values and therefore sort earlier within each user's prefix — which matches the dominant access pattern for session analytics: most-recent-first.

Four column families separate data by access pattern: `info` holds session_id, duration_seconds, conversion_status, and referrer — core metadata most frequently queried together; `device` holds type, os, and browser, queried together for device-breakdown reports; `geo` holds city, state, country, and ip_address, queried together for location analytics; and `activity` holds viewed_products and page_view_count, the behavioural engagement data. This physical separation matters in practice: a device-breakdown report reads only the `device` family's files, while the `geo`, `info`, and `activity` files are never opened for that query.

### 4.2 Product Performance Table

The row key is `product_id#date`, for example `prod_00950#2026-04-08`.

`product_id` is placed first because the primary query is "retrieve all performance metrics for a specific product across all dates," and placing it first groups all rows for a given product contiguously on disk. The date portion is left in normal, non-reversed form, because product performance analysis is chronological — tracking how view counts evolve over time — and a reverse timestamp would invert the resulting trend line. A standard ISO date string (`YYYY-MM-DD`) sorts lexicographically in ascending chronological order, which is the natural direction for trend analysis.

This table uses a single column family, `metrics`, containing `view_count`, and is designed to accommodate `cart_add_count` and `purchase_count` as additional qualifiers without requiring a schema migration. A single column family is appropriate here because all three metrics are always queried together as a unified daily snapshot — there is no analytical reason to retrieve `view_count` without `purchase_count`.

### 4.3 Data Loading

Loading was implemented in Python using `happybase 1.3.0` over the Thrift protocol on port 9090. Batch writing with `batch_size=1000` reduces network round-trips from 150,000 individual RPC calls down to approximately 150 batched calls.

For the product performance table specifically, view counts were accumulated in a Python dictionary in memory before any HBase writes occurred. This ensures each product-day combination is written exactly once with its final aggregated count, rather than being incremented in place, which would produce incorrect counts on re-runs of the loader.

Loading results: sessions_0.json contributed 100,000 sessions and sessions_1.json contributed 50,000, for a total of 150,003 session rows — the extra 3 rows coming from manual test rows used during schema verification. The product performance table received 208,286 rows in total, 118,491 from file 0 and 89,795 from file 1.

### 4.4 Queries and Performance

The first query retrieves all sessions for a specific user:

```
scan 'sessions', {STARTROW => 'user_000603#', STOPROW => 'user_000603$'}
```

This returned 80 rows in 0.67 seconds out of 150,003 total rows. The `$` character (ASCII 36, just above `#` at ASCII 35) provides a clean upper boundary that excludes any other user's rows.

The second query retrieves sessions for that same user within a specific time range, using the composite row key as a two-dimensional bound:

```
scan 'sessions', {STARTROW => 'user_000603#8224310960483', STOPROW => 'user_000603#8224392710370'}
```

This returned 1 row in 0.057 seconds, demonstrating that the composite row key supports a user-prefix-plus-timestamp-bound range scan in a single operation.

The third query retrieves a product's performance across all dates:

```
scan 'product_performance', {STARTROW => 'prod_00950#', STOPROW => 'prod_00950$'}
```

This returned 90 rows — 90 days of metrics — in 0.13 seconds out of 208,286 total product-day rows.

The fourth query filters by a non-key field, `conversion_status`, using a `SingleColumnValueFilter`. Because `conversion_status` is stored in a cell rather than in the row key, this query requires a full table scan. This query was included deliberately to illustrate HBase's fundamental constraint: queries that do not use the row key cannot avoid reading every row. The correct solution for making this query fast would be a secondary index table — a separate HBase table with `conversion_status` as the row key prefix, pointing back to the sessions table.

## 5. Spark Batch Processing and Analytics

### 5.1 Environment

PySpark 3.5.0 runs inside a `jupyter/pyspark-notebook` Docker container. All JSON files are mounted into the container at `/home/jovyan/data/` and read directly by Spark, with no database connection required.

### 5.2 Data Loading Finding

All JSON source files use a single-array format rather than JSON Lines, which requires `.option("multiLine", True)` on every read. Omitting this option does not raise an error — instead, Spark silently produces a `_corrupt_record` column, a failure mode that is easy to miss without explicit schema inspection.

### 5.3 Cleaning and Normalisation Pipeline

Five issues were identified and resolved during cleaning. Timestamp strings — `registration_date`, `last_active`, and the transaction `timestamp` field — are inferred by Spark as plain strings rather than timestamps, requiring explicit `to_timestamp()` and `date_format()` calls. Nested fields such as `geo_data` and `device_profile` are inferred as nested structs and accessed via dot notation, for example `col("geo_data.country")`, or extracted with `withColumn` and `col()`.

A more subtle issue arose from an ambiguous `subtotal` column: the raw transactions data contains a `subtotal` field at both the transaction level and the line-item level. Exploding the `items` array without renaming causes an `AMBIGUOUS_REFERENCE` error on any subsequent reference to `subtotal`. This was resolved by aliasing the transaction-level field as `order_subtotal` and the item-level field as `line_subtotal` during cleaning.

Spark's automatic schema inference also misreads the `cart_contents` field in the sessions data: rather than inferring it as a map, it infers a struct with one field per product ID across the entire 1,500-product catalogue. This was resolved by supplying an explicit `MapType(StringType(), StructType(...))` schema for the field. Finally, approximately 20% of transactions were generated independently of the session flow and carry a null `session_id`; these were handled with `fillna({"session_id": "no_session"})`.

### 5.4 Spark SQL

Cleaned DataFrames were registered as temporary views using `createOrReplaceTempView()`, enabling three representative SQL queries. A multi-source join combined users with transactions on `user_id` to compute per-user total spend. A query using a common table expression and a scalar subquery identified users whose total spend exceeds the population average, with the top result being user_001552 at $43,634. A third query joined users (for country) with transactions (for revenue) to find the top five countries by revenue: KM at $424,894, GR at $418,829, NE at $402,196, LI at $394,198, and SR at $387,876.

### 5.5 Cohort Analysis

This analysis addresses a specific business question: do users who registered in different months show different spending patterns over their subsequent months as customers?

The method groups users by `registration_month`, extracted from `registration_date`, and groups transactions by `activity_month`. The two are joined on `user_id`, and a `months_since_registration` column is computed as the integer difference between activity month and registration month. The results are pivoted into a cohort table, with registration month as rows, months since registration as columns, and total revenue as values. Registration months present in the data span 2025-09 through 2026-03.

Several findings emerge from the cohort table. Revenue is consistently highest at months 3 to 5 since registration across all cohorts, reflecting a ramp-up period after users first register. The October 2025 cohort shows the highest single-month revenue of any cohort at any point: $2,804,777 at month 7 since registration. The March 2026 cohort shows $589,816 in its first month (month 0), the highest first-month revenue of any cohort, which suggests improving acquisition quality over time. The 2026-01 cohort had a first-month revenue of $1,136,390 recorded at month 2. A diagonal band of null values appears in the cohort table because cohorts registered earlier in the window have more months of history visible than recent cohorts, a direct consequence of the dataset's fixed 90-day observation window.

This observation window — set by the generator's `TIMESPAN_DAYS=90` constraint — means no cohort has more than 9 months of visible activity. The cohort analysis therefore reveals the dataset's structural time horizon as much as it reveals genuine cohort behaviour, and this is documented here as a limitation rather than treated as a finding about real customer behaviour.

## 6. Analytics Integration: Customer Lifetime Value

This analysis addresses a question that spans all three data sources: which users are most valuable to the business, accounting not just for how much they have spent but also for how frequently they engage and how often their sessions convert into purchases?

A simple revenue sum identifies high spenders but ignores engagement quality. A user who spends a given amount across 100 sessions with a 2% conversion rate is fundamentally different from a user who spends the same amount across 5 sessions with a 40% conversion rate. A combined CLV score is intended to capture both dimensions.

### 6.1 Data Sources

| Source | Contribution |
|---|---|
| transactions.json | per-user total_spend, order_count, avg_order_value, first/last purchase date |
| sessions_*.json | per-user total_sessions, avg_session_duration, conversion_rate |
| users.json | per-user registration_date, country |

All three sources were joined on `user_id`. An inner join confirmed that all 2,000 users have both transaction and session records, with the combined result containing exactly 2,000 rows.

### 6.2 CLV Formula

```
CLV score = total_spend × (1 + conversion_rate) × log(total_sessions + 1)
```

`total_spend` provides the revenue base. The `(1 + conversion_rate)` term linearly rewards users who convert a higher proportion of their sessions into purchases. The `log(total_sessions + 1)` term rewards session frequency on a diminishing-returns scale, so that a user going from 5 to 10 sessions is rewarded more, proportionally, than one going from 100 to 105 sessions — preventing extremely high-session users from dominating the score purely on volume.

### 6.3 Segmentation

Users were ranked by CLV score in descending order using the `percent_rank()` window function, then bucketed into three segments: High Value for the top 20% (percentile at or below 0.20), Mid Value for the next 30% (percentile between 0.20 and 0.50), and Low Value for the remaining 50% (percentile above 0.50).

| Segment | Users | Avg CLV Score | Avg Total Spend | Avg Conversion Rate |
|---|---|---|---|---|
| High Value | 400 | 159,557 | $29,566 | 23.6% |
| Mid Value | 600 | 120,993 | $23,073 | 21.1% |
| Low Value | 1,000 | 83,813 | $16,438 | 18.6% |

The High Value segment, comprising 20% of users, shows both the highest average spend and the highest conversion rate — both dimensions move together rather than trading off against each other. The Low Value segment is correspondingly not just lower-spending but also less engaged, with the lowest conversion rate of the three segments. This justifies directing retention and loyalty spending specifically toward the High Value segment, rather than distributing it evenly across all customers.

## 7. Visualisations and Key Findings

Four charts were produced using matplotlib and seaborn within the Spark notebook.

The first is a line chart of monthly total revenue from the transactions dataset, plotted as a time series showing the revenue trajectory across the 90-day observation window.

The second is a cohort spending heatmap, with registration month (2025-09 through 2026-03) as rows and months since registration (0 through 9) as columns, and total revenue as the cell value. The diagonal band of null values makes the 90-day observation window immediately visible as a structural pattern in the data — a limitation far more apparent from the heatmap than from the underlying numeric table — and confirms that revenue peaks at months 3 to 5 across all cohorts.

The third is a horizontal bar chart of the top 10 products by revenue across all transactions, derived from the exploded transactions DataFrame grouped by `product_id`.

The fourth is a dual-axis chart combining bars and a line for CLV segment distribution: bars show user count per segment on the left axis, while a line shows average spend per segment on the right axis. The two axes move in opposite directions — population grows from High to Low Value (400, then 600, then 1,000), while average spend falls correspondingly ($29,566, then $23,073, then $16,438). This confirms that customer value is concentrated in a numerically smaller, higher-value minority, the standard justification for prioritising retention spend on the High Value segment.

## 8. Technology Selection Justification

MongoDB was selected for document storage because the product catalogue, user profiles, and transaction history are naturally document-shaped: each entity has variable nested structures, such as price history arrays, embedded line items, and geographic data, and the relationships between entities — a transaction referencing a user and products — do not require the full relational join capability that would justify a relational database instead. MongoDB's flexible schema accommodates heterogeneous product types, and its aggregation pipeline provided sufficient analytical capability for the required queries. Embedding frequently co-accessed data, such as line items inside transactions and geographic data inside users, enables single-document reads for the most common access patterns.

HBase was selected for time-series session data because that data has three properties that make HBase a strong fit: high write volume, since sessions are generated continuously; known and fixed query patterns, always by user and optionally time-bounded; and scale potential, with 150,000 sessions in this dataset and production platforms generating millions per day. HBase's architecture is optimised for exactly these conditions — writes go to an in-memory MemStore before flushing to disk in batches, well-suited to continuous write pressure, and the row key encodes the primary query dimensions directly, enabling sub-second prefix scans. MongoDB's flexible querying would be advantageous if session query patterns were varied or unknown, but for a fixed set of time-series user lookups, HBase's design is the better fit.

Spark was selected for batch processing and analytics because the analytical tasks in this project — cleaning, cohort analysis, cross-source joins, and CLV estimation — involve scanning and aggregating across large datasets, namely 40,000 transactions and 150,000 sessions. Spark's DataFrame API and Spark SQL provide a unified abstraction for these operations, with lazy evaluation meaning transformations are not executed until an action such as `show()` or `count()` is called, allowing query plan optimisation before execution. Spark's principal advantage over single-node Pandas processing is horizontal scalability: the same code runs unchanged on a cluster with data partitioned across many machines.

## 9. Scalability Considerations

MongoDB scales horizontally via sharding. The `user_id` field is a natural shard key for the users and transactions collections, since queries filtered by `user_id` can be routed directly to the correct shard. The Atlas-managed deployment used in this project supports one-click cluster scaling.

HBase is designed for horizontal scaling: adding RegionServers to the cluster causes automatic redistribution of table regions across the expanded cluster. The sessions table's row key design specifically avoids hotspotting, since row keys are prefixed by `user_id`, a high-cardinality field, meaning writes are distributed across regions rather than concentrating on a single region.

Spark scales by adding workers to the cluster. The `jupyter/pyspark-notebook` container used here is single-node; in production this would be replaced by a multi-node Spark cluster, for example via Spark Standalone mode or Kubernetes. The notebook code requires no changes to run in distributed mode, since Spark's partitioning is transparent to the DataFrame API.

## 10. Limitations and Future Work

The 90-day `TIMESPAN_DAYS` constraint in the generator limits cohort analysis to a maximum of 9 months of visible history. Cohorts registered near the start of the observation window have longer visible histories than recent cohorts, introducing survivorship bias into the cohort comparison; a longer observation window would allow more statistically comparable cohorts.

Spark does not connect to MongoDB or HBase at runtime in this implementation — integration is at the data file level rather than via live database connectors. The MongoDB Spark Connector and the HBase-Spark Connector (SHC) would enable live, incremental processing pipelines, and were considered but excluded due to their configuration complexity relative to the available project time.

The HBase sessions table currently supports only row-key-based queries. A secondary index table, with `conversion_status` as the row key prefix, would enable efficient filtering by conversion status without requiring a full table scan.

The CLV model used in Section 6 is a heuristic proxy rather than a formal financial CLV model. A proper CLV model would incorporate discount rate, customer churn probability, and customer acquisition cost, none of which are available as inputs in the generated dataset.

MongoDB and HBase containers are deployed without authentication credentials. This is appropriate for local development and assessment but would not be appropriate for any production or networked deployment.

Finally, `categories.json` was not loaded into MongoDB; the loading script omits it. Product queries currently reference `category_id` without resolving it to a category name. A `$lookup` join to a populated categories collection would enable category-name-based reporting and is a natural next step beyond the current implementation.