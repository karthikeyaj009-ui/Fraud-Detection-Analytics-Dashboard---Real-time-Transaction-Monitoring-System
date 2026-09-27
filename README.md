# Fraud Detection Analytics Dashboard — Real-time Transaction Monitoring System

A real-time fraud detection system that monitors credit card transactions as they happen, checks them against a fraud watchlist, and displays everything on an interactive dashboard. Built entirely on Databricks.

---

## What This Project Does 

Imagine you're a bank and thousands of credit card transactions are happening every second. You need to:

1. **Catch every transaction** as it flows through a Kafka message stream
2. **Check each one against a fraud watchlist** — a list of cards or merchants that have been flagged as risky
3. **Alert when a flagged card makes a transaction** or when someone spends more than their allowed limit
4. **Show all of this on a dashboard** so analysts can see what's happening in real time

That's exactly what this project does.

---

## How The Data Flows

The project uses a **three-layer architecture** (called the "medallion" pattern). Think of it like a water filtration system — data gets cleaner and more useful at each stage:

```
Kafka Stream (credit card transactions)          Fraud Watchlist (JSON files)
         |                                              |
         v                                              v
    +---------+                                    +---------+
    | BRONZE  |  <-- Raw data as-is, no cleaning      | BRONZE  |
    +---------+                                    +---------+
         |                                              |
         v                                              v
    +---------+                                    +---------+
    | SILVER  |  <-- Parsed, cleaned, typed data       | SILVER  |
    +---------+                                    +---------+
         |                                              |
         +--------------------+-----------------------+
                              |
                              v
                        +---------+
                        |  GOLD   |  <-- Fraud detection joins + time-based summaries
                        +---------+
                              |
                              v
                        +-----------+
                        | DASHBOARD |  <-- Interactive visualizations
                        +-----------+
```

---

## Layer 1: Bronze (Raw Data Ingestion)

This is where raw data first lands. No cleaning happens here — we just capture everything.

| File | What It Does | Output Table |
| --- | --- | --- |
| `Kafka_source.py` | Connects to a Kafka topic on Confluent Cloud and reads live credit card transaction messages as they arrive | `fraud_kafka_project.bronze.bronze_credit_card_transactions` |
| `Fraud_watchlist.py` | Uses Auto Loader to pick up JSON watchlist files from a Unity Catalog volume as they're dropped in | `fraud_kafka_project.bronze.stream_fraud_watchlist` |

**What comes in from Kafka:**
Each message contains transaction details like transaction ID, customer ID, card number, merchant info, amount, payment channel, city, whether it's international, and transaction status.

**What comes in from the watchlist:**
Each JSON file contains a fraud watchlist entry with a watchlist ID, the entity (card number) being watched, the risk level, the reason it was flagged (e.g., phishing, blacklisted merchant), who reported it, and when it becomes effective.

---

## Layer 2: Silver (Cleaning & Transformation)

This is where we parse, clean, and add structure to the raw data.

| File | What It Does | Output Table |
| --- | --- | --- |
| `silver_transaction_transformations.py` | Takes raw Kafka messages, unpacks the JSON inside each message into proper columns (transaction ID, amount, card number, etc.), and adds timestamps for tracking | `fraud_kafka_project.silver.silver_credit_card_transactions` |
| `silver_fraud_watchlist.py` | Cleans the watchlist data — uppercases IDs, converts dates to proper timestamp format, and organizes columns | `fraud_kafka_project.silver.cleaned_fraud_watchlist` |
| `my_transformation.py` (batch customer process) | Processes customer data using **Auto CDC** (Change Data Capture). This means if a customer's info changes (like their transaction limit), only the changed rows get updated — it doesn't reprocess everything. Uses SCD Type 1 (keeps only the latest version). | `fraud_kafka_project.silver.customers` |

---

## Layer 3: Gold (Fraud Detection & Analytics)

This is where the magic happens. We combine data from multiple sources to actually detect fraud and create summaries for the dashboard.

| File | What It Does | Output Table |
| --- | --- | --- |
| `strema_stream_join.py` | **The core fraud detector.** Joins the live transaction stream with the live fraud watchlist stream (matching card numbers) and adds customer details. Uses watermarks (5-minute tolerance) to handle late-arriving data. If a card on the watchlist makes a transaction, it shows up here. | `fraud_kafka_project.gold.fraud_dectection` |
| `static_table_join and stream_table_join.py` | Joins customer data (static table) with the transaction stream to find transactions where the amount exceeds the customer's allowed transaction limit | `fraud_kafka_project.gold.gold_fraud` (via `transaction_alert` view) |
| `Aggregrations_tumbling_window.py` | Counts how many transactions happen in each 1-minute window (tumbling = non-overlapping fixed windows) | `fraud_kafka_project.gold.transaction_count` |
| `aggregartions_sliding_window.py` | Counts transactions in 5-minute windows that slide every 1 minute (sliding = overlapping windows, gives a smoother trend) | `fraud_kafka_project.gold.transactions_sliding_window` |

---

## Fraud Watchlist Generation

| File | What It Does |
| --- | --- |
| `fraud_watchlist/File_generator` (notebook) | Reads `fraud_watchlist.csv` and generates individual JSON files (one per watchlist entry), writing them to a Unity Catalog volume with a 5-second delay between each file. This simulates a real-time feed of watchlist updates. It also remembers where it left off, so you can re-run it without duplicating entries. |
| `fraud_watchlist/fraud_watchlist.csv` | The source CSV file containing the fraud watchlist entries |

---

## Dashboard: Fraud Detection Analytics

The dashboard has **2 pages** with **23 widgets** total:

### Page 1: Fraud Detection Overview
A high-level view of what's happening with flagged transactions.

* **KPI Counters:** Total Transactions, Total Amount (INR), Unique Customers, Avg Amount (INR)
* **Bar Charts:** Risk Level Distribution, Fraud Reason Code, Transactions by Merchant Category, Customer Segment Distribution, Top Cities by Transactions
* **Pie Charts:** Transaction Status, Payment Channel Distribution, International vs Domestic, Card Type Distribution

### Page 2: Transaction Trends & Pipeline Metrics
A deeper dive into trends over time and pipeline health.

* **KPI Counters:** Total Transactions, Watchlist Entries, Sliding Window Total
* **Line Chart:** Transaction Volume Over Time (hourly)
* **Bar Charts:** Pipeline Layer Counts (Bronze/Silver/Gold record counts), Watchlist by Risk Level, Watchlist by Reason Code, Avg Amount by Category, Amount by Risk Level
* **Pie Chart:** Watchlist by Reported Source

**Tables the dashboard queries:**

| Dataset | Table |
| --- | --- |
| Pipeline Layer Counts | `fraud_kafka_project.bronze.bronze_credit_card_transactions`, `silver_credit_card_transactions`, `gold.fraud_dectection` |
| Gold Fraud Data | `fraud_kafka_project.gold.fraud_dectection` |
| Watchlist Data | `fraud_kafka_project.bronze.stream_fraud_watchlist` |
| Sliding Window Data | `fraud_kafka_project.gold.transactions_sliding_window` |

---

## Project Folder Structure

```
fraud_kafka_project/
|
|-- fraud_watchlist/
|   |-- File_generator (notebook)     # Generates JSON watchlist files from CSV
|   |-- fraud_watchlist.csv           # Source watchlist data
|
|-- batch customer process/
|   |-- transformations/
|       |-- my_transformation.py      # Auto CDC for customer data (SCD Type 1)
|
|-- Kafka_fraud_transactions_project/
|   |-- bronze/
|   |   |-- Kafka_source.py           # Ingest from Kafka stream
|   |   |-- Fraud_watchlist.py        # Ingest watchlist JSON files via Auto Loader
|   |
|   |-- silver/
|   |   |-- silver_transaction_transformations.py   # Parse & clean Kafka messages
|   |   |-- silver_fraud_watchlist.py               # Clean watchlist data
|   |
|   |-- gold/
|       |-- strema_stream_join.py                    # Fraud detection: transactions x watchlist
|       |-- static_table_join and stream_table_join.py  # Transactions exceeding customer limit
|       |-- Aggregrations_tumbling_window.py         # 1-minute transaction counts
|       |-- aggregartions_sliding_window.py           # 5-minute sliding transaction counts
|
|-- Fraud Detection Analytics.lvdash.json   # The dashboard file
|-- explore (notebook)                        # Exploration & testing
|-- files_test (notebook)                      # File ingestion testing
```

---

## Technologies Used

* **Databricks Delta Live Tables (DLT)** — Declarative pipeline framework that automatically manages the data flow between layers
* **Apache Kafka (Confluent Cloud)** — Real-time message streaming for credit card transactions
* **Auto Loader (CloudFiles)** — Automatically picks up new files as they arrive in cloud storage
* **Unity Catalog** — Centralized governance for tables, schemas, and volumes
* **Structured Streaming with Watermarks** — Handles late-arriving data gracefully (up to 5-10 minute tolerance)
* **Auto CDC (Change Data Capture)** — Efficiently processes only changed customer records
* **Windowed Aggregations** — Tumbling (fixed) and sliding (overlapping) time windows for trend analysis
* **Databricks SQL Dashboards (Lakeview)** — Interactive visualizations with counters, bar charts, pie charts, and line charts

---

## How To Run

1. **Set up Unity Catalog:** Ensure the `fraud_kafka_project` catalog exists with `bronze`, `silver`, and `gold` schemas, plus the required UC volumes for file storage.

2. **Generate watchlist files:** Run the `File_generator` notebook to create JSON watchlist entries from the CSV.

3. **Run the pipeline:** Create a DLT pipeline in Databricks and add all the `.py` files from the bronze, silver, and gold folders (plus the batch customer process). Start the pipeline update.

4. **View the dashboard:** Open the `Fraud Detection Analytics` dashboard to see real-time fraud detection metrics and trends.

---

## Key Fraud Detection Logic

The system flags a transaction as potentially fraudulent when:

1. **The card number matches an entry on the fraud watchlist** — This is the stream-stream join in `strema_stream_join.py`. If a card that's been reported (for phishing, blacklisted merchant, etc.) makes a transaction, it gets flagged immediately.

2. **The transaction amount exceeds the customer's transaction limit** — This is the static-stream join in `static_table_join and stream_table_join.py`. If someone spends more than their bank-assigned limit, it shows up as an alert.

Both checks run in real time as transactions flow through the system.