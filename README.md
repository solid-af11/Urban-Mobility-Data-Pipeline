# 🚕 Urban Mobility Intelligence Pipeline
### _End-to-End Analytics Engineering Project_

---

## 📖 Overview

This project implements a modern **ELT (Extract, Load, Transform)** pipeline to analyze NYC Taxi & Limousine Commission (TLC) data. The goal is to provide actionable insights into urban mobility trends, peak demand periods, and revenue generation.

The pipeline is **fully automated** — a single DAG trigger in Airflow ingests raw data, loads it into PostgreSQL, and runs all dbt transformations and data quality tests automatically.

---

## 📂 Project Structure

```
urban-mobility-pipeline/
├── dags/
│   └── taxi_ingest_dags.py        # Airflow DAG: ingestion + dbt orchestration
├── dbt_project/                   # Data transformation layer (SQL)
│   ├── models/
│   │   ├── staging/               # Bronze layer: Cleaning & Type Casting
│   │   │   └── stg_trips.sql
│   │   └── marts/                 # Gold layer: Business-ready aggregations
│   │       └── daily_summary.sql
│   ├── models/schema.yml          # Source definitions & data quality tests
│   ├── packages.yml               # dbt packages (dbt_utils)
│   └── dbt_project.yml            # dbt configuration
├── dbt_profiles/
│   └── profiles.yml               # dbt connection profile (used inside Docker)
├── docker-compose.yml             # Infrastructure as Code (Airflow, Postgres)
├── .gitignore
└── README.md
```

---

## 🏗️ Technical Architecture

The pipeline follows the **Medallion Architecture** (Bronze → Gold) to ensure data quality and lineage.

```
NYC TLC Parquet File (S3)
        ↓  [Airflow: PythonOperator]
PostgreSQL — raw_nyc_trips        ← Bronze Layer (raw TEXT ingestion)
        ↓  [Airflow: BashOperator → dbt build]
PostgreSQL — stg_trips            ← Silver Layer (typed, cleaned view)
        ↓
PostgreSQL — daily_summary        ← Gold Layer (aggregated table)
        ↓
Metabase Dashboard
```

| Component | Tool |
|---|---|
| Orchestration | Apache Airflow 2.7.1 |
| Infrastructure | Docker & Docker Compose |
| Data Warehouse | PostgreSQL 15 |
| Transformation | dbt-core 1.11.7 + dbt-postgres |
| Data Quality | dbt schema tests + dbt_utils |
| Visualization | Metabase |

---

## 🛠️ Key Engineering Features

- **Fully Automated ELT:** Airflow orchestrates both ingestion and dbt transformations in a single DAG. No manual dbt runs needed.
- **Memory-Efficient Ingestion:** Pandas loads only the first 100,000 rows of the Parquet file to handle large datasets without crashing local environments.
- **Resilient Ingestion:** Uses `if_exists='replace'` to make the DAG safely re-runnable without duplicate key errors.
- **Schema-on-Read:** Raw data is loaded into PostgreSQL as-is, with strict type casting handled downstream in dbt staging models.
- **Data Quality Tests:** dbt runs `not_null` and `expression_is_true` (via dbt_utils) tests automatically on every build.
- **Observability:** Python logging tracks ingestion status in real time via Airflow task logs.
- **Containerized Environment:** Docker Compose spins up Airflow and PostgreSQL together with a single command, ensuring environment parity.

---

## 🚀 Step-by-Step Execution Guide

### 1. Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git installed
- Python 3.9+ (only needed if running dbt outside Docker)

### 2. Clone the Repository

```bash
git clone https://github.com/solid-af11/Urban-Mobility-Data-Pipeline.git
cd Urban-Mobility-Data-Pipeline
git checkout data-engg
```

### 3. Start the Infrastructure

```bash
docker compose up -d
```

Wait ~90 seconds for Airflow and PostgreSQL to initialize. Airflow will also install dbt automatically on startup.

Verify both containers are running:

```bash
docker compose ps
```

Both `airflow_runner` and `dw_postgres` should show **Up**.

### 4. Trigger the Pipeline

1. Open **http://localhost:8080** in your browser
2. Login with the auto-generated credentials (find password with: `docker logs airflow_runner 2>&1 | grep -i password`)
3. Find the **`nyc_taxi_pipeline`** DAG
4. Toggle it **ON** (unpause)
5. Click ▶️ **Trigger DAG**

The DAG will automatically run two tasks in sequence:

```
ingest_raw_data  →  run_dbt_transforms
```

Both tasks should turn **green** within ~30 seconds.

### 5. Verify the Data

```bash
# Check raw ingested data
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "SELECT COUNT(*) FROM raw_nyc_trips;"

# Check Gold layer output
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "SELECT * FROM daily_summary LIMIT 5;"
```

### 6. Teardown

```bash
docker compose down        # stop containers, keep data
docker compose down -v     # stop + delete all data (fresh start)
```

---

## 🔁 DAG Flow

```
nyc_taxi_pipeline (runs @monthly)
│
├── ingest_raw_data [PythonOperator]
│     • Downloads yellow_tripdata_2023-01.parquet from NYC TLC
│     • Loads first 100,000 rows into PostgreSQL (raw_nyc_trips)
│
└── run_dbt_transforms [BashOperator]
      • dbt build → creates stg_trips view (Silver)
      • dbt build → creates daily_summary table (Gold)
      • Runs not_null + expression_is_true data quality tests
```

---

## 🗃️ Data Models

### `stg_trips` (Silver — View)
Cleans and casts raw trip data.

| Column | Type | Description |
|---|---|---|
| pickup_time | TIMESTAMP | Trip pickup datetime |
| distance | FLOAT | Trip distance in miles |
| fare | FLOAT | Total fare amount |
| location_id | INT | Pickup location ID |

### `daily_summary` (Gold — Table)
Aggregated daily metrics for business reporting.

---

## 🧠 Technical Decisions

| Decision | Rationale |
|---|---|
| ELT over ETL | Leverage PostgreSQL's processing power for transformations |
| Schema-on-Read | Load raw data as-is to prevent ingestion failures on schema changes |
| `if_exists='replace'` | Makes the DAG idempotent and safely re-runnable |
| dbt inside Airflow container | Eliminates manual dbt runs — full automation with one DAG trigger |
| `/tmp` for dbt logs | Avoids Docker volume permission errors on dbt log writes |
| `dbt_utils` package | Enables expressive data quality tests like `expression_is_true` |

---

## 🐛 Known Issues Fixed

| Issue | Fix Applied |
|---|---|
| `profile: 'default'` mismatch | Changed to `'urban_mobility'` in `dbt_project.yml` |
| Missing `profiles.yml` | Created at `dbt_profiles/profiles.yml` for Docker use |
| Missing `packages.yml` | Added with `dbt_utils 1.1.1` |
| Missing source definition | Added `sources:` block to `models/schema.yml` |
| `schema.yml` in wrong directory | Moved from project root into `models/` |
| Wrong test syntax for `dbt_utils` | Added `arguments:` nesting |
| `PULocationID` column not found | Quoted as `"PULocationID"` for PostgreSQL case sensitivity |
| dbt log permission error in Docker | dbt runs from `/tmp` copy inside container |

---

## 🔧 Debugging Guide

### Docker Issues

**Containers not starting**
```bash
docker compose ps                        # check status of all containers
docker compose logs airflow_runner       # view full Airflow logs
docker compose logs dw_postgres          # view full Postgres logs
```

**Airflow container keeps restarting**
```bash
docker compose down -v                   # wipe all volumes
docker compose up -d                     # fresh start
```

**Port 8080 or 5432 already in use**
```bash
# Find what's using the port
sudo lsof -i :8080
sudo lsof -i :5432
# Kill the process or change the port in docker-compose.yml
```

**Can't find Airflow password**
```bash
docker logs airflow_runner 2>&1 | grep -i "password"
```

---

### Airflow DAG Issues

**DAG not appearing in the UI**
```bash
# Check the DAG file for syntax errors
python3 dags/taxi_ingest_dags.py

# Check Airflow picked it up
docker logs airflow_runner 2>&1 | grep -i "dag\|error\|import"
```

**`ingest_raw_data` task failed**
```bash
# Get the detailed task log
docker exec airflow_runner find /opt/airflow/logs -name "*.log" \
  | grep "ingest_raw_data" | tail -1 \
  | xargs docker exec airflow_runner cat
```

**`duplicate key value` error on re-run**
```bash
# Drop and recreate the raw table
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "DROP TABLE IF EXISTS raw_nyc_trips;"
# Then re-trigger the DAG
```

**`run_dbt_transforms` task failed**
```bash
# Get the detailed dbt task log
docker exec airflow_runner find /opt/airflow/logs -name "*.log" \
  | grep "run_dbt_transforms" | tail -1 \
  | xargs docker exec airflow_runner cat
```

**dbt not found inside Airflow container**
```bash
docker exec airflow_runner dbt --version
# If missing, restart the container — dbt installs on startup
docker compose restart airflow
```

---

### PostgreSQL Issues

**Cannot connect to database**
```bash
# Verify postgres is running and port is mapped
docker compose ps

# Test connection manually
docker exec -it dw_postgres psql -U admin -d nyc_warehouse -c "\dt"
```

**Check what tables exist**
```bash
docker exec -it dw_postgres psql -U admin -d nyc_warehouse -c "\dt public.*"
```

**Inspect raw data**
```bash
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "SELECT COUNT(*) FROM raw_nyc_trips;"
```

**Inspect Gold layer**
```bash
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "SELECT * FROM daily_summary LIMIT 10;"
```

**Wipe all data and start fresh**
```bash
docker compose down -v
docker compose up -d
```

---

### dbt Issues (running locally outside Docker)

**`dbt: command not found`**
```bash
export PATH=$PATH:~/.local/bin
echo 'export PATH=$PATH:~/.local/bin' >> ~/.bashrc
source ~/.bashrc
dbt --version
```

**`profile not found` error**
```bash
# Make sure profiles.yml exists at the right location
cat ~/.dbt/profiles.yml

# If missing, create it:
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << 'EOF'
urban_mobility:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: admin
      password: password123
      dbname: nyc_warehouse
      schema: public
      threads: 4
EOF
```

**`source not found` error**
```bash
# Ensure schema.yml is inside models/ not the project root
ls dbt_project/models/schema.yml
```

**`dbt debug` connection refused**
```bash
# Make sure Docker containers are running first
docker compose ps

# Then test connection
cd dbt_project && dbt debug
```

**dbt test failures**
```bash
# Run only tests to see what's failing
cd dbt_project && dbt test

# Run a specific model in isolation
dbt run --select stg_trips
dbt run --select daily_summary
```

**Clear dbt cache and rebuild from scratch**
```bash
cd dbt_project
rm -rf target/ dbt_packages/
dbt deps
dbt build
```

---

### Quick Health Check

Run this sequence to verify the entire stack is healthy:

```bash
# 1. Containers running?
docker compose ps

# 2. Airflow web server up?
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health

# 3. PostgreSQL reachable?
docker exec -it dw_postgres psql -U admin -d nyc_warehouse -c "SELECT 1;"

# 4. Raw data loaded?
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "SELECT COUNT(*) FROM raw_nyc_trips;"

# 5. Gold layer built?
docker exec -it dw_postgres psql -U admin -d nyc_warehouse \
  -c "SELECT COUNT(*) FROM daily_summary;"

# 6. dbt healthy? (local)
cd dbt_project && dbt debug
```

All six checks passing = pipeline is fully healthy ✅

---

## 👤 Author

**Pritish Tyagi** — [github.com/solid-af11](https://github.com/solid-af11)
