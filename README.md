# COVID‑19 ETL & Power BI Dashboard 🚀

This repo contains a full data engineering pipeline that:
- Extracts COVID‑19 data via API
- Transforms data with PySpark
- Loads and merges data into PostgreSQL
- Visualizes insights using Power BI

---

## 🌐 Project Overview

1. **Extract**: 
   - Fetches data from the [disease.sh](https://disease.sh) API for continents & countries
2. **Transform**:
   - Uses PySpark to clean, cast, round, join, and add timestamps
3. **Load**:
   - Writes to PostgreSQL staging tables
   - Runs PL/pgSQL MERGE logic to upsert into dimensional tables
4. **Visualize**:
   - Power BI report with drill-down by continent → country

---

## ⚙️ Setup Instructions

### 1. Clone the repo
```bash
git clone https://github.com/harsha-d-26/covid-19_data_analysis.git
cd covid-19_data_analysis
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment
- Create a .env file in the project folder
- Set PG_DB, PG_USER, PG_PASSWORD, PG_HOST, PG_PORT

### 4. PostgreSQL Driver
- Download the JDBC driver from https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
- Place the .jar file pyspark_installed_folder\pyspark\jars folder

### 5. PostgreSQL DB tables
- Create a database named COVID19DB
- Execute the following command to create the tables and procedure
```bash
psql -U <username> -d <database> -f Tables_DDL.sql
psql -U <username> -d <database> -f merge_data_proc.sql
```

### 6. Execute ETL Script
```bash
python etl_script.py
```

### 7. Open Power BI Dashboard
- Open PBI Dashboard.pbix
- To refresh data connect to PostgreSQL DB under data source menu

📊 Dashboards & Highlights

![image](https://github.com/user-attachments/assets/8a3e9739-045e-47a0-ad36-b64a9716e07d)

![image](https://github.com/user-attachments/assets/b18005ef-2e76-4288-a9de-e7c38bb6f642)

🧩 Power BI Semantic Model
 - Tables: dim_location, fact_continent_cases, fact_country_cases
 - Relationships: continent_id, country_id
 - Supports: hierarchical drill-down, time-series, KPIs

🧠 Best Practices
- ✅ Modular, reusable code
- ✅ Staging + Merge enables incremental loading
- ✅ .env-based config for security
- ✅ README-ready for projects and portfolios

