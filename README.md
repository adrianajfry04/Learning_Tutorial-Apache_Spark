# 🏫 Brazilian School Census ETL Pipeline with Apache Spark

A data engineering project that builds a **Star Schema ETL pipeline** using Apache PySpark, PostgreSQL, and Metabase to analyze Brazilian school census data (Censo Escolar) from INEP.

> Based on the Medium article: [Creating a Simple ETL Pipeline With Apache Spark](https://joaopedro214.medium.com/creating-a-simple-etl-pipeline-with-apache-spark-825cc17c8cf6) by João Pedro

---

## 📊 Dashboard Preview

The final Metabase dashboard includes:
- Latest census year & total school count (number cards)
- School distribution by region and dependency type (Sankey chart)
- Total schools by region (bar chart)
- Schools by dependency type — Federal, State, Municipal, Private (pie chart)
- Schools by number of computers (area chart)
- Schools with internet access (row chart)
- Top 10 municipalities by school count (table)

---

## 🗂️ Project Structure

```
spark-etl/
├── data/
│   └── .gitkeep              # Placeholder — put ESCOLAS.CSV here
├── extract.py                # Downloads and extracts census CSV from INEP
├── transform_load.py         # PySpark ETL — builds star schema in PostgreSQL
├── check_columns.py          # Utility to inspect CSV column names
├── docker-compose.yaml       # Spins up PostgreSQL + Metabase containers
├── requirements.txt          # Python dependencies
├── postgresql-42.7.11.jar    # JDBC driver for Spark → PostgreSQL connection
├── star_schema.ipynb         # Jupyter notebook version of the ETL
└── README.md
```

---

## 🏗️ Architecture

```
INEP Open Data Portal
        │
        ▼
  extract.py
  (download + extract ESCOLAS.CSV)
        │
        ▼
  transform_load.py
  (PySpark reads CSV → builds Star Schema → writes to PostgreSQL)
        │
        ▼
  PostgreSQL (Docker)
  ├── DIM_LOCAL
  ├── DIM_TP_DEPENDENCIA
  ├── DIM_TP_LOCALIZACAO
  ├── DIM_IN_BIBLIOTECA
  ├── DIM_IN_REFEITORIO
  ├── DIM_IN_COMPUTADOR
  ├── DIM_IN_INTERNET
  ├── DIM_IN_ENERGIA_INEXISTENTE
  ├── DIM_IN_ESGOTO_INEXISTENTE
  └── FACT_CENSO_ESCOLAR
        │
        ▼
  Metabase (Docker)
  (BI dashboards & visualizations)
```

---

## ⚙️ Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.8+ | |
| Java (JDK) | 17 or 21 | Required by PySpark 4.x |
| Docker Desktop | Latest | Must be running before `docker compose up` |

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/adrianajfry04/spark-etl.git
cd spark-etl
```

### 2. Set up Python environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Set JAVA_HOME

```bash
# Windows (example path)
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"

# macOS/Linux
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
```

### 4. Start Docker containers

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 16** on port `5432` (user: `censo`, password: `123`, db: `censo_escolar`)
- **Metabase** on port `3000`

### 5. Create the Metabase internal database

```bash
docker exec -it spark-etl-postgres-1 psql -U censo -d censo_escolar -c "CREATE DATABASE metabase;"
```

### 6. Download the census data

```bash
python extract.py          # downloads 2017 data by default
python extract.py 2018     # or specify a year
```

> This downloads ~500MB from INEP, extracts `ESCOLAS.CSV` into `data/`, and removes the ZIP.

### 7. Run the ETL pipeline

```bash
python transform_load.py
```

This will:
- Read `ESCOLAS.CSV` with PySpark (~282,000 rows, 166 columns)
- Build 9 dimension tables in PostgreSQL
- Build and populate `FACT_CENSO_ESCOLAR` with FK references

Expected runtime: ~5–10 minutes depending on hardware.

### 8. Open Metabase

Visit `http://localhost:3000` (wait 3–5 minutes after containers start for first launch).

Connect to your database using:
- **Host:** `postgres`
- **Port:** `5432`
- **Database:** `censo_escolar`
- **Username:** `censo`
- **Password:** `123`

---

## 🗄️ Star Schema

### Dimension Tables

| Table | Key Column | Description |
|-------|-----------|-------------|
| `DIM_LOCAL` | `CO_UF`, `CO_MUNICIPIO`, `CO_REGIAO` | Geographic location |
| `DIM_TP_DEPENDENCIA` | `TP_DEPENDENCIA` | Admin type (Federal/State/Municipal/Private) |
| `DIM_TP_LOCALIZACAO` | `TP_LOCALIZACAO` | Location type (Urban/Rural) |
| `DIM_IN_BIBLIOTECA` | `IN_BIBLIOTECA` | Has library (0/1) |
| `DIM_IN_REFEITORIO` | `IN_REFEITORIO` | Has cafeteria (0/1) |
| `DIM_IN_COMPUTADOR` | `IN_COMPUTADOR` | Has computers (0/1) |
| `DIM_IN_INTERNET` | `IN_INTERNET` | Has internet (0/1) |
| `DIM_IN_ENERGIA_INEXISTENTE` | `IN_ENERGIA_INEXISTENTE` | No electricity (0/1) |
| `DIM_IN_ESGOTO_INEXISTENTE` | `IN_ESGOTO_INEXISTENTE` | No sewage (0/1) |

### Fact Table

`FACT_CENSO_ESCOLAR` — one row per school per census year, containing:
- `NU_ANO_CENSO` — census year
- `NU_SALAS_EXISTENTES` — total classrooms
- `NU_SALAS_UTILIZADAS` — classrooms in use
- `NU_COMPUTADOR` — number of computers
- `NU_COMP_ADMINISTRATIVO` — admin computers
- `NU_COMP_ALUNO` — student computers
- `NU_FUNCIONARIOS` — number of staff
- Foreign keys to all dimension tables

---

## 📦 Python Dependencies

```
pyspark
psycopg2-binary
requests
urllib3
```

---

## ⚠️ Known Issues & Notes

- The venv included in the repo was built on Windows (Python 3.14). **Recreate it on your machine** with `python -m venv venv && pip install -r requirements.txt`
- The JDBC jar (`postgresql-42.7.11.jar`) must be in the project root — Spark uses it to connect to PostgreSQL
- Spark prints harmless warnings about `NativeCodeLoader` and temp file cleanup on Windows — these can be ignored
- The CSV delimiter is `|` (pipe), not `,` — this is already configured in `transform_load.py`
- Census data schema varies by year — the column config in `transform_load.py` is tuned for the 2017 dataset

---

## 📚 References

- [Medium Article — Creating a Simple ETL Pipeline With Apache Spark](https://joaopedro214.medium.com/creating-a-simple-etl-pipeline-with-apache-spark-825cc17c8cf6)
- [INEP Open Data Portal](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)

---

## 📄 License

MIT License — feel free to use and adapt for your own projects.
