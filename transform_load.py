"""
transform_load.py — Full Star Schema ETL for Censo Escolar.

Pipeline:
    1. Read  ESCOLAS.CSV  with PySpark
    2. Build dimension tables → write to PostgreSQL
    3. Build FACT_CENSO_ESCOLAR with FK references → write to PostgreSQL

Prerequisites:
    • Docker containers running:  docker compose up -d
    • Java 17 or 21 installed and JAVA_HOME pointing to it
    • postgresql-42.7.11.jar present in the same folder as this script
    • data/ESCOLAS.CSV present  (run extract.py first if needed)

Usage:
    python transform_load.py
"""

import os
import sys
from datetime import datetime
from itertools import chain

import psycopg2
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JDBC_JAR   = os.path.join(SCRIPT_DIR, "postgresql-42.7.11.jar")
DATA_PATH  = os.path.join(SCRIPT_DIR, "data", "ESCOLAS.CSV")

# ── PostgreSQL connection ─────────────────────────────────────────────────────
POSTGRES_HOST     = "localhost"
POSTGRES_PORT     = "5432"
POSTGRES_USER     = "censo"
POSTGRES_PASSWORD = "123"
POSTGRES_DB       = "censo_escolar"

POSTGRES_JDBC_URL   = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Dimension configuration ───────────────────────────────────────────────────
INTEGER_DIMENSIONS = [
    "TP_DEPENDENCIA",
    "TP_LOCALIZACAO",
    "IN_ENERGIA_INEXISTENTE",
    "IN_ESGOTO_INEXISTENTE",
    "IN_BIBLIOTECA",
    "IN_REFEITORIO",
    "IN_COMPUTADOR",
    "IN_INTERNET",
]

FACT_COLUMNS = [
    "NU_ANO_CENSO",
    "NU_SALAS_EXISTENTES",
    "NU_SALAS_UTILIZADAS",
    "NU_COMPUTADOR",
    "NU_COMP_ADMINISTRATIVO",
    "NU_COMP_ALUNO",
    "NU_FUNCIONARIOS",
]

DIMENSION_TABLES_CONFIG = {
    "DIM_LOCAL": {
        "fields": [
            {"field": "CO_UF",        "type": "integer"},
            {"field": "CO_MUNICIPIO", "type": "integer"},
            {"field": "CO_REGIAO",    "type": "integer"},
        ]
    }
}

DIMENSION_TABLES_CONFIG.update({
    f"DIM_{dim}": {
        "fields": [{"field": dim, "type": "integer"}]
    }
    for dim in INTEGER_DIMENSIONS
})

# ── Fact table configuration ──────────────────────────────────────────────────
FACT_TABLE_NAME = "FACT_CENSO_ESCOLAR"

DIMENSION_ID_CONFIG = {
    table_name: [f["field"] for f in cfg["fields"]]
    for table_name, cfg in DIMENSION_TABLES_CONFIG.items()
}

FACT_ALL_COLUMNS = FACT_COLUMNS + [f"ID_{t}" for t in DIMENSION_ID_CONFIG]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _check_java():
    """Validate that JAVA_HOME points to Java 17+ (required by Spark 4.x)."""
    java_home = os.environ.get("JAVA_HOME", "")
    if not java_home:
        print(
            "\n[ERROR] JAVA_HOME is not set.\n"
            "        Install Java 17 or 21 and set JAVA_HOME, for example:\n"
            "          Windows: setx JAVA_HOME \"C:\\Program Files\\Eclipse Adoptium\\jdk-21...\"\n"
            "          Linux:   export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64\n"
        )
        sys.exit(1)
    print(f"[✓] JAVA_HOME = {java_home}")


def _check_files():
    if not os.path.isfile(JDBC_JAR):
        sys.exit(f"[ERROR] JDBC jar not found: {JDBC_JAR}")
    if not os.path.isfile(DATA_PATH):
        sys.exit(
            f"[ERROR] Data file not found: {DATA_PATH}\n"
            "        Run  python extract.py  first to download the data."
        )
    print(f"[✓] JDBC jar  : {JDBC_JAR}")
    print(f"[✓] Data file : {DATA_PATH}  ({os.path.getsize(DATA_PATH) >> 20} MB)")


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CensoEscolarETL")
        .config("spark.jars", JDBC_JAR)
        .config("spark.sql.shuffle.partitions", "4")
        # Prevent driver memory issues on large CSVs
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv(spark: SparkSession):
    print(f"\n[{datetime.now():%H:%M:%S}] Reading CSV...")
    df = (
        spark.read
        .format("csv")
        .option("header",      "true")
        .option("inferSchema", "true")
        .option("delimiter",   "|")
        .option("encoding",    "latin1")
        .load(DATA_PATH)
    )
    print(f"[{datetime.now():%H:%M:%S}] Rows: {df.count():,}   Columns: {len(df.columns)}")
    return df


def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        )
        print(f"[✓] Connected to PostgreSQL  ({POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB})")
        return conn
    except psycopg2.OperationalError as e:
        sys.exit(
            f"\n[ERROR] Cannot connect to PostgreSQL.\n"
            f"        Make sure Docker containers are running:  docker compose up -d\n"
            f"        Details: {e}"
        )


# ── ETL Steps ─────────────────────────────────────────────────────────────────

def step_dimensions(data, conn):
    """Write all dimension tables to PostgreSQL."""
    print(f"\n{'─'*50}")
    print(f"  STEP 1 — Dimension tables ({len(DIMENSION_TABLES_CONFIG)} tables)")
    print(f"{'─'*50}")

    for table_name, cfg in DIMENSION_TABLES_CONFIG.items():
        print(f"\n  → {table_name}")
        (
            data
            .select([
                F.col(f["field"]).cast(f["type"]).alias(f["field"])
                for f in cfg["fields"]
            ])
            .distinct()
            .withColumn("id", F.monotonically_increasing_id())
            .write
            .jdbc(
                url=POSTGRES_JDBC_URL,
                table=table_name,
                mode="overwrite",
                properties=POSTGRES_JDBC_PROPS,
            )
        )
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id);")
        cur.close()
        conn.commit()
        print(f"     done ✓")


def step_create_fact_table(conn):
    """Create the fact table schema with FK constraints."""
    print(f"\n{'─'*50}")
    print(f"  STEP 2 — Create fact table schema")
    print(f"{'─'*50}")

    nl = ",\n            "
    facts_col_sql = nl.join(f"{col} INTEGER" for col in FACT_COLUMNS)
    fk_col_sql    = nl.join(f"ID_{dim} BIGINT" for dim in DIMENSION_ID_CONFIG)
    fk_sql        = nl.join(
        f"ADD CONSTRAINT {FACT_TABLE_NAME}_{dim}_fk "
        f"FOREIGN KEY (ID_{dim}) REFERENCES {dim}(id)"
        for dim in DIMENSION_ID_CONFIG
    )

    sql = f"""
        DROP TABLE IF EXISTS {FACT_TABLE_NAME} CASCADE;
        CREATE TABLE {FACT_TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            {facts_col_sql},
            {fk_col_sql}
        );
        ALTER TABLE {FACT_TABLE_NAME}
        {fk_sql};
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        print(f"  Table {FACT_TABLE_NAME} created ✓")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()


def step_write_facts(spark, data):
    """Join dimension IDs onto the fact data and write to PostgreSQL."""
    print(f"\n{'─'*50}")
    print(f"  STEP 3 — Write fact table")
    print(f"{'─'*50}")

    # Select only the columns we need
    needed_cols = list(chain(*DIMENSION_ID_CONFIG.values())) + FACT_COLUMNS
    facts_data  = data.select([
        F.col(c).cast("integer").alias(c) if c in FACT_COLUMNS else F.col(c)
        for c in needed_cols
        if c in data.columns
    ])

    # Resolve dimension IDs by joining each dimension table
    for table_name, fields in DIMENSION_ID_CONFIG.items():
        # Only join on columns that exist in the fact data
        join_fields = [f for f in fields if f in facts_data.columns]
        if not join_fields:
            facts_data = facts_data.withColumn(f"ID_{table_name}", F.lit(None).cast("long"))
            continue

        dim_df = (
            spark.read
            .jdbc(
                url=POSTGRES_JDBC_URL,
                table=table_name,
                properties=POSTGRES_JDBC_PROPS,
            )
            .withColumnRenamed("id", f"ID_{table_name}")
        )
        facts_data = (
            facts_data
            .join(dim_df, on=join_fields, how="left")
            .drop(*join_fields)
        )

    print(f"  Writing {FACT_TABLE_NAME} to PostgreSQL...")
    (
        facts_data
        .select(*FACT_ALL_COLUMNS)
        .write
        .jdbc(
            url=POSTGRES_JDBC_URL,
            table=FACT_TABLE_NAME,
            mode="append",
            properties=POSTGRES_JDBC_PROPS,
        )
    )
    print(f"  Fact table written ✓")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  Censo Escolar — Star Schema ETL")
    print("=" * 50)

    _check_java()
    _check_files()

    spark = create_spark_session()
    data  = read_csv(spark)
    conn  = get_pg_connection()

    try:
        step_dimensions(data, conn)
        step_create_fact_table(conn)
        step_write_facts(spark, data)
    finally:
        conn.close()
        spark.stop()

    print(f"\n{'='*50}")
    print("  ETL complete!")
    print("  Open Metabase at  http://localhost:3000")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()