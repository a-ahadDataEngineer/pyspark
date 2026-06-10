# ⚡ PySpark Complete Learning Guide

<div align="center">

```
██████╗ ██╗   ██╗███████╗██████╗  █████╗ ██████╗ ██╗  ██╗
██╔══██╗╚██╗ ██╔╝██╔════╝██╔══██╗██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝ ╚████╔╝ ███████╗██████╔╝███████║██████╔╝█████╔╝ 
██╔═══╝   ╚██╔╝  ╚════██║██╔═══╝ ██╔══██║██╔══██╗██╔═██╗ 
██║        ██║   ███████║██║     ██║  ██║██║  ██║██║  ██╗
╚═╝        ╚═╝   ╚══════╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝
```

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=black)

**A complete, structured guide to mastering Apache Spark & PySpark — from architecture to advanced transformations.**

[![Duration](https://img.shields.io/badge/⏱%20Duration-5h%2046m-orange?style=flat-square)]()
[![Level](https://img.shields.io/badge/📊%20Level-Beginner%20→%20Advanced-brightgreen?style=flat-square)]()
[![Platform](https://img.shields.io/badge/🔧%20Platform-Databricks-red?style=flat-square)]()

</div>

---

## 📌 Table of Contents

| # | Topic | Timestamp | Level |
|---|-------|-----------|-------|
| 01 | [🎬 Introduction](#-introduction) | `0:00` | 🟢 Beginner |
| 02 | [🔥 What is Apache Spark?](#-what-is-apache-spark) | `3:47` | 🟢 Beginner |
| 03 | [🏗️ Apache Spark Architecture](#-apache-spark-architecture) | `5:33` | 🟢 Beginner |
| 04 | [⏳ Lazy Evaluation in Apache Spark](#-lazy-evaluation-in-apache-spark) | `10:47` | 🟡 Intermediate |
| 05 | [🔄 Spark Jobs, Stages, and Tasks](#-spark-jobs-stages-and-tasks) | `12:51` | 🟡 Intermediate |
| 06 | [🆓 Databricks Free Account](#-databricks-free-account) | `14:46` | 🟢 Beginner |
| 07 | [🖥️ Databricks Overview](#-databricks-overview) | `16:28` | 🟢 Beginner |
| 08 | [📥 Data Ingestion](#-data-ingestion) | `20:03` | 🟢 Beginner |
| 09 | [📓 Databricks Notebook Overview](#-databricks-notebook-overview) | `21:46` | 🟢 Beginner |
| 10 | [⚙️ Spark Cluster](#-spark-cluster) | `22:46` | 🟡 Intermediate |
| 11 | [📖 Data Reading with PySpark](#-data-reading-with-pyspark) | `23:36` | 🟢 Beginner |
| 12 | [🔌 Spark Data Reader API](#-spark-data-reader-api) | `27:46` | 🟡 Intermediate |
| 13 | [🗺️ Spark DAG](#-spark-dag) | `34:00` | 🟡 Intermediate |
| 14 | [🧱 StructType and DDL Schema](#-structtype-and-ddl-schema) | `43:00` | 🟡 Intermediate |
| 15 | [🔀 Data Transformation — Beginner](#-data-transformation-with-pyspark-for-beginners) | `56:00` | 🟢 Beginner |
| 16 | [⚡ Intermediate Transformations](#-pyspark-intermediate-level-transformations) | `1:53:37` | 🟡 Intermediate |
| 17 | [🚀 Advanced Level Functions](#-pyspark-advanced-level-functions) | `3:21:51` | 🔴 Advanced |
| 18 | [🪟 Window Functions](#-window-functions-in-pyspark) | `4:22:09` | 🔴 Advanced |
| 19 | [🛠️ User Defined Functions (UDFs)](#-user-defined-functions-in-pyspark) | `4:52:20` | 🔴 Advanced |
| 20 | [✍️ Data Writing with PySpark](#-data-writing-with-pyspark) | `5:02:10` | 🟡 Intermediate |
| 21 | [📝 Data Writing Modes](#-data-writing-modes-in-pyspark) | `5:09:59` | 🟡 Intermediate |
| 22 | [🗃️ Parquet File Format](#-parquet-file-format) | `5:23:33` | 🟡 Intermediate |
| 23 | [🏛️ Managed vs External Tables](#-managed-vs-external-tables-in-spark) | `5:35:34` | 🔴 Advanced |
| 24 | [🗄️ SparkSQL](#-sparksql) | `5:46:49` | 🟡 Intermediate |

---

## 🎬 Introduction
> **`⏱ 0:00`**

Welcome to the complete PySpark learning journey. This course takes you from **zero to hero** covering everything from Apache Spark fundamentals to production-ready data engineering patterns. Whether you're a data analyst stepping into big data or a software engineer exploring distributed computing — this guide is your roadmap.

**What you'll build:**
- ✅ Real-world data pipelines
- ✅ Scalable transformation logic
- ✅ Optimized Spark jobs on Databricks
- ✅ SQL-powered analytics with SparkSQL

---

## 🔥 What is Apache Spark?
> **`⏱ 3:47`**

Apache Spark is an **open-source, distributed computing engine** designed for large-scale data processing. Originally developed at UC Berkeley's AMPLab, it has become the industry standard for big data analytics.

```
📦 Apache Spark Ecosystem
├── 🔵 Spark Core        → Base engine, RDDs, scheduling
├── 🟣 Spark SQL         → Structured data + DataFrames
├── 🟡 Spark Streaming   → Real-time data pipelines
├── 🟢 MLlib             → Machine learning at scale
└── 🔴 GraphX            → Graph computation
```

**Key advantages over Hadoop MapReduce:**
- ⚡ **100x faster** in-memory processing
- 🔄 Supports batch, streaming, ML, and graph workloads
- 🐍 Native APIs in Python (PySpark), Scala, Java, R
- 🔗 Integrates with HDFS, S3, Delta Lake, Kafka, and more

---

## 🏗️ Apache Spark Architecture
> **`⏱ 5:33`**

```
┌─────────────────────────────────────────────────────────┐
│                    SPARK APPLICATION                     │
│                                                         │
│   ┌─────────────┐         ┌──────────────────────────┐  │
│   │   DRIVER    │◄───────►│     CLUSTER MANAGER      │  │
│   │  (Master)   │         │  (YARN / Mesos / K8s /   │  │
│   │             │         │   Spark Standalone)       │  │
│   │ SparkContext│         └──────────────────────────┘  │
│   └──────┬──────┘                    │                  │
│          │                           │                  │
│          ▼                           ▼                  │
│   ┌──────────────────────────────────────────────────┐  │
│   │                    EXECUTORS                     │  │
│   │  ┌─────────┐   ┌─────────┐   ┌─────────┐        │  │
│   │  │ Worker 1│   │ Worker 2│   │ Worker 3│  ...   │  │
│   │  │  Tasks  │   │  Tasks  │   │  Tasks  │        │  │
│   │  │  Cache  │   │  Cache  │   │  Cache  │        │  │
│   │  └─────────┘   └─────────┘   └─────────┘        │  │
│   └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

| Component | Role |
|-----------|------|
| **Driver** | Orchestrates the application, holds SparkContext |
| **Cluster Manager** | Allocates resources across the cluster |
| **Executor** | Runs tasks, stores cached data |
| **Task** | Smallest unit of work, runs on a partition |

---

## ⏳ Lazy Evaluation in Apache Spark
> **`⏱ 10:47`**

Spark uses **lazy evaluation** — transformations are **not executed** until an action is called. This enables Spark to optimize the entire execution plan before running.

```python
# ⚙️ These are TRANSFORMATIONS (lazy — nothing runs yet)
df = spark.read.csv("data.csv")        # lazy
df_filtered = df.filter(df.age > 25)   # lazy
df_selected = df_filtered.select("name", "age")  # lazy

# 🚀 This is an ACTION (triggers execution)
df_selected.show()   # ← Only NOW does Spark execute everything
```

```
TRANSFORMATION CHAIN:
  Read → Filter → Select → GroupBy → Agg
              ↓
         (Optimized by Catalyst Optimizer)
              ↓
         Single Execution Plan 🚀
```

> 💡 **Why it matters:** Spark builds a logical plan and optimizes it before any data moves. This avoids unnecessary computation and I/O.

---

## 🔄 Spark Jobs, Stages, and Tasks
> **`⏱ 12:51`**

```
ACTION TRIGGERED
      │
      ▼
  ┌───────┐
  │  JOB  │  ← One job per Action (show, count, write)
  └───┬───┘
      │
      ▼
  ┌─────────┐   ┌─────────┐
  │ STAGE 1 │──►│ STAGE 2 │  ← Stages split at shuffle boundaries
  └────┬────┘   └────┬────┘
       │              │
       ▼              ▼
  [Task][Task]   [Task][Task][Task]
  ← One task per partition →
```

| Unit | Description |
|------|-------------|
| **Job** | Triggered by an action; top-level unit |
| **Stage** | Group of tasks with no shuffle between them |
| **Task** | Executes on a single data partition |
| **Shuffle** | Data redistribution across partitions (expensive!) |

---

## 🆓 Databricks Free Account
> **`⏱ 14:46`**

Databricks offers a **free Community Edition** — perfect for learning PySpark without infrastructure setup.

```
📋 Setup Steps:
  1. Visit → https://community.cloud.databricks.com
  2. Sign up with your email
  3. Select "Community Edition" (free tier)
  4. Create your first cluster
  5. Start a notebook & write PySpark! ✅
```

**Community Edition includes:**
- 🆓 Free single-node cluster
- 📓 Collaborative notebooks
- 🗂️ DBFS (Databricks File System) storage
- 🔌 Pre-installed Spark runtime

---

## 🖥️ Databricks Overview
> **`⏱ 16:28`**

Databricks is a **unified data analytics platform** built on Apache Spark. It simplifies big data engineering, data science, and machine learning workflows.

```
Databricks Platform
├── 📁 Workspace        → Organize notebooks, folders, repos
├── ⚙️ Compute          → Clusters, jobs, serverless SQL
├── 📊 Delta Lake       → ACID transactions on data lakes
├── 🤖 MLflow           → ML experiment tracking
├── 🔗 Unity Catalog    → Unified data governance
└── 📈 SQL Analytics    → BI-friendly SQL warehouse
```

---

## 📥 Data Ingestion
> **`⏱ 20:03`**

Data ingestion is the process of **loading raw data** into your Spark environment for processing.

```python
# Upload via Databricks UI → DBFS
# Then read using PySpark:

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/data/sample.csv")
```

**Common ingestion sources:**
| Source | Format |
|--------|--------|
| Local Files | CSV, JSON, Parquet, ORC |
| Cloud Storage | AWS S3, Azure ADLS, GCS |
| Databases | JDBC (PostgreSQL, MySQL) |
| Streaming | Kafka, Event Hub |
| Delta Lake | Delta format (`.delta`) |

---

## 📓 Databricks Notebook Overview
> **`⏱ 21:46`**

Databricks Notebooks support **multi-language cells** in a single notebook.

```python
# 🐍 Python cell
df.show()

# %sql — SQL cell
# SELECT * FROM my_table LIMIT 10;

# %scala — Scala cell
# val rdd = sc.parallelize(1 to 100)

# %md — Markdown cell
# ## This is a heading

# %sh — Shell cell
# ls /dbfs/FileStore/
```

> 💡 Switch languages per cell using **magic commands** (`%python`, `%sql`, `%scala`, `%r`, `%sh`, `%md`)

---

## ⚙️ Spark Cluster
> **`⏱ 22:46`**

A Spark cluster is the **distributed computing environment** that executes your Spark jobs.

```
Cluster Configuration (Databricks):
├── 🔧 Runtime Version  → Spark 3.x + Python 3.x
├── 🖥️ Driver Type      → Memory-optimized / Compute
├── 👷 Worker Nodes     → Scale from 2 to N workers
├── ⚡ Auto-scaling     → Dynamic resource allocation
└── 📅 Auto-terminate   → Saves cost when idle
```

**Cluster Types:**
- **All-Purpose Cluster** → Interactive development
- **Job Cluster** → Automated pipeline runs
- **SQL Warehouse** → Optimized for SQL queries

---

## 📖 Data Reading with PySpark
> **`⏱ 23:36`**

```python
# ─── CSV ───────────────────────────────────────────────
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("path/to/file.csv")

# ─── JSON ──────────────────────────────────────────────
df_json = spark.read.json("path/to/file.json")

# ─── Parquet ───────────────────────────────────────────
df_parquet = spark.read.parquet("path/to/file.parquet")

# ─── Multiple Files ────────────────────────────────────
df_multi = spark.read.csv("path/to/folder/*.csv")
```

---

## 🔌 Spark Data Reader API
> **`⏱ 27:46`**

The unified **DataFrameReader API** provides a fluent interface for reading all data sources.

```python
# Full API syntax
df = spark.read \
    .format("csv")                           # format
    .schema(my_schema)                       # explicit schema
    .option("header", "true")               # format options
    .option("nullValue", "NA")              #
    .option("dateFormat", "yyyy-MM-dd")     #
    .option("mode", "PERMISSIVE")           # PERMISSIVE / DROPMALFORMED / FAILFAST
    .load("dbfs:/path/to/data/")            # path

# Shorthand methods
spark.read.csv("path")
spark.read.json("path")
spark.read.parquet("path")
spark.read.orc("path")
spark.read.text("path")
```

**Read Modes:**
| Mode | Behavior |
|------|----------|
| `PERMISSIVE` | Nullifies bad records (default) |
| `DROPMALFORMED` | Silently drops bad rows |
| `FAILFAST` | Throws exception on bad data |

---

## 🗺️ Spark DAG
> **`⏱ 34:00`**

The **Directed Acyclic Graph (DAG)** is Spark's execution blueprint — a visual representation of all transformations and how data flows between them.

```
DAG Visualization:

 [CSV Read]
     │
     ▼
 [Filter: age > 25]
     │
     ▼
 [Select: name, dept, salary]
     │
     ▼
 [GroupBy: dept]
     │
     ▼
 [Aggregate: avg(salary)]
     │
     ▼
 [Sort: desc]
     │
     ▼
 [Write: Parquet] ← ACTION triggers DAG execution
```

> 💡 View the DAG in **Spark UI → Jobs → DAG Visualization** after running an action.

**Catalyst Optimizer Pipeline:**
```
Unresolved Plan → Logical Plan → Optimized Plan → Physical Plan → RDDs
```

---

## 🧱 StructType and DDL Schema
> **`⏱ 43:00`**

Explicitly defining a schema prevents Spark from scanning data to infer types — making reads **faster and more reliable**.

```python
from pyspark.sql.types import *

# ─── StructType (Programmatic) ─────────────────────────
schema = StructType([
    StructField("id",         IntegerType(),  nullable=False),
    StructField("name",       StringType(),   nullable=True),
    StructField("dob",        DateType(),     nullable=True),
    StructField("salary",     DoubleType(),   nullable=True),
    StructField("is_active",  BooleanType(),  nullable=True),
])

# ─── DDL Schema (String-based) ─────────────────────────
ddl_schema = "id INT, name STRING, dob DATE, salary DOUBLE, is_active BOOLEAN"

# ─── Apply Schema ──────────────────────────────────────
df = spark.read.schema(schema).csv("data.csv", header=True)
df = spark.read.schema(ddl_schema).csv("data.csv", header=True)
```

---

## 🔀 Data Transformation with PySpark (For Beginners)
> **`⏱ 56:00`**

Core DataFrame transformations every PySpark developer must know:

```python
from pyspark.sql.functions import *

# ─── Select Columns ────────────────────────────────────
df.select("name", "age", "salary")
df.select(col("name"), col("age") * 1.1)

# ─── Filter / Where ────────────────────────────────────
df.filter(col("age") > 25)
df.where((col("dept") == "IT") & (col("salary") > 50000))

# ─── Add / Rename / Drop Columns ───────────────────────
df.withColumn("tax", col("salary") * 0.2)
df.withColumnRenamed("salary", "annual_salary")
df.drop("unwanted_col")

# ─── Sort / Order ──────────────────────────────────────
df.sort("salary")
df.orderBy(col("salary").desc(), col("name").asc())

# ─── Distinct & Limit ──────────────────────────────────
df.distinct()
df.limit(100)

# ─── GroupBy + Aggregation ─────────────────────────────
df.groupBy("dept").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    sum("salary").alias("total_payroll")
)
```

---

## ⚡ PySpark Intermediate Level Transformations
> **`⏱ 1:53:37`**

```python
# ─── Joins ─────────────────────────────────────────────
df_emp.join(df_dept, df_emp.dept_id == df_dept.id, "inner")
df_emp.join(df_dept, "dept_id", "left")
df_emp.join(df_dept, "dept_id", "right")
df_emp.join(df_dept, "dept_id", "full")
df_emp.join(df_dept, "dept_id", "left_anti")   # rows NOT in right
df_emp.join(df_dept, "dept_id", "left_semi")   # rows in both, left cols only

# ─── Union / Union By Name ─────────────────────────────
df1.union(df2)                    # same column order
df1.unionByName(df2)              # match by column name

# ─── String Functions ──────────────────────────────────
df.select(
    upper(col("name")),
    lower(col("email")),
    trim(col("code")),
    substring(col("name"), 1, 3),
    concat(col("first"), lit(" "), col("last")),
    length(col("description")),
    regexp_replace(col("phone"), "[^0-9]", "")
)

# ─── Date Functions ────────────────────────────────────
df.select(
    current_date(),
    current_timestamp(),
    to_date(col("date_str"), "yyyy-MM-dd"),
    date_add(col("start_date"), 30),
    datediff(col("end_date"), col("start_date")),
    year(col("dob")), month(col("dob")), dayofmonth(col("dob"))
)

# ─── Null Handling ─────────────────────────────────────
df.na.drop()                        # drop rows with any null
df.na.drop(subset=["salary"])       # drop if salary is null
df.na.fill({"salary": 0, "city": "Unknown"})
df.na.replace(["N/A", ""], None)
```

---

## 🚀 PySpark Advanced Level Functions
> **`⏱ 3:21:51`**

```python
# ─── Higher-Order Functions (Arrays & Maps) ────────────
from pyspark.sql.functions import *

# Transform array elements
df.select(transform(col("scores"), lambda x: x * 2))

# Filter array elements
df.select(filter(col("tags"), lambda x: x != "inactive"))

# Aggregate over array
df.select(aggregate(col("nums"), lit(0), lambda acc, x: acc + x))

# ─── Explode ───────────────────────────────────────────
df.select("id", explode(col("hobbies")).alias("hobby"))
df.select("id", posexplode(col("items")))    # with position index
df.select("id", explode_outer(col("tags"))) # keeps rows with null arrays

# ─── Pivot ─────────────────────────────────────────────
df.groupBy("dept").pivot("year", [2021, 2022, 2023]).agg(sum("revenue"))

# ─── Conditional Logic ─────────────────────────────────
df.withColumn("grade",
    when(col("score") >= 90, "A")
    .when(col("score") >= 80, "B")
    .when(col("score") >= 70, "C")
    .otherwise("F")
)

# ─── Broadcast Join (Performance Optimization) ─────────
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")  # ships small DF to each executor
```

---

## 🪟 Window Functions in PySpark
> **`⏱ 4:22:09`**

Window functions perform calculations **across a set of rows related to the current row** — without collapsing the DataFrame.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# ─── Define Window Spec ────────────────────────────────
window_dept = Window.partitionBy("dept").orderBy(col("salary").desc())
window_running = Window.partitionBy("dept").orderBy("hire_date") \
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# ─── Ranking Functions ─────────────────────────────────
df.withColumn("rank",        rank().over(window_dept))
df.withColumn("dense_rank",  dense_rank().over(window_dept))
df.withColumn("row_number",  row_number().over(window_dept))
df.withColumn("percent_rank", percent_rank().over(window_dept))
df.withColumn("ntile_4",     ntile(4).over(window_dept))

# ─── Analytic Functions ────────────────────────────────
df.withColumn("prev_salary", lag(col("salary"), 1).over(window_dept))
df.withColumn("next_salary", lead(col("salary"), 1).over(window_dept))
df.withColumn("first_salary", first("salary").over(window_dept))
df.withColumn("last_salary",  last("salary").over(window_dept))

# ─── Aggregate Window Functions ────────────────────────
df.withColumn("running_total", sum("salary").over(window_running))
df.withColumn("dept_avg",      avg("salary").over(Window.partitionBy("dept")))
df.withColumn("dept_max",      max("salary").over(Window.partitionBy("dept")))
```

---

## 🛠️ User Defined Functions in PySpark
> **`⏱ 4:52:20`**

UDFs let you apply **custom Python logic** to DataFrame columns.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# ─── Standard UDF ──────────────────────────────────────
def clean_name(name):
    return name.strip().title() if name else None

clean_name_udf = udf(clean_name, StringType())
df.withColumn("clean_name", clean_name_udf(col("name")))

# ─── Decorator Syntax ──────────────────────────────────
@udf(returnType=IntegerType())
def age_bucket(age):
    if age < 25:   return 1
    elif age < 40: return 2
    elif age < 60: return 3
    else:          return 4

df.withColumn("bucket", age_bucket(col("age")))

# ─── Pandas UDF (Vectorized — Much Faster!) ────────────
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def upper_pandas(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.withColumn("upper_name", upper_pandas(col("name")))
```

> ⚠️ **Performance Note:** Standard UDFs serialize row-by-row (slow). Prefer **Pandas UDFs** (vectorized via Apache Arrow) whenever possible.

---

## ✍️ Data Writing with PySpark
> **`⏱ 5:02:10`**

```python
# ─── DataFrameWriter API ───────────────────────────────
df.write \
  .format("parquet")
  .mode("overwrite")
  .option("compression", "snappy")
  .partitionBy("year", "month")
  .save("dbfs:/output/path/")

# ─── Shorthand Writers ─────────────────────────────────
df.write.csv("path/output.csv", header=True)
df.write.json("path/output/")
df.write.parquet("path/output/")
df.write.orc("path/output/")

# ─── Write to Delta Lake ───────────────────────────────
df.write.format("delta").save("dbfs:/delta/my_table")

# ─── Write to Table ────────────────────────────────────
df.write.saveAsTable("my_database.my_table")
```

---

## 📝 Data Writing Modes in PySpark
> **`⏱ 5:09:59`**

```python
# ─── Write Modes ───────────────────────────────────────

# OVERWRITE — Replace existing data entirely
df.write.mode("overwrite").parquet("path/")

# APPEND — Add data to existing dataset
df.write.mode("append").parquet("path/")

# ERROR / ERRORIFEXISTS — Fail if data exists (default)
df.write.mode("error").parquet("path/")

# IGNORE — Skip write if data already exists
df.write.mode("ignore").parquet("path/")
```

| Mode | Behavior |
|------|----------|
| `overwrite` | ⚠️ Replaces all existing data |
| `append` | ➕ Adds to existing data |
| `error` | 🚫 Fails if output exists (default) |
| `ignore` | ✅ Skips silently if output exists |

---

## 🗃️ Parquet File Format
> **`⏱ 5:23:33`**

Parquet is a **columnar storage format** — the gold standard for Spark workloads.

```
ROW-BASED (CSV/JSON):                COLUMNAR (Parquet):
┌──────────────────────┐             ┌────┬──────┬────────┐
│ id │ name  │ salary  │             │ ID │ NAME │ SALARY │
├────┼───────┼─────────┤             ├────┼──────┼────────┤
│ 1  │ Alice │ 90000   │             │ 1  │Alice │ 90000  │
│ 2  │ Bob   │ 85000   │             │ 2  │ Bob  │ 85000  │
│ 3  │ Carol │ 92000   │             │ 3  │Carol │ 92000  │
└──────────────────────┘             └────┴──────┴────────┘
  → Must read ALL columns            → Read ONLY needed columns
```

**Why Parquet?**
| Feature | Benefit |
|---------|---------|
| Columnar storage | Read only needed columns — faster queries |
| Built-in compression | Snappy, GZIP, LZ4, ZSTD support |
| Schema embedded | Self-describing — no separate schema file |
| Predicate pushdown | Filter data before loading into memory |
| Splittable | Enables parallel reads across nodes |

```python
# Write Parquet
df.write.parquet("path/", compression="snappy")

# Read Parquet
df = spark.read.parquet("path/")

# Partition on write (massive read performance boost)
df.write.partitionBy("country", "year").parquet("path/")
```

---

## 🏛️ Managed vs External Tables in Spark
> **`⏱ 5:35:34`**

```
                    MANAGED TABLE              EXTERNAL TABLE
                    ─────────────────          ──────────────────
  Data Location:    Spark controls             You control (S3/ADLS/HDFS)
  DROP TABLE:       Deletes data too ⚠️        Only drops metadata ✅
  Schema:           Spark manages              Defined manually
  Best For:         Internal pipelines         Shared data / existing stores
```

```python
# ─── Create Managed Table ──────────────────────────────
df.write.saveAsTable("my_db.employees")

# ─── Create External Table ─────────────────────────────
df.write.option("path", "dbfs:/external/employees/") \
        .saveAsTable("my_db.employees_ext")

# ─── Create via SparkSQL ───────────────────────────────
spark.sql("""
  CREATE TABLE employees (
    id INT, name STRING, salary DOUBLE
  )
  USING PARQUET
  LOCATION 'dbfs:/external/employees/'
""")
```

---

## 🗄️ SparkSQL
> **`⏱ 5:46:49`**

SparkSQL allows you to run **standard SQL queries** directly on DataFrames and tables.

```python
# ─── Register Temp View ────────────────────────────────
df.createOrReplaceTempView("employees")
df.createOrReplaceGlobalTempView("employees_global")  # cross-session

# ─── Run SQL Queries ───────────────────────────────────
result = spark.sql("""
    SELECT
        dept,
        COUNT(*) AS headcount,
        ROUND(AVG(salary), 2) AS avg_salary,
        MAX(salary) AS top_salary
    FROM employees
    WHERE hire_date >= '2020-01-01'
    GROUP BY dept
    HAVING COUNT(*) > 5
    ORDER BY avg_salary DESC
""")

# ─── Interop: SQL ↔ DataFrame API ─────────────────────
# SQL result is just a DataFrame
result.withColumn("bonus", col("avg_salary") * 0.1).show()

# ─── DDL via SparkSQL ──────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("USE analytics")
spark.sql("SHOW TABLES")
spark.sql("DESCRIBE TABLE employees")
spark.sql("DROP TABLE IF EXISTS temp_results")
```

---

## 🎓 Learning Path Summary

```
BEGINNER ──────────────────────────────────────────► ADVANCED

  What is Spark?                              Window Functions
  Spark Architecture              Advanced Transformations
  Databricks Setup             PySpark Joins & Aggregations
  Data Reading        Intermediate Transformations
  Basic Transforms    Spark DAG & Optimization
  Schema Definition
```

---

## 📚 Quick Reference Cheatsheet

```python
# ─── SparkSession ──────────────────────────────────────
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# ─── Read ──────────────────────────────────────────────
df = spark.read.csv("path", header=True, inferSchema=True)

# ─── Inspect ───────────────────────────────────────────
df.show(5)             # display first 5 rows
df.printSchema()       # column names + types
df.describe().show()   # statistics
df.count()             # row count
df.columns             # list of column names
df.dtypes              # list of (column, type) tuples

# ─── Core Imports ──────────────────────────────────────
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

---

<div align="center">

**Built with ❤️ for the PySpark community**

![Made with PySpark](https://img.shields.io/badge/Made%20with-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Powered%20by-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)

*Happy Sparking! ⚡*

</div>