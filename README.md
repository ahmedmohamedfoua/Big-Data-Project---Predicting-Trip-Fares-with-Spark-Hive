# my-big-data-project-2025



## Predicting Ride-Sharing Trip Fares with Spark & Hive

This repository implements a CRISP-DM–based workflow for predicting the total fare amount (total_amount) 
of High‑Volume For‑Hire Vehicle (FHV) trips in New York City using the 2024 TLC dataset. 
It covers data ingestion (Sqoop → PostgreSQL → HDFS/Hive), ETL for exploratory analysis, Spark ML preprocessing, 
supervised modeling (Linear Regression & GBT), and end-to-end deployment planning.

## Repository Structure

├── data/                   # Local directories for train/test CSV (output)

├── hive/                   # HQL scripts for data quality checks & cleaning

├── sql/                    # SQL scripts for table creation & validation

├── scripts/                # Shell scripts for ETL / Sqoop / Spark jobs

├── output/                 # Evaluation & prediction results (CSV)

├── ml.ipynb                # Jupyter notebook: full ML pipeline, hyperparameter tuning

├── dim_base.avsc           # Avro schema for dim_base table

├── dim_base.java           # Java class for building dim_base entries

├── README.md               # This file

├── resize_parquet.py       # Utility: downsample or repartition Parquet data

└── .gitignore

## Getting Started

```
cd existing_repo
git remote add origin https://github.com/VladimirZelenokor1/Big-Data-Project---Predicting-Trip-Fares-with-Spark-Hive
git branch -M main
git push -uf origin main
```

# Prerequisites

- [ ] Hadoop cluster with HDFS & Hive
- [ ] Sqoop & PostgreSQL (for staging)
- [ ] Apache Spark (3.x) with MLlib
- [ ] Python 3.8+ (PySpark, pandas)

# ML Preprocessing & Modeling

Open ml.ipynb and execute cells to:

- [ ] Load Hive table into Spark DataFrame.

- [ ] Apply ML Pipeline:

- Impute missing values

- Cyclical encoding of time features

- StringIndex + OneHot

- VectorAssembler + StandardScaler

- [ ] Split data, train & tune Linear Regression and GBT:

- 3-fold CV, RMSE optimization

- Save best models under project/models/

- [ ] Evaluate on hold-out test set, output CSV to output/

# Results & Evaluation

Model comparison: output/model_comparison_reg.csv

Predictions: output/model1_predictions.csv, output/model2_predictions.csv

# Report

Full project report (LaTeX) follows CRISP-DM structure

# Limitations & Future Work

- Resource constraints prevented full model training in-cluster — see ml.ipynb for code.

- Future: incorporate real-time traffic, dynamic surge multipliers, live deployment.
