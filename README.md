# Big Data Project: Predicting Trip Fares with Spark & Hive 🚖✨

![Big Data](https://img.shields.io/badge/Big--Data-Project-blue?style=for-the-badge&logo=apache-spark) ![Release](https://img.shields.io/badge/Release-v1.0-orange?style=for-the-badge) ![Python](https://img.shields.io/badge/Python-3.8-yellow?style=for-the-badge)

Welcome to the **Big Data Project** focused on predicting NYC ride-sharing trip fares. This project utilizes a comprehensive big data pipeline built on the CRISP-DM framework. It incorporates various technologies to ingest, process, and analyze data efficiently.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Technologies Used](#technologies-used)
3. [Data Ingestion](#data-ingestion)
4. [ETL Process](#etl-process)
5. [Model Training and Tuning](#model-training-and-tuning)
6. [Deployment](#deployment)
7. [Getting Started](#getting-started)
8. [Releases](#releases)
9. [Contributing](#contributing)
10. [License](#license)

## Project Overview

This project aims to predict the trip fares for NYC ride-sharing services using data from the NYC Taxi and Limousine Commission (TLC). The project leverages various big data technologies to create a robust pipeline. The workflow involves:

- Ingesting 2024 TLC data using Sqoop.
- Storing the data in HDFS and Hive.
- Performing ETL and feature engineering using Spark and PySpark.
- Training and tuning machine learning models, including Linear Regression and Gradient Boosted Trees.
- Outlining an end-to-end deployment strategy.

## Technologies Used

This project incorporates the following technologies:

- **Big Data**: Apache Hadoop, Hive
- **Data Engineering**: Sqoop, Spark, PySpark
- **Machine Learning**: Spark ML, Scikit-learn
- **Data Processing**: SQL
- **Notebooks**: Jupyter Notebook
- **Languages**: Python

## Data Ingestion

Data ingestion is the first step in our pipeline. We use Sqoop to import the 2024 TLC data into HDFS. This process ensures that we have a reliable source of data for our analysis.

### Steps for Data Ingestion

1. **Install Sqoop**: Ensure that Sqoop is installed on your Hadoop cluster.
2. **Connect to the Database**: Use the appropriate JDBC connection string to connect to the TLC database.
3. **Import Data**: Execute the Sqoop command to import data into HDFS.

```bash
sqoop import --connect jdbc:postgresql://your_database_url --username your_username --password your_password --table your_table_name --target-dir /user/hadoop/tlc_data
```

## ETL Process

The ETL (Extract, Transform, Load) process is crucial for preparing the data for analysis. We use Spark and PySpark to handle this process efficiently.

### Steps for ETL

1. **Extract**: Load data from HDFS into Spark DataFrames.
2. **Transform**: Clean and preprocess the data, including handling missing values and feature engineering.
3. **Load**: Store the transformed data back into Hive for further analysis.

### Sample ETL Code

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL Process").enableHiveSupport().getOrCreate()

# Load data
df = spark.read.csv("/user/hadoop/tlc_data/*.csv", header=True, inferSchema=True)

# Transform data
df_cleaned = df.dropna()
df_transformed = df_cleaned.withColumn("fare_amount", df_cleaned["fare_amount"].cast("float"))

# Load to Hive
df_transformed.write.mode("overwrite").saveAsTable("tlc_fares")
```

## Model Training and Tuning

Once the data is ready, we proceed to train our machine learning models. We focus on two primary algorithms: Linear Regression and Gradient Boosted Trees.

### Steps for Model Training

1. **Split Data**: Divide the dataset into training and testing sets.
2. **Train Models**: Use Spark ML to train both models.
3. **Tune Hyperparameters**: Use cross-validation to optimize model performance.

### Sample Model Training Code

```python
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

# Prepare data for training
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
train_data = assembler.transform(df_transformed)

# Train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="fare_amount")
lr_model = lr.fit(train_data)

# Train Gradient Boosted Tree model
gbt = GBTRegressor(featuresCol="features", labelCol="fare_amount")
gbt_model = gbt.fit(train_data)

# Evaluate models
evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName="rmse")
lr_rmse = evaluator.evaluate(lr_model.transform(train_data))
gbt_rmse = evaluator.evaluate(gbt_model.transform(train_data))
```

## Deployment

After training and tuning the models, we outline a deployment strategy. This involves integrating the models into a web application or API for real-time predictions.

### Deployment Steps

1. **Model Serialization**: Save the trained models using joblib or pickle.
2. **API Development**: Create a RESTful API using Flask or FastAPI.
3. **Containerization**: Use Docker to containerize the application for easier deployment.

### Sample Deployment Code

```python
import joblib
from flask import Flask, request, jsonify

app = Flask(__name__)

# Load model
model = joblib.load("linear_regression_model.pkl")

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json(force=True)
    prediction = model.predict(data["features"])
    return jsonify({'prediction': prediction.tolist()})

if __name__ == '__main__':
    app.run(debug=True)
```

## Getting Started

To get started with this project, follow these steps:

1. **Clone the Repository**: 
   ```bash
   git clone https://github.com/ahmedmohamedfoua/Big-Data-Project---Predicting-Trip-Fares-with-Spark-Hive.git
   cd Big-Data-Project---Predicting-Trip-Fares-with-Spark-Hive
   ```

2. **Install Dependencies**: Ensure you have the necessary libraries installed.
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Jupyter Notebook**: 
   ```bash
   jupyter notebook
   ```

4. **Follow the Instructions**: Open the notebook and follow the instructions to run the ETL process and model training.

## Releases

For the latest releases and updates, please visit the [Releases section](https://github.com/ahmedmohamedfoua/Big-Data-Project---Predicting-Trip-Fares-with-Spark-Hive/releases). You can download the latest version and execute the provided scripts.

## Contributing

Contributions are welcome! If you would like to contribute to this project, please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes.
4. Commit your changes (`git commit -m 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Create a new Pull Request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Feel free to explore the code and enhance the project further. Your contributions can help improve the accuracy and efficiency of predicting trip fares in NYC!