# 🚦 California Traffic Collision Data Analysis (ETL Pipeline)

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)
![Status](https://img.shields.io/badge/Status-Completed-green)

## 📌 Project Overview
This project is an end-to-end **ETL (Extract, Transform, Load) and Data Analysis** pipeline built to investigate traffic collision patterns in California. Using a massive dataset (~5 million records) from the **Statewide Integrated Traffic Records System (SWITRS)**, the project leverages **Apache Spark (PySpark)** for big data processing and **AWS S3** for scalable storage.

The analysis focuses on identifying high-risk locations, predicting collision severity, and understanding the impact of environmental factors (weather, lighting) and temporal factors (DST, time of day) on road safety.

> **Coursework Context:** This project was developed as part of the **Executive Diploma in Data Science and Gen AI** curriculum at **IIIT-Bangalore**.

---

## 🎯 Objectives & Business Value
* **Big Data Processing:** Utilize PySpark to clean, transform, and analyze large-scale crash data.
* **Cloud Integration:** Implement an ETL pipeline storing processed data in **AWS S3** for downstream usage.
* **Safety Insights:** Identify accident hotspots and causes to inform city planning and policy-making.
* **Predictive Modeling:** Develop machine learning models (Logistic Regression, SVM, KNN) to predict the severity of injuries in collisions.

---

## 🛠️ Tech Stack
* **Language:** Python
* **Big Data Engine:** Apache Spark (PySpark 3.5.4)
* **Cloud Storage:** AWS S3
* **Data Manipulation:** Pandas, SQL (SQLite)
* **Visualization:** Matplotlib, Seaborn
* **Machine Learning:** Scikit-Learn (KNN, SVM, Logistic Regression)

---

## 📂 Dataset
The data is sourced from the **California Highway Patrol (CHP)** via Kaggle. It spans from **2001 to 2020** and consists of three primary tables:
1.  **Collisions:** Details on location, time, weather, and severity.
2.  **Parties:** Information on drivers, pedestrians, and cyclists involved.
3.  **Victims:** Demographics and injury details of victims.

* **Data Source:** [California Traffic Collision Data from SWITRS](https://www.kaggle.com/datasets/alexgude/california-traffic-collision-data-from-switrs)

---

## ⚙️ Project Workflow

### 1. Data Ingestion & Preparation
* Integrated with Kaggle API to fetch the raw SQLite database.
* Loaded data into **PySpark DataFrames** for distributed processing.
* Applied sampling techniques to handle memory constraints during development (focusing on the most recent 300,000 records).

### 2. Data Cleaning & Transformation
* **Schema Validation:** Cast numerical columns (latitude, longitude, victim counts) and parsed timestamps.
* **Missing Value Handling:** Imputed nulls for categorical (e.g., 'Unknown') and numerical features.
* **Outlier Detection:** Used Interquartile Range (IQR) to identify and filter outliers in victim counts.
* **Feature Engineering:** Mapped categorical variables (Weather, Road Surface) using StringIndexers for ML readiness.

### 3. Exploratory Data Analysis (EDA)
* **Severity Distribution:** Analyzed the class imbalance between minor injuries and fatalities.
* **Temporal Trends:** Visualized collision frequency by hour of day (identifying peak accident times) and day of week.
* **Environmental Factors:** Correlated lighting conditions and weather patterns with collision severity.
* **Geospatial Analysis:** Mapped collisions to identify county-level hotspots (e.g., LA County).

### 4. ETL & Storage
* Processed clean datasets were written to **CSV format** and uploaded to an **AWS S3 bucket** for scalable access.
* Configured Spark to write optimized partitions for efficient querying.

### 5. Predictive Modeling
* Built classification models to predict **Collision Severity** (Fatal vs. Injury vs. Property Damage).
* **Models Tested:**
    * K-Nearest Neighbors (KNN)
    * Support Vector Machine (SVM)
    * **Logistic Regression** (Best Performer)
* **Metrics:** Evaluated using F1-Score and Accuracy, addressing class imbalance via stratified sampling.

---

## 📊 Key Insights
* **High-Risk Timing:** Collision frequency peaks during rush hours, but **fatality rates** are higher during late-night hours and weekends, correlating with lower visibility and potential alcohol involvement.
* **Location Hotspots:** Significant accident clusters were identified near **State Route 60 and Grand Avenue**.
* **Alcohol Influence:** A strong correlation exists between alcohol involvement and the severity of collisions on weekends (Friday–Sunday).
* **Model Performance:** Logistic Regression achieved the highest F1-score (0.41) and "Lenient Accuracy" (~79%), proving most effective at distinguishing between severity levels given the noisy nature of crash data.

---

## 💻 How to Run
1.  **Prerequisites:**
    * Python environment with Jupyter Notebook support.
    * Apache Spark installed or a cloud environment (Google Colab/Databricks).
    * Kaggle API key (`kaggle.json`) for dataset download.
    * AWS credentials for S3 upload (optional if running locally).

2.  **Installation:**
    ```bash
    pip install pyspark pandas matplotlib seaborn kagglehub
    ```

3.  **Execution:**
    * Clone the repo:
        ```bash
        git clone [https://github.com/YOUR_USERNAME/California-Traffic-ETL.git](https://github.com/jeno22ndr/California-Traffic-ETL.git)
        ```
    * Open `ETL_Traffic_Data_Analysis_PONJENO_NADAR.ipynb`.
    * Run the cells sequentially. Ensure you provide your own API keys where prompted.

---

## 👤 Author
**Ponjeno Nadar**
* *Executive Diploma in Data Science and Gen AI*
* *IIIT-Bangalore*

---

*Disclaimer: This analysis is for educational purposes using public datasets. The findings should not be used as a substitute for official traffic safety reports.*
