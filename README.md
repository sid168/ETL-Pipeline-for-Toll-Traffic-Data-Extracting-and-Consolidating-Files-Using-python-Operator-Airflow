# ETL-Pipeline-for-Toll-Traffic-Data-Extracting-and-Consolidating-Files-Using-python-Operator-Airflow
In this project, we aim to de-congest national highways by analyzing road traffic data from various toll plazas. Each toll plaza uses different file formats for its data, and we will extract, transform, and consolidate these files using Apache Airflow.


### **Code Structure**

1. **Create Python Functions**:
    - `download_dataset`
    - `untar_dataset`
    - `extract_data_from_csv`
    - `extract_data_from_tsv`
    - `extract_data_from_fixed_width`
    - `consolidate_data`
    - `transform_data`

2. **Define Airflow DAG**:
    - Create a DAG using PythonOperators to execute the ETL functions in the correct order.

---


#### 1. **Imports and DAG Definition**

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import tarfile
import pandas as pd
import requests

# Define default arguments for the DAG
default_args = {
    'owner': 'YourName',
    'start_date': datetime.today(),
    'email': ['youremail@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='ETL Pipeline for Road Traffic Data',
    schedule_interval='@daily',
)
```


- We import the necessary modules for defining the DAG and PythonOperators.
- The `default_args` dictionary defines metadata like the DAG owner, start date, retry attempts, and delay between retries.
- The DAG is scheduled to run daily (`schedule_interval='@daily'`).

---

#### 2. **Python Function to Download the Dataset**

```python
def download_dataset():
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
    local_path = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    
    response = requests.get(url)
    
    with open(local_path, 'wb') as file:
        file.write(response.content)

# Task 1: Download dataset
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)
```


- The `download_dataset` function downloads the dataset from the URL and saves it in the staging directory.
- `requests.get` fetches the file, and it is written to the local file system.
- `PythonOperator` is used to execute this function as part of the Airflow DAG.

---

#### 3. **Untar the Dataset**

```python
def untar_dataset():
    local_path = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    extract_path = '/home/project/airflow/dags/python_etl/staging/'
    
    with tarfile.open(local_path) as tar:
        tar.extractall(path=extract_path)

# Task 2: Untar dataset
untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)
```


- `untar_dataset` extracts the downloaded `.tgz` file into the staging directory.
- The `tarfile` module is used to untar the file.
- Another `PythonOperator` is used to execute the untar task in the pipeline.

---

#### 4. **Extract Data from CSV**

```python
def extract_data_from_csv():
    input_file = '/home/project/airflow/dags/python_etl/staging/vehicle-data.csv'
    output_file = '/home/project/airflow/dags/python_etl/staging/csv_data.csv'
    
    df = pd.read_csv(input_file)
    selected_columns = df[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    selected_columns.to_csv(output_file, index=False)

# Task 3: Extract data from CSV
extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)
```


- `extract_data_from_csv` reads the CSV file, selects the necessary columns, and saves them to `csv_data.csv`.
- We use **Pandas** to handle data extraction and file saving.
- This task is also handled by `PythonOperator`.

---

#### 5. **Extract Data from TSV**

```python
def extract_data_from_tsv():
    input_file = '/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv'
    output_file = '/home/project/airflow/dags/python_etl/staging/tsv_data.csv'
    
    df = pd.read_csv(input_file, sep='\t')
    selected_columns = df[['Number of axles', 'Tollplaza id', 'Tollplaza code']]
    selected_columns.to_csv(output_file, index=False)

# Task 4: Extract data from TSV
extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
```


- This function processes the TSV file by reading it with Pandas and selecting relevant columns.
- The data is saved into a CSV file (`tsv_data.csv`).
- The task is linked to the DAG using `PythonOperator`.

---

#### 6. **Extract Data from Fixed-width File**

```python
def extract_data_from_fixed_width():
    input_file = '/home/project/airflow/dags/python_etl/staging/payment-data.txt'
    output_file = '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'
    
    colspecs = [(0, 10), (10, 20)]  # Define column widths
    df = pd.read_fwf(input_file, colspecs=colspecs, header=None)
    df.columns = ['Type of Payment code', 'Vehicle Code']
    df.to_csv(output_file, index=False)

# Task 5: Extract data from fixed-width file
extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)
```


- `read_fwf` from Pandas reads fixed-width files. We define the column widths using `colspecs`.
- The selected data is saved as `fixed_width_data.csv`.
- This task is added to the DAG with `PythonOperator`.

---

#### 7. **Consolidate Data**

```python
def consolidate_data():
    csv_file = '/home/project/airflow/dags/python_etl/staging/csv_data.csv'
    tsv_file = '/home/project/airflow/dags/python_etl/staging/tsv_data.csv'
    fixed_width_file = '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'
    output_file = '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
    
    df_csv = pd.read_csv(csv_file)
    df_tsv = pd.read_csv(tsv_file)
    df_fixed_width = pd.read_csv(fixed_width_file)
    
    consolidated_df = pd.concat([df_csv, df_tsv, df_fixed_width], axis=1)
    consolidated_df.to_csv(output_file, index=False)

# Task 6: Consolidate data
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)
```


- The `consolidate_data` function merges data from the CSV, TSV, and fixed-width files into one consolidated file.
- It uses Pandas to concatenate the DataFrames horizontally.
- The result is saved in `extracted_data.csv`.

---

#### 8. **Transform Data**

```python
def transform_data():
    input_file = '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
    output_file = '/home/project/airflow/dags/python_etl/staging/transformed_data.csv'
    
    df = pd.read_csv(input_file)
    df['Vehicle type'] = df['Vehicle type'].str.upper()
    df.to_csv(output_file, index=False)

# Task 7: Transform data
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)
```


- This function capitalizes the "Vehicle type" column in the extracted data.
- The transformed data is saved in `transformed_data.csv`.

---

#### 9. **Define Task Pipeline**

```python
download_task >> untar_task >> extract_csv_task >> extract_tsv_task >> extract_fixed_width_task >> consolidate_task >> transform_task
```

- This line defines the order of task execution. Each task will wait for its predecessor to complete before executing.



This ETL pipeline uses Apache Airflow’s PythonOperator to automate the process of downloading, extracting, transforming, and consolidating road traffic data. The pipeline can run on a daily schedule

