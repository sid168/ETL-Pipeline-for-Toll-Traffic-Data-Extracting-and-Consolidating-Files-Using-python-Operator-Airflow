from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import tarfile
import csv

# Define default_args for the DAG
default_args = {
    'owner': 'YourName',
    'depends_on_past': False,
    'email': ['dummy@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
    start_date=datetime.today(),
    catchup=False
)

def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    local_path = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)

def untar_dataset():
    local_path = '/home/project/airflow/dags/python_etl/staging/tolldata.tgz'
    extract_path = '/home/project/airflow/dags/python_etl/staging/'
    with tarfile.open(local_path) as tar:
        tar.extractall(path=extract_path)

def extract_data_from_csv():
    input_file = '/home/project/airflow/dags/python_etl/staging/vehicle-data.csv'
    output_file = '/home/project/airflow/dags/python_etl/staging/csv_data.csv'
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])
        writer.writeheader()
        for row in reader:
            writer.writerow({
                'Rowid': row['Rowid'],
                'Timestamp': row['Timestamp'],
                'Anonymized Vehicle number': row['Anonymized Vehicle number'],
                'Vehicle type': row['Vehicle type']
            })

def extract_data_from_tsv():
    input_file = '/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv'
    output_file = '/home/project/airflow/dags/python_etl/staging/tsv_data.csv'
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        writer = csv.DictWriter(outfile, fieldnames=['Number of axles', 'Tollplaza id', 'Tollplaza code'])
        writer.writeheader()
        for row in reader:
            writer.writerow({
                'Number of axles': row['Number of axles'],
                'Tollplaza id': row['Tollplaza id'],
                'Tollplaza code': row['Tollplaza code']
            })

def extract_data_from_fixed_width():
    input_file = '/home/project/airflow/dags/python_etl/staging/payment-data.txt'
    output_file = '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Type of Payment code', 'Vehicle Code'])
        for line in infile:
            writer.writerow([line[0:6].strip(), line[6:12].strip()])

def consolidate_data():
    output_file = '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code'])

        # Read csv_data.csv
        with open('/home/project/airflow/dags/python_etl/staging/csv_data.csv', 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header
            csv_data = list(reader)

        # Read tsv_data.csv
        with open('/home/project/airflow/dags/python_etl/staging/tsv_data.csv', 'r') as tsvfile:
            reader = csv.reader(tsvfile)
            next(reader)  # Skip header
            tsv_data = list(reader)

        # Read fixed_width_data.csv
        with open('/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv', 'r') as fwfile:
            reader = csv.reader(fwfile)
            next(reader)  # Skip header
            fw_data = list(reader)

        for i in range(len(csv_data)):
            writer.writerow(csv_data[i] + tsv_data[i] + fw_data[i])

def transform_data():
    input_file = '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'
    output_file = '/home/project/airflow/dags/python_etl/staging/transformed_data.csv'
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Vehicle type'] = row['Vehicle type'].upper()
            writer.writerow(row)

with dag:
   task1 = PythonOperator(task_id='download_dataset', python_callable=download_dataset)
   task2 = PythonOperator(task_id='untar_dataset', python_callable=untar_dataset)
   task3 = PythonOperator(task_id='extract_data_from_csv', python_callable=extract_data_from_csv)
   task4 = PythonOperator(task_id='extract_data_from_tsv', python_callable=extract_data_from_tsv)
   task5 = PythonOperator(task_id='extract_data_from_fixed_width', python_callable=extract_data_from_fixed_width)
   task6 = PythonOperator(task_id='consolidate_data', python_callable=consolidate_data)
   task7 = PythonOperator(task_id='transform_data', python_callable=transform_data)

   # Define task dependencies
   task1 >> task2 >> [task3, task4, task5] >> task6 >> task7

