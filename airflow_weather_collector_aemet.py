# Weather collector (airflow version)

# Author: Oriol Ordi

# This script is meant to be run on airflow

# The script collects hourly weather data from different spanish cities
# The data collected is from the AEMET API
# The data collected is dumped into Amazon S3


# Import statements
import requests
import pandas as pd
import boto3
from datetime import date, datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import io


# Define the function that the PythonOperator will call
def s3_upload_dataframes_in_bytes_function():
    # Set up the url for the data as well as the API key for AEMET data
    stations = {'Barcelona': '0201D', 'Madrid': '3195', 'Villalon': '2593D', 'Valladolid': '2422', 'Tarragona': '0042Y'}
    url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/'
    api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvcmlvbC5vcmRpQGdtYWlsLmNvbSIsImp0aSI6IjQ0NjYyNzRiLTZjZTEtNDQ4OS1i' \
            'NWVjLTZjMjVmNDBkNTU4YiIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTk4ODgxNzQ0LCJ1c2VySWQiOiI0NDY2Mjc0Yi02Y2UxLTQ0ODktY' \
            'jVlYy02YzI1ZjQwZDU1OGIiLCJyb2xlIjoiIn0.wgkrh9IMh_tkUEOhu234JemE5Mex2xOCHIgDC1Z5Bzw'
    # Define the name of the S3 bucket where the data will be dumped
    #bucket_name = "bucketweatheraemetairflow"
    BUCKET_NAME = 'bucketweatheraemetairflow'
    # Iterate for every city and get the data from AEMET
    dict_dfs_weather = {}
    for city, city_code in stations.items():
        # Get the complete URL for the requests.get method
        complete_url = url + city_code + '/?api_key=' + api_key
        # Call the function to connect with the AEMET API and get the wheather info
        json_access = requests.get(complete_url).json()
        # Check if the data could be accessed
        if json_access['estado'] == 200:
            # Get the list of dictionaries of hourly data
            json_weather = requests.get(json_access['datos']).json()
            # Convert the list of dictionaries to a pandas dataframe
            df_weather = pd.DataFrame(json_weather)
            # Set the time as index and sort by the time (the index)
            df_weather.set_index('fint', inplace=True)
            df_weather.sort_index(inplace=True)
            dict_dfs_weather[city] = df_weather

    # Get year, month and day
    the_date = date.today().strftime('%Y-%m-%d')
    the_year = the_date.split('-')[0]
    the_month = the_date.split('-')[1]
    the_day = the_date.split('-')[2]
    # Set the S3 bucket name
    bucket_name = 'bucketttbototest'
    # Define the hook with the name of the airflow connection
    hook = S3Hook('aws_s3')
    for city, df in dict_dfs_weather.items():
        # Write bytes as buffer and upload them to S3, ultimately getting a csv file
        with io.BytesIO() as buffer:
            buffer.write(
                bytes(
                    df.to_csv(None, sep=",", quotechar='"'),
                    encoding="utf-8"
                )
            )
            hook.load_bytes(
                buffer.getvalue(),
                bucket_name=bucket_name,
                key=city + '/' + the_year + '/' + the_month + '/' + the_day + '.csv',
                replace=True
            )


# Define the DAG
weather_dag_name = DAG(dag_id='weather_dag',
                       start_date=datetime(2020, 7, 20),
                       schedule_interval='50 23 * * *')


# Define the task (PythonOperator)
s3_task = PythonOperator(task_id='weather_task',
                         python_callable=s3_upload_dataframes_in_bytes_function,
                         dag=weather_dag_name)
