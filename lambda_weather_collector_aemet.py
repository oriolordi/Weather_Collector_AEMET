# Weather collector (AWS Lambda version)

# Author: Oriol Ordi

# This script is meant to be run on amazon lambda, together will amaon cloudwatch to be run daily

# The script collects hourly weather data from different spanish cities
# The data collected is from the AEMET API
# The data collected is dumped into Amazon S3


# Import statements
import requests
import pandas as pd
import boto3
from datetime import date


# Define the function to collect data from the AEMET API
def get_weather_df(complete_url):
    available = True
    # Get the data from the requests.get method
    try:
        json_access = requests.get(complete_url).json()
    except requests.exceptions.SSLError:
        # Return False if it can't reach the API
        available = False
    # Check if the data could be accessed
    if json_access['estado'] == 200:
        try:
            # Get the list of dictionaries of hourly data
            json_weather = requests.get(json_access['datos']).json()
            # Convert the list of dictionaries to a pandas dataframe
            df_weather = pd.DataFrame(json_weather)
            # Set the time as index and sort by the time (the index)
            df_weather.set_index('fint', inplace=True)
            df_weather.sort_index(inplace=True)
        except:
        # Return False if it can't reach the information
            available = False
    else:
        # Return False if it can't reach the API
        available = False
    # Return True if it reached the information, along with the df
    if available:
        return True, df_weather
    else:
        return False, 0


# Define the function to upload the weather information to S3
# This function saves each dataframe (for each city) containing the weather
# for the day into an S3 csv file
def upload_dfs_to_s3(dict_dfs_weather, bucket_name):
    # Access the s3 service
    s3 = boto3.resource('s3')
    # Check if the bucket exists, create it otherwise
    bucket_names = [bucket.name for bucket in s3.buckets.all()]
    if bucket_name not in bucket_names:
        s3.create_bucket(Bucket=bucket_name)
    # Get year, month and day
    the_date = date.today().strftime('%Y-%m-%d')
    the_year = the_date.split('-')[0]
    the_month = the_date.split('-')[1]
    the_day = the_date.split('-')[2]
    # Create a file for each of the dataframes
    for city, df in dict_dfs_weather.items():
        # Create the name of the file
        file_name = city + '/' + the_year + '/' + the_month + '/' + the_day + '.csv'
        # Select the file
        file = s3.Object(bucket_name, file_name)
        # Upload the csv to the file in S3
        try:
            file.put(Body=df.to_csv())
        except:
            pass


# Define the main function
def main():
    # Set up the url for the data as well as the API key for AEMET data
    stations = {'Barcelona': '0201D', 'Madrid': '3195', 'Villalon': '2593D', 'Valladolid': '2422', 'Tarragona': '0042Y'}
    url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/'
    api_key = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvcmlvbC5vcmRpQGdtYWlsLmNvbSIsImp0aSI6IjQ0NjYyNzRiLTZjZTEtNDQ4OS1i' \
            'NWVjLTZjMjVmNDBkNTU4YiIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTk4ODgxNzQ0LCJ1c2VySWQiOiI0NDY2Mjc0Yi02Y2UxLTQ0ODktY' \
            'jVlYy02YzI1ZjQwZDU1OGIiLCJyb2xlIjoiIn0.wgkrh9IMh_tkUEOhu234JemE5Mex2xOCHIgDC1Z5Bzw'
    # Define the name of the S3 bucket where the data will be dumped
    bucket_name = "bucketweatheraemetlambda"

    # Iterate for every city and get the data from AEMET
    dict_dfs_weather = {}
    for city, city_code in stations.items():
        # Get the complete URL for the requests.get method
        complete_url = url + city_code + '/?api_key=' + api_key
        # Call the function to connect with the AEMET API and get the wheather info
        info_reached, df = get_weather_df(complete_url)
        if info_reached:
            dict_dfs_weather[city] = df

    # Check if the program managed to extract any dataframes
    if bool(dict_dfs_weather):
        upload_dfs_to_s3(dict_dfs_weather, bucket_name)


# Execute the lambda function
def lambda_handler(event, context):
    # Execute the main function
    main()
