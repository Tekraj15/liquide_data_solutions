
import boto3
from io import StringIO
import requests
import pandas as pd
import numpy as np

import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date

current_date = date.today()
curr_day = current_date.strftime("%Y-%m-%d")
s3_client = boto3.client('s3')
bucket_name = 'tekraj-test2'
key_fin = 'liquid_raw_data/finance_fr/load_date = {}/finance_ratio_standalone_{}.csv'.format(curr_day, curr_day)
key_quart= 'liquid_raw_data/quarter_cons/load_date = {}/fquarter_cons{}.csv'.format(curr_day, curr_day)

# Extractive Data from API
def retrieve_data(token, date, section, filename):
  
    # endpoint URL
    base_url = "https://contentapi.accordwebservices.com/RawData/GetTxtFile"
    # Construct the API request URL
    api_url = f"{base_url}?filename={filename}&date={date}&section={section}&sub=&token={token}"

    try:
      
      # Construct the API request URL with the parameters
      api_url = f"{base_url}?filename={filename}&date={date}&section={section}&sub=&token={token}"
      response = requests.get(api_url).json()
      status_code = requests.get(api_url).status_code
      if status_code == 200:

        table_data = response['Table']
        df = pd.DataFrame(table_data)


        # Return the extracted data
        return df

      else:
        print(f"API request failed with status code: {response.status_code}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Error occurred during API request: {e}")
        return None

# API Request Credentials for Finance_fr (Standalone) dataset

token = "fMqHkvwLKoN6rTyt_j7F3HNgnvhBtWWE"
date = "16082021"
section = "Fundamental"
filename_fn = "Finance_fr"
filename_quart = 'Quarterly_Cons'

s3_path_fin = 's3://'+ s3_bucket + '/' + file_path
s3_path_quart = 's3://'+ s3_bucket + '/' + file_path

fin_df = retrieve_data(token, date, section, filename_fn)
quarter_cons_df = retrieve_data(token, date, section, filename_quart)
if data is not None:
    # Print the extracted data
    print(data)
else:
    # Handle the case when data retrieval fails
    print("Data retrieval failed.")

csv_buffer = StringIO()
fin_df.to_csv(csv_buffer, index=False)


# Upload the finance_ration CSV data to S3
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_path_fin)

# Upload the finance_ration CSV data to S3

quarter_cons_df.to_csv(csv_buffer, index=False)
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_path_quart)

