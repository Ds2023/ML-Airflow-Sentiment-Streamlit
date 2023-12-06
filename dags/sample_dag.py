import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import os
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
from random import randint

default_args = {
    'owner': 'nduti',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_dataframe():
    
    sample_df = pd.DataFrame(columns = ['reviews','date'])
    sample_df['review'] = ['a','b','c','d','e']
    sample_df['noma'] = [1,2,3,4,5]
    
    return sample_df

def upload_df():
    
    data = generate_dataframe()
    
    csv_content = data.to_csv(index=False)

    access_key = 'AKIA3YJY5LWFUUSONFK2'
    secret_key = '1AaVsTfz7pNx0Snj3/9trnBuYj/SozNADRT0CSCC'

    try:
        
        s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )
    
        bucket_name = "trial-dag-bucket"
        key_name = 'sample_data2.csv'

        s3.Bucket(bucket_name).put_object(Key=key_name, Body=csv_content)
        logging.info("DataFrame uploaded to S3 successfully")
            
    except Exception as e:
        
        logging.error(f"Error occurred while uploading to S3: {str(e)}")

dag = DAG('upload_trial_dag',
          default_args=default_args,
          description='DAG to generate DataFrame and upload to S3',
          schedule_interval='@daily',  # Change the interval as needed
          catchup=False)

        
generate_df_task = PythonOperator(
    task_id='generate_dataframe_task',
    python_callable=generate_dataframe,
    dag=dag
)

# Create task to upload DataFrame to S3
upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_df,
    dag=dag
)

generate_df_task >> upload_to_s3_task