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

default_args = {
    'owner': 'nduti',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

base_url = "https://www.airlinequality.com/airline-reviews/british-airways/"
page_size = 100

def scrape_all_pages():
    try:
        reviews_df = pd.read_csv('C:/Users/user/Desktop/Airflow_Docker/raw_reviews.csv', index_col=0)
    except FileNotFoundError:
        reviews_df = pd.DataFrame(columns=["Review", "Date"])
    
    page_number = 1
    new_reviews = []
    new_dates = []

    while True:
        try:
            url = f"{base_url}page/{page_number}/?sortby=post_date%3ADesc&pagesize={page_size}"
            response = requests.get(url)
            content = response.content
            parsed_content = BeautifulSoup(content, 'html.parser')

            reviews = []
            dates = []

            for review in parsed_content.find_all("div", {"class": "text_content"}):
                review_text = review.get_text().strip()
                if review_text not in reviews_df["Review"].values:
                    reviews.append(review_text)

            for time in parsed_content.find_all("time", {"itemprop": "datePublished"}):
                date_text = time.get_text().strip()
                dates.append(date_text)

            if not reviews:
                break

            new_reviews.extend(reviews)
            new_dates.extend(dates)
            page_number += 1

        except Exception as e:
            logging.error(f"Error occurred while scraping: {str(e)}")
            break

    if new_reviews:
        new_reviews_df = pd.DataFrame({"Review": new_reviews, "Date": new_dates})
        reviews_df = pd.concat([reviews_df, new_reviews_df], ignore_index=True)
        try:
            reviews_df.to_csv('C:/Users/user/Desktop/Airflow_Docker/raw_reviews.csv', index=False)
        except Exception as e:
            logging.error(f"Error occurred while saving CSV: {str(e)}")
    
    else:
        logging.info("No new reviews found")
        
    return reviews_df  # Return the DataFrame after scraping

def upload_to_s3(kwargs):
    
    reviews_df = kwargs['ti'].xcom_pull(task_ids='scrape_task')
    
    if reviews_df is not None:
        
        access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        try:
            s3 = boto3.resource(
            service_name='s3',
            region_name='us-east-1',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
            )
        
            bucket_name = "unprocessed-reviews"
            key_name = 'reviews_data.csv'

            csv_content = reviews_df.to_csv(index=False)
            s3.Bucket(bucket_name).put_object(Key=key_name, Body=csv_content)
            logging.info("DataFrame uploaded to S3 successfully")
            
        except Exception as e:
            logging.error(f"Error occurred while uploading to S3: {str(e)}")
            
    else:
        logging.info("No DataFrame to upload")            

# Create an instance of the DAG
with DAG('data_scraping_and_upload_final', default_args=default_args, schedule_interval='@daily') as dag:

    # Task 1: Scrape data
    scrape_task = PythonOperator(
        task_id='scrape_data_task',
        python_callable=scrape_all_pages
    )

    # Task 2: Save data locally and upload to S3
    upload_task = PythonOperator(
        task_id='save_upload_data_task',
        provide_context=True,
        python_callable=upload_to_s3
    )

    # Set task dependencies
    scrape_task >> upload_task

