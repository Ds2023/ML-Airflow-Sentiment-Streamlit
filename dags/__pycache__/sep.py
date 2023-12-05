# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import boto3
# import os

# default_args = {
#     'owner': 'nduti',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 10, 10),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# base_url = "https://www.airlinequality.com/airline-reviews/british-airways/"
# page_size = 100

# def scrape_all_pages():
#     global reviews_df
    
#     # Load existing reviews DataFrame or create a new one if it doesn't exist
#     try:
#         reviews_df = pd.read_csv('C:/Users/user/Desktop/Airflow_Docker/data/reviews.csv', index_col=0)
#     except FileNotFoundError:
#         reviews_df = pd.DataFrame(columns=["Review", "Date"])

#     page_number = 1
#     new_reviews = []
#     new_dates = []

#     while True:
#         url = f"{base_url}page/{page_number}/?sortby=post_date%3ADesc&pagesize={page_size}"
#         response = requests.get(url)
#         content = response.content
#         parsed_content = BeautifulSoup(content, 'html.parser')
        
#         reviews = []
#         dates = []
        
#         for review in parsed_content.find_all("div", {"class": "text_content"}):
#             review_text = review.get_text().strip()
#             if review_text not in reviews_df["Review"].values:
#                 reviews.append(review_text)
        
#         for time in parsed_content.find_all("time", {"itemprop": "datePublished"}):
#             date_text = time.get_text().strip()
#             dates.append(date_text)

#         if not reviews:
#             break

#         new_reviews.extend(reviews)
#         new_dates.extend(dates)
#         page_number += 1

#     if new_reviews:
#         new_reviews_df = pd.DataFrame({"Review": new_reviews, "Date": new_dates})
#         reviews_df = pd.concat([reviews_df, new_reviews_df], ignore_index=True)
#         reviews_df.to_csv('C:/Users/user/Desktop/Airflow_Docker/data/reviews.csv', index=False)
        
#         return new_reviews, new_dates
#     else:
#         return [], []

# def upload_to_s3(new_reviews, new_dates):
#     if new_reviews:
#         access_key = os.environ.get('AWS_ACCESS_KEY_ID')
#         secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
#         # Upload DataFrame to S3 bucket
#         s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#         bucket_name = 'unprocessed-reviews'
#         s3_file_key = 'reviews.csv'

#         new_reviews_df = pd.DataFrame({"Review": new_reviews, "Date": new_dates})
#         new_reviews_df.to_csv('/tmp/reviews.csv', index=False)

#         with open('/tmp/reviews.csv', 'rb') as data:
#             s3.upload_fileobj(data, bucket_name, s3_file_key)

#         print(f"Found {len(new_reviews)} new reviews and uploaded to S3")
#     else:
#         print("No new reviews found.")

# dag = DAG('web_scraping_dag2', default_args=default_args, schedule_interval='@daily')

# scrape_task = PythonOperator(
#     task_id='scrape_task',
#     python_callable=scrape_all_pages,
#     dag=dag,
# )

# upload_task = PythonOperator(
#     task_id='upload_task',
#     python_callable=upload_to_s3,
#     provide_context=True,  # Required to receive task context in the function
#     dag=dag,
# )

# scrape_task >> upload_task  # Set task dependencies
