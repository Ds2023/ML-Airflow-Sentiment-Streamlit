import boto3
import os

def upload_to_bucket():

    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    bucket_name = 'unprocessed-reviews'
    file_name = 'C:/Users/user/Desktop/Scraping/reviews.csv'
    key_name = 'reviews_again.csv'

    bucket = s3.Bucket(bucket_name)
    bucket.upload_file(Filename=file_name, Key=key_name)