import boto3
import os
import pandas as pd


def download_from_bucket():
    
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    obj = s3.Bucket("unprocessed-reviews").Object("reviews_again.csv").get()
    data = pd.read_csv(obj['Body'],index_col = 0)

    return data

data = download_from_bucket()
data.to_csv("downloaded_data.csv")