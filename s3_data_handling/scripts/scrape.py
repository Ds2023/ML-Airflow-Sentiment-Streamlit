import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import os

base_url = "https://www.airlinequality.com/airline-reviews/british-airways/"
page_size = 100

def scrape_all_pages():
    global reviews_df
    
    access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # Load existing reviews DataFrame or create a new one if it doesn't exist
    try:
        reviews_df = pd.read_csv('C:/Users/user/Desktop/Scraping/reviews.csv', index_col=0)
    except FileNotFoundError:
        reviews_df = pd.DataFrame(columns=["Review","Date"])

    page_number = 1
    new_reviews = []
    new_dates = []

    while True:
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

    if new_reviews:
        new_reviews_df = pd.DataFrame({"Review": new_reviews, "Date": new_dates})
        reviews_df = pd.concat([reviews_df, new_reviews_df], ignore_index=True)
        reviews_df.to_csv('C:/Users/user/Desktop/Scraping/reviews.csv', index=False)
        
    else:
        print("No new reviews found")
        
scrape_all_pages()