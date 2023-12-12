# End to End Sentiment Analysis Project

# Introduction

This project encompasses the extraction of review data from a website through scraping techniques. Utilizing Airflow, we'll orchestrate a data pipeline responsible for the extraction and subsequent storage of these reviews into an S3 bucket.

Following this, we'll execute sentiment analysis on the gathered data. Finally, we leverage Streamlit to deploy an interactive dashboard, facilitating a comprehensive view of the sentiment analysis results, ensuring a user-friendly exploration of the insights derived from the analysis

# Architecture
<img src="Sentiment_Architecture.png">

Components:

- **Beautifoul Soup**: We'll utilise this library to scrape data from an airline reviews website.
- **Docker**: Facilitates the execution of airflow in a containerized environment.
- **Apache Airflow**: Responsible for orchestrating the pipeline, carrying out scheduled scraping and uploading the raw data into a s3 bucket.
- **S3 Bucket**: Allows for storage of the raw data.
- **NLTK Library**: Facilitates text processing for sentiment analysis.
- **Streamlit**: Allows the deployment of an interactive dashboard relaying the analysis findings. 
