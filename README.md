# End to End Sentiment Analysis Project

# Introduction

This project encompasses the extraction of review data from a website through scraping techniques. Utilizing Airflow, we'll orchestrate a data pipeline responsible for the extraction and subsequent storage of these reviews into an S3 bucket.

Following this, we'll execute sentiment analysis on the gathered data. Finally, we leverage Streamlit to deploy an interactive dashboard, facilitating a comprehensive view of the sentiment analysis results, ensuring a user-friendly exploration of the insights derived from the analysis

# Architecture
<img src="Sentiment_Architecture.png">

Components:

- **Beautifoul Soup**: We'll utilise this library to scrape data from an airline reviews website.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.
