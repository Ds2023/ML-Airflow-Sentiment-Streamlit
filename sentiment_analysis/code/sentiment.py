import streamlit as st
import nltk
import pandas as pd
import seaborn as sns 
import matplotlib.pyplot as plt
import plotly.express as px
from wordcloud import WordCloud
import re
import boto3
import os
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
nltk.download('averaged_perceptron_tagger')

def download_from_bucket():
    
    access_key = os.environ.get('AWS_ACCESS_KEY_ID') 
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY') 

    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    obj = s3.Bucket("trial-dag-bucket").Object("reviews_data.csv").get()
    data = pd.read_csv(obj['Body'],index_col = 0)

    return data

def extract_text_after_pipe(df):
    # Apply split by "|" to the 'Reviews' column and extract the part after the sign
    df['Text'] = df['Review'].apply(lambda x: x.split("|")[1].strip() if "|" in str(x) else x.strip())
    return df

# data = download_from_bucket()
# print(data.head())
# print(data.shape)
# print(data.columns)

def process_text(text):
    
    try:
    
        stop_words = set(stopwords.words('english'))
        analyzer = SentimentIntensityAnalyzer()

        def remove_re(text):
            regex = re.compile(r"[^\w\s]")
            text = regex.sub("", text)
            return text

        def preprocess_text(text):
            tokens = word_tokenize(text.lower())
            filtered_tokens = [token for token in tokens if token not in stop_words]
            lemmatizer = WordNetLemmatizer()
            lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]
            processed_text = ' '.join(lemmatized_tokens)
            return processed_text

        def get_score(text):
            scores = analyzer.polarity_scores(text)
            return scores['compound']

        def get_sentiment(compound):
            if compound >= 0.05:
                return 'Positive'
            elif compound < -0.05:
                return 'Negative'
            else:
                return 'Neutral'

        cleaned_text = remove_re(text)
        preprocessed_text = preprocess_text(cleaned_text)
        score = get_score(preprocessed_text)
        sentiment = get_sentiment(score)
        
        return preprocessed_text, score, sentiment
    except Exception as e:
        print(f"An error occurred while processing the data: {str(e)}")

def get_scores():
    
    data = download_from_bucket()

    if data is not None:
        
        data = data.reset_index()
        data = extract_text_after_pipe(data)       
        data['Processed_Reviews'], data['Sentiment_Score'], data['Sentiment'] = zip(*data['Text'].apply(lambda x: process_text(x)))

    return data

def preprocess_date(date_str):
    # Regular expression to remove ordinal suffixes from day (e.g., '1st', '2nd', '3rd', etc.)
    return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

# Define the load_data function
def load_data():
    # Load data from CSV file
    data = get_scores()
    
    # Preprocess 'Date' column to remove suffixes
    data['Date'] = data['Date'].apply(preprocess_date)
    
    # Convert 'Date' column to datetime
    data['Date'] = pd.to_datetime(data['Date'])
    
    return data

st.set_page_config(layout='wide')

st.title("Sentiment Analysis Dashboard")
st.subheader("British Airways")
st.markdown('<style>div.block-container{padding-top:1rem;}<style>',unsafe_allow_html=True)

@st.cache_data
def preprocess_date(date_str):
    # Regular expression to remove ordinal suffixes from day (e.g., '1st', '2nd', '3rd', etc.)
    return re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)

@st.cache_data
# Define the load_data function
def load_data():
    
    data = get_scores()
    
    # Preprocess 'Date' column to remove suffixes
    data['Date'] = data['Date'].apply(preprocess_date)
    
    # Convert 'Date' column to datetime
    data['Date'] = pd.to_datetime(data['Date'])
    
    return data

df = load_data()
# st.write(df)

# Get the min and max date
col1, col2 = st.columns((2))
startDate = df["Date"].min()
endDate = df["Date"].max()
with col1:
    date1 = pd.to_datetime(st.date_input("Start Date", startDate))
    st.write("Review Start Date: ",startDate)
with col2:
    date2 = pd.to_datetime(st.date_input("End Date", endDate))

df = df[(df["Date"] >= date1) & (df["Date"] <= date2)].copy()

# Sidebar Filter
st.sidebar.header("Choose your Filter: ")
label = st.sidebar.multiselect("Select Sentiment",df['Sentiment'].unique())
if not label:
    df = df.copy()
else:
    df = df[df['Sentiment'].isin(label)]
    
# Metrics
col1, col2 ,col3= st.columns(3)
metric1 = len(df)
metric2 = round(df['Sentiment_Score'].mean(),2)
if df['Sentiment_Score'].mean() > 0:
    metric3 = 'Positive'
else:
    metric3 = 'Negative'
col1.metric("Reviews To Date", metric1)
col2.metric("Average Sentiment Score",metric2)
col3.metric("Overall Sentiment",metric3)

# Create a pie chart
col1,col2 = st.columns([0.6,0.4])
## Visualization over time
with col1:
        st.subheader("Sentiment over the years (NLTK_Model)")
        df['Year'] = df['Date'].dt.year
        viz_df = df.groupby(['Year', 'Sentiment']).size().reset_index(name='count')
        fig,ax = plt.subplots(figsize=(12,6))
        df['Year'] = df['Date'].dt.year
        viz_df = df.groupby('Year',as_index=False)['Sentiment'].value_counts()
        sns.despine(top=True,right=True,left=True)
        sns.barplot(data=viz_df,x='Year',y='count',hue='Sentiment',legend=False)
        ax.set_axisbelow(True)
        ax.grid(axis='y') 
        st.pyplot(fig)
        
with col2:
    st.subheader("Sentiment Distribution")
    fig, ax = plt.subplots(figsize=(5, 5))
    # Outer pie chart (larger)
    value_counts = df['Sentiment'].value_counts()
    labels = value_counts.index.tolist()
    counts = value_counts.values.tolist()
    wedges_outer = ax.pie(counts, labels=labels, autopct='%1.1f%%', startangle=90, radius=1)
    plt.setp(wedges_outer[0], width=0.3, edgecolor='white')  # Update the wedges_outer
    # Inner pie chart (smaller to create the donut shape)
    wedges_inner = ax.pie([1], radius=0.7, colors='white')
    # Equal aspect ratio ensures that pie is drawn as a circle
    ax.axis('equal')
    # Display the chart using Streamlit
    st.pyplot(fig)
    
# Wordcloud on positive reviews
col1,col2 = st.columns((2))
with col1:
    st.subheader("Positive Sentiment")
    positive_df = df[df['Sentiment'] == 'Positive']
    text = ' '.join(positive_df['Processed_Reviews'].tolist())
    if text:
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
        plt.figure(figsize=(10, 8))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        st.pyplot(plt)
    else:
        st.write("No Positive Reviews Available")
        
# Wordcloud on negative reviews
with col2:
    st.subheader("Negative Sentiment")
    negative_df = df[df['Sentiment'] == 'Negative']
    text2 = ' '.join(negative_df['Processed_Reviews'].tolist())
    if text2:
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text2)
        plt.figure(figsize=(10, 8))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        st.pyplot(plt)
    else:
        st.write("No Negative Reviews Available")
        
# Score Distribution
fig = px.histogram(df, x='Sentiment_Score', title='Score Distribution')
st.plotly_chart(fig,use_container_width=True)

# Data Preview
with st.expander("Data Preview"):    
    st.dataframe(df.style.highlight_max(axis=0))

