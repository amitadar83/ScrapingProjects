import requests
from bs4 import BeautifulSoup
from textblob import TextBlob
import time
import csv
from confluent_kafka import Producer
import hashlib
import json
from confluent_kafka.admin import AdminClient, NewTopic

# Create an instance of the AdminClient class
admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

# Define the topic configurations
topic = NewTopic(
    "news_headlines",
    num_partitions=1,
    replication_factor=1
)

# Create the topic
admin_client.create_topics([topic])

# Initialize Kafka producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()}')


def generate_key(headline):
    return hashlib.md5(headline.encode()).hexdigest()


url = "https://edition.cnn.com/politics"
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}

html_page = requests.get(url, headers=headers)
soup = BeautifulSoup(html_page.content, 'lxml')

latest_headlines = soup.findAll("div", class_="container_lead-plus-headlines__cards-wrapper")[0].find_all("span")

for i in range(0, 9):
    headline = latest_headlines[i].get_text()
    sentiment_score = TextBlob(headline)
    key = generate_key(headline)

    payload = {
        'headline': headline,
        'sentiment_score': str(sentiment_score.sentiment)
    }
    payload_json = json.dumps(payload)
    producer.produce("news_headlines", key=key, value=payload_json, callback=delivery_report)

    # print(headline + ':' + str(sentiment_score.sentiment))
producer.flush()