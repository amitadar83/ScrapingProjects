from confluent_kafka import Consumer, KafkaError

# Consumer configuration
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance with provided config
consumer = Consumer(config)

# Subscribe to topic
consumer.subscribe(['news_headlines'])

# Process messages
while True:
    msg = consumer.poll(1)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    print(f"Received message: {msg.value().decode('utf-8')}")

consumer.close()
