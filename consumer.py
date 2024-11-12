from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    topic='cc_retail_data',  
    bootstrap_servers='localhost:9094',
    group_id='cc_retail_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")
