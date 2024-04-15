from kafka import KafkaProducer

# Define Kafka producer with connection details
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to send data to a specific Kafka topic
def send_data_to_kafka(topic_name, data):
  producer.send(topic_name, data.encode('utf-8'))

# Example usage: sending impression data
impression_data = {'ad_id': 123, 'user_id': 'abc', 'timestamp': '2024-04-16'}
send_data_to_kafka('ad_impressions', json.dumps(impression_data))

producer.flush()  # Flush data to Kafka