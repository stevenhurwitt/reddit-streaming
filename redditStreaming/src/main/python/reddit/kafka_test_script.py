from kafka import KafkaProducer
import json

def test_kafka_connection(host="kafka:9092"):
    """Test Kafka connection and basic producer functionality"""
    try:
        # Create producer with detailed configuration
        producer = KafkaProducer(
            bootstrap_servers=[host],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000,
            request_timeout_ms=30000,
            api_version=(0, 10, 2)
        )
        
        # Send test message
        producer.send('test_topic', {'test': 'message'})
        producer.flush()
        print(f"Successfully sent test message to Kafka at {host}")
        return True
        
    except Exception as e:
        print(f"Kafka connection test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_kafka_connection()