import json
import random
import time
from datetime import datetime, timedelta

from confluent_kafka import KafkaError, Producer


def create_producer(bootstrap_servers):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'kafka-producer'
    }
    return Producer(conf)


def send_event(producer, topic, event):
    try:
        producer.produce(topic, key=str(event['id']), value=json.dumps(
            event), callback=delivery_report)
        producer.flush()
        return True
    except KafkaError as e:
        print(f"Failed to send event: {e}")
        return False


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def generate_event(current_time, id):
    event_types = ['click', 'view', 'like']
    return {
        'id': id,
        'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
        'event_type': random.choice(event_types)
    }


def check_topic_connection(producer, topic):
    print("Checking connection to topic...")
    test_event = generate_event(datetime.now(), 0)
    if send_event(producer, topic, test_event):
        print("Successfully connected to topic.")
    else:
        print("Failed to connect to topic. Retrying...")
        time.sleep(5)
        check_topic_connection(producer, topic)


def main():
    bootstrap_servers = 'kafka:29092'
    topic = 'meu-topico'

    producer = create_producer(bootstrap_servers)
    check_topic_connection(producer, topic)

    current_time = datetime.now()
    event_id = 1

    while True:
        event = generate_event(current_time, event_id)
        send_event(producer, topic, event)

        current_time += timedelta(minutes=1)
        event_id += 1
        time.sleep(0.5)


if __name__ == "__main__":
    main()
