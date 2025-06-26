from kafka import KafkaConsumer
from topics import verify_topics
from consumer import connect_consumer
from handle_message import handle_message


def run():
    
    verify_topics()
    consumer = connect_consumer()
    for msg in consumer:
        handle_message(msg)
    
    if consumer is not None:
        consumer.close()
        
if __name__ == '__main__':
    run()