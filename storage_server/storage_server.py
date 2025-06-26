from kafka import KafkaConsumer
from topics import verify_topics
from consumer import connect_consumer
from handle_message import handle_message


def run():
    
    verify_topics()
    consumer = connect_consumer()
    for msg in consumer:
        handle_message(msg)
        # record = json.loads(msg.value)
        # employee_id = int(record['id'])
        # name = record['name']

        # if employee_id != 3:
        #     print(f"This employee is not Karen. It's actually {name}")
    
    if consumer is not None:
        consumer.close()
        
if __name__ == '__main__':
    run()