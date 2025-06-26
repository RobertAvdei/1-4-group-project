from kafka import KafkaConsumer
from constants import PRESET_TOPICS

    
def connect_consumer(topics = PRESET_TOPICS):
    print('Connecting consumer')
    consumer = KafkaConsumer()
    consumer.subscribe(topics)
    print('Consumer Connected')
    return consumer

if __name__ == "__main__":
    consumer = connect_consumer()
    
    for msg in consumer:
        print(msg.topic)
        row = msg.value.decode('utf-8').splitlines()[1]
        print(row)

