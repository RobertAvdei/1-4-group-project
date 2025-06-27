from kafka import KafkaConsumer, TopicPartition
import json

def set_consumer(topic):
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset="earliest", enable_auto_commit=True, value_deserializer=lambda x: json.loads(x.decode("utf-8")), consumer_timeout_ms=600,)
    consumer.subscribe(topic)
    partition = TopicPartition(topic, 0)
    end_offset = consumer.end_offsets([partition])
    consumer.seek(partition,list(end_offset.values())[0]-1)

    for m in consumer:
      print(m.value)