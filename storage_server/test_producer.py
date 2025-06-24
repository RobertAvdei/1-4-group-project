import json
from kafka import KafkaProducer
print("About to connect")
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print('Connected')
producer.send('patients', [{'Patient_id': '5'},{'Patient_id': '6'}])
producer.flush()
producer.close()
# for _ in range(100):
#     print('sending')
#     producer.send('test-topic',value= b'some_message_bytes',partition=1)