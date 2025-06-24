from kafka import KafkaConsumer
print('About to connect')
consumer = KafkaConsumer('patients')
print('Consumer Connected')
for msg in consumer:
    print(msg)
    print(msg.value)