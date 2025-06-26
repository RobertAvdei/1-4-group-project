import json
from kafka import KafkaProducer
import pandas as pd
import csv

path = "C:\\School\\Year 1\\Block 4\\Group Project\\data\\test\\test_patients.csv"
# csv_file = pd.read_csv(path)
# print(csv_file.to_string())
csv_string = ''
with open(path, 'r') as file:
    csv_string = file.read()
print(csv_string)

print("About to connect")
# producer = KafkaProducer(bootstrap_servers='localhost:9092',
#                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers='localhost:9092')
print('Connected')
# producer.send('patients', [{'Patient_id': '5'},{'Patient_id': '6'}])
# producer.send('patients',csv_file.to_string().encode("utf-8"))
producer.send('patients',csv_string.encode("utf-8"))
producer.flush()
producer.close()
