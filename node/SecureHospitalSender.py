import json
from pathlib import Path
import socket
from confluent_kafka import Producer as ConfluentProducer
from kafka import KafkaProducer as KafkaPythonProducer

class SecureHospitalSender: #The class can use either Confluent's Kafka producer or kafka-python producer but defaults to using Confluent's producer
    def __init__(self, use_confluent=True):
        self.use_confluent = use_confluent

        cert_path = Path('certs')
        ca_file = cert_path / 'ca.pem'
        cert_file = cert_path / 'service.cert'
        key_file = cert_path / 'service.key'

        if use_confluent:
            print("Using Confluent Kafka Producer")
            conf = {
                'bootstrap.servers': 'localhost:9092',
                'client.id': socket.gethostname(),
                'security.protocol': 'PLAINTEXT'  
            }
            self.producer = ConfluentProducer(conf)
            self.producer_type = 'confluent'

        else: #If SSL certs exist, creates a secure producer connecting to port 9093
            print("Using kafka-python KafkaProducer")
            if ca_file.exists() and cert_file.exists() and key_file.exists():
                print("SSL certificates found. Using secure kafka-python KafkaProducer")
                self.producer = KafkaPythonProducer(
                    bootstrap_servers='localhost:9093',
                    security_protocol='SSL',
                    ssl_cafile=str(ca_file),
                    ssl_certfile=str(cert_file),
                    ssl_keyfile=str(key_file),
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
            else:
                print("SSL certificates NOT found. Using plaintext kafka-python KafkaProducer")
                self.producer = KafkaPythonProducer(
                    bootstrap_servers='localhost:9092',
                    security_protocol='PLAINTEXT',
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
            self.producer_type = 'kafkapython'

    def _send_to_kafka(self, data, topic):
        try:
            if self.producer_type == 'confluent':
                self.producer.produce(topic, value=json.dumps(data).encode('utf-8'))
                self.producer.flush()
                print(f"Sent data to topic: {topic} using Confluent Kafka")

            elif self.producer_type == 'kafkapython':
                self.producer.send(topic, data)
                self.producer.flush()
                print(f"Sent data to topic: {topic} using kafka-python")

        except Exception as e:
            print(f"Failed to send: {e}")

    def send_json(self, file_path):
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        topic = p.stem  # filename without extension as topic name
        with p.open() as f:
            raw_data = json.load(f)
            self._send_to_kafka(raw_data, topic)

    def send_csv(self, file_path):
        p = Path(file_path)
        if not p.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        topic = p.stem
        with p.open() as f:
            raw_data = f.read()
            self._send_to_kafka(raw_data, topic)

    def send_sqlite(self, db_path, table='encounters'):
        import sqlite3
        p = Path(db_path)
        if not p.exists():
            raise FileNotFoundError(f"SQLite DB not found: {db_path}")

        topic = table
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table}")
            data = [dict(row) for row in cursor.fetchall()]
            self._send_to_kafka(data, topic)
        finally:
            conn.close()


### TESTING
if __name__ == "__main__":
    sender = SecureHospitalSender(use_confluent=True)

# Example: send a JSON file (pls make sure the file exists in the path)
sender.send_json("C:\\Users\\haile\\Desktop\\Adsai\\First Year\\Block 4\\Intorduction to Data Engineering\\Intro to Data Engineering Project\\medications.json")

# Or send CSV file
sender.send_csv("C:\\Users\\haile\\Desktop\\Adsai\\First Year\\Block 4\\Intorduction to Data Engineering\\Intro to Data Engineering Project\\observations.csv")

# Or send SQLite database table data
sender.send_sqlite("C:\\Users\\haile\\Desktop\\Adsai\\First Year\\Block 4\\Intorduction to Data Engineering\\Intro to Data Engineering Project\\ehr_journeys_database.sqlite", table='encounters')