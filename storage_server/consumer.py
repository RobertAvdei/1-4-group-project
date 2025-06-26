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
        # print(msg)
        print(msg.topic)
        row = msg.value.decode('utf-8').splitlines()[1]
        print(row)
        # record = json.loads(msg.value)
        # print(record)

    # consumer = KafkaConsumer(
    #     'patients',
    #     auto_offset_reset='earliest',
    #     bootstrap_servers=['localhost:9092'],
    #     api_version=(0, 10),
    #     consumer_timeout_ms=1000
    # )

                # row = (f"('{record['patient_id']}','{record['address']}','{record['city']}',"
                #         f"'{record['date_of_birth']}','{record['first_name']}','{record['gender']}',"
                #         f"'{record['last_name']}','{record['phone_number']}','{record['state']}','{record['zip_code']}')")