from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id='test_admin'
)

# topic_list = []
# topic_list.append(NewTopic(name="test_3", num_partitions=1, replication_factor=1))
# admin_client.create_topics(new_topics=topic_list, validate_only=False)

existing_topics = admin_client.describe_topics()

PRESET_TOPICS = ['patients','encounters','diagnoses','observations','medications']

for topic_info in existing_topics:
    topic = topic_info['topic']
    print(topic) 
    # if topic is not 'patients':
    #     topic_list = []
    #     topic_list.append(NewTopic(name="patients", num_partitions=1, replication_factor=1))
    #     admin_client.create_topics(new_topics=topic_list, validate_only=False)