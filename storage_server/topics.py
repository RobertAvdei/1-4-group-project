from kafka.admin import KafkaAdminClient, NewTopic
from constants import PRESET_TOPICS

    
def verify_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id='test_admin'
    )
    print('Checking topics...')
    existing_topics_info = admin_client.describe_topics()
    existing_topics = []
    existing_topics.extend(
        topic_info['topic'] for topic_info in existing_topics_info
    )
    print('Existing topics: ',existing_topics)

    missing_topics = []
    missing_topics.extend(
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in PRESET_TOPICS
        if topic not in existing_topics
    )
    
    if missing_topics:
        print('Generating missing topics')
        admin_client.create_topics(new_topics=missing_topics, validate_only=False)
    print('Topics: DONE')
        
if __name__ == "__main__":
    verify_topics()