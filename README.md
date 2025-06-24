# 1-4-group-project
Year 1 Block 4 group project repo



## Scripts

Create Topic 


Add Topic
docker-compose exec kafka kafka-topics.sh --create --topic test-topic --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server localhost:9092


List Topics
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092