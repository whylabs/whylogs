from kafka import KafkaConsumer, TopicPartition, KafkaProducer
import json

def create_producer():
    return KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def create_consumer(topic):
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092', 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    # Manually assign partitions
    # https://github.com/dpkp/kafka-python/issues/601#issuecomment-331419097
    assignments = []
    partitions = consumer.partitions_for_topic(topic)
    for p in partitions:
        print(f'topic {topic} - partition {p}')
        assignments.append(TopicPartition(topic, p))
    consumer.assign(assignments)

    return consumer

    