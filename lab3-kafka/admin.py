from confluent_kafka.admin import AdminClient, NewTopic

config = {
    'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

topic = 'books'
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

for name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{name}' created")
    except Exception as e:
        print(f"Topic '{name}' not created: {e}")

for t in admin_client.list_topics().topics.keys():
    print(t)

# admin_client.delete_topics([topic])
