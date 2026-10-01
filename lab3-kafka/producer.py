import os
import socket
import time
import urllib.request

from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

topic = 'books'

book_url = 'https://www.gutenberg.org/ebooks/103.txt.utf-8'
book_file = 'around_the_world_in_80_days.txt'

if not os.path.exists(book_file):
    urllib.request.urlretrieve(book_url, book_file)

nb_lines = 0
with open(book_file, encoding='utf-8') as f:
    for line in f:
        producer.produce(topic=topic, value=line.rstrip('\n'))
        producer.poll(0)
        nb_lines += 1
        time.sleep(0.01)

producer.flush()
print(f"{nb_lines} lines sent to topic '{topic}'")
