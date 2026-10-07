import argparse
import json
import socket
from datetime import datetime, timedelta

from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams

parser = argparse.ArgumentParser()
parser.add_argument('--wiki', default='en.wikipedia.org')
parser.add_argument('--namespace', type=int, default=0)
parser.add_argument('--minutes', type=int, default=10)
parser.add_argument('--topic', default='wikistreams')
args = parser.parse_args()

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'compression.type': 'lz4'}

producer = Producer(conf)

stream = EventStreams(streams=['recentchange'])
stream.register_filter(server_name=args.wiki, type='edit',
                       namespace=args.namespace)

stop_time = datetime.now() + timedelta(minutes=args.minutes)
nb_events = 0

while datetime.now() < stop_time:
    change = next(stream)
    producer.produce(topic=args.topic,
                     value=json.dumps(change).encode('utf-8'))
    producer.poll(0)
    nb_events += 1
    if nb_events % 100 == 0:
        print(f"{nb_events} events sent, last: {change['title']}")

producer.flush()
print(f"{nb_events} events from {args.wiki} sent to topic '{args.topic}'")
