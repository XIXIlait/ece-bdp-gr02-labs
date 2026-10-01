import re
from collections import Counter

from confluent_kafka import Consumer

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'books_reader',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

topic = 'books'
consumer.subscribe([topic])

output_file = 'book_cleaned.txt'

stop_words = {
    "a", "about", "after", "again", "all", "am", "an", "and", "any", "are",
    "as", "at", "be", "been", "before", "but", "by", "can", "could", "did",
    "do", "does", "for", "from", "had", "has", "have", "he", "her", "here",
    "him", "his", "how", "i", "if", "in", "into", "is", "it", "its", "me",
    "more", "my", "no", "not", "now", "of", "on", "one", "only", "or",
    "other", "our", "out", "over", "said", "she", "should", "so", "some",
    "than", "that", "the", "their", "them", "then", "there", "these",
    "they", "this", "those", "to", "too", "up", "upon", "very", "was", "we",
    "were", "what", "when", "where", "which", "while", "who", "whom", "why",
    "will", "with", "would", "you", "your"
}


def clean_line(line):
    words = [re.sub(r"[^\w]", "", word.lower()) for word in line.split()]
    return [w for w in words if w != "" and w not in stop_words]


MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0

nb_messages = 0
nb_lines_written = 0
word_counts = Counter()

with open(output_file, 'a', encoding='utf-8') as out:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        empty_polls = 0
        error_count = 0
        nb_messages += 1

        words = clean_line(msg.value().decode('utf-8'))
        if words:
            out.write(" ".join(words) + "\n")
            nb_lines_written += 1
            word_counts.update(words)

consumer.close()

print(f"{nb_messages} messages received")
print(f"{nb_lines_written} cleaned lines written to {output_file}")
print("Top 10 words:", word_counts.most_common(10))
