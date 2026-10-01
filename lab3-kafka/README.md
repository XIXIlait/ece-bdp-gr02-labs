# Lab 3: Kafka

A producer reads a book from Project Gutenberg line by line and sends each line to a Kafka topic. A consumer reads the topic, cleans each line and writes the result to a new file.

## Files

| File | Role |
|---|---|
| `admin.py` | Creates the topic `books` |
| `producer.py` | Downloads the book (Around the World in Eighty Days) and sends each line to `books` |
| `consumer.py` | Reads `books`, cleans each line (lower case, punctuation, stop words) and writes `book_cleaned.txt` |
| `book_cleaned.txt` | Output of the consumer |
| `demo/` | Demo files from the course (topic `timer`) |

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker run -d --name kafka -p 9092:9092 apache/kafka-native:4.1.1
python admin.py
```

Terminal 1:

```bash
python consumer.py
```

Terminal 2:

```bash
python producer.py
```

Stop Kafka at the end, and start the same container again next time (`docker run` fails if the container `kafka` already exists):

```bash
docker stop kafka
docker start kafka
```

The producer waits 10 ms between two lines (about 1 min 30 for the whole book), so the lines can be seen arriving in the consumer while the producer is running. The consumer stops after 10 seconds without new messages.

To run the lab again from the start, delete `book_cleaned.txt` and recreate the topic (uncomment `delete_topics` in `admin.py`, run it, comment it again and run it once more).

## Result

- 8,312 lines sent and received
- 6,380 cleaned lines written (empty lines after cleaning are skipped)
- Top words: fogg (601), passepartout (402), mr (389), phileas (255), fix (240)

## Notes

- The book is the same as in the word count lab (lab 1), so the results can be compared: the top words are the same, which shows that the whole book went through Kafka.
- The topic has 1 partition, so the lines are received in the same order as in the book.
- Kafka keeps the messages: a consumer started after the producer still reads the whole book.
- The consumer belongs to the group `books_reader`. Kafka saves its position (offset) in the topic, so a second run only reads the new messages and adds them at the end of `book_cleaned.txt`.
