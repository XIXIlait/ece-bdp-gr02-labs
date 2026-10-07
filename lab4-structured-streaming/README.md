# Lab 4: Structured Streaming

In this lab we read the edits made on Wikipedia in real time. A Python producer sends every edit to Kafka, and Spark Structured Streaming reads the Kafka topic and computes statistics on time windows (number of edits, bots vs humans, size of the edits).

## Files

| File | What it does |
|---|---|
| `compose.yaml` | Starts Kafka and Jupyter (with Spark) together with Docker Compose |
| `admin.py` | Creates the topic `wikistreams` |
| `wikistream_producer.py` | Producer from the course: edits from `fr.wikipedia.org` for 10 minutes |
| `wikistream_producer_filtered.py` | Our producer with filters: language, type of page and duration can be changed |
| `notebooks/wikistream_pyspark.ipynb` | Demo from the course: edits per hour, bots vs humans |
| `notebooks/wikistream_explorations.ipynb` | Our explorations (executed, also exported in HTML) |
| `data/`, `jobs/` | Folders asked in the instructions (empty) |

## How to run it

1. Start Kafka and Jupyter:

```bash
docker compose up -d
docker logs pyspark_notebook
```

The second command gives the link to open Jupyter.

2. Create a Python environment for the producers and create the topic:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python admin.py
```

3. Send Wikipedia edits to Kafka (each producer runs for 10 minutes, they can run at the same time in 2 terminals):

```bash
export PYWIKIBOT_NO_USER_CONFIG=1
python wikistream_producer.py
python wikistream_producer_filtered.py
```

Our producer follows English Wikipedia articles by default. Other filters can be used, for example German Wikipedia for 5 minutes:

```bash
python wikistream_producer_filtered.py --wiki de.wikipedia.org --minutes 5
```

4. In Jupyter, open `work/wikistream_explorations.ipynb` and run all the cells. The demo `work/wikistream_pyspark.ipynb` prints its results in the logs:

```bash
docker logs -f pyspark_notebook
```

5. Stop everything at the end:

```bash
docker compose down
```

## What we had to change to make it work

- `compose.yaml`: we added `PYTHONPATH`, otherwise `pyspark` is not found in Jupyter (same problem as in lab 2).
- Demo notebook: the Kafka connector version is `4.2.0` instead of `4.1.0`, to match the Spark version of the image (4.2.0).
- `pywikibot` needs the package `requests-sse` to read the stream, it is in `requirements.txt`.
- `PYWIKIBOT_NO_USER_CONFIG=1` avoids the error about the missing `user-config.py` of pywikibot.

## What we did

- **Tumbling windows** (5 minutes, no overlap): number of edits and average size change per window, bots vs humans.
- **Overlapping windows** (10 minutes, a new one every 5 minutes): bytes added and removed. Each edit is counted in 2 windows, so the curve is smoother.
- **Size of the edits**: additions vs removals, and the pages that changed the most.
- **Most active users**: bots vs humans.
- **Filters in the producer**: language (`--wiki`), type of page (`--namespace`, 0 = articles) and duration (`--minutes`).

## Results

- 8,925 edits in total: 8,447 French edits (from the stream history, 30 September) and 478 live English article edits.
- Bots make 17.7 % of the French edits but only 3.6 % of the English article edits.
- Most edits add content: 71 % in French, 74 % in English.
- The biggest change is one edit that removed 106,471 bytes from the English article "Textile industry in Bangladesh".
- The 3 most active French accounts are bots (OrlodrimBot, CodexBot, CodexBot2).

## Note

The producer from the course uses `since='20260209'`, so Wikipedia first sends the history it still has (about one week) instead of the live edits. This is why the French edits are from 30 September. Our filtered producer has no `since`, so it reads the live edits.
