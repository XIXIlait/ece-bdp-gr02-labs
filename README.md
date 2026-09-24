# ECE Big Data Processing — Labs (gr-02)

- Group: gr-02
- project/lab member 1: Alexis Lainault, git username: XIXIlait
- project/lab member 2: Antoine Appourchaux, git username: Antoine924

Labs for the ECE *Big Data Processing* course, fall 2026 (Adaltas).

## Labs

| Lab | Topic | Notebook |
|---|---|---|
| Lab 1 | Unstructured data analysis with RDDs (word count) | [lab1-rdd-word-count/word_count.ipynb](lab1-rdd-word-count/word_count.ipynb) |
| Lab 2 | Structured data analysis with DataFrames and SparkSQL (NYC taxi) | [lab2-sparksql-dataframes/lab_sparksql_and_dataframes.ipynb](lab2-sparksql-dataframes/lab_sparksql_and_dataframes.ipynb) |

## Running the notebooks

The labs run in the [Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/index.html) `pyspark-notebook` image. The repository is mounted in the container so the notebooks are saved straight into the repo.

```bash
docker run --name pyspark_notebook --rm \
  --user root \
  -e NB_UID="$(id -u)" \
  -e NB_GID="$(id -g)" \
  -e CHOWN_EXTRA="/home/jovyan/work" \
  -e CHOWN_EXTRA_OPTS="-R" \
  -v "$(pwd)":/home/jovyan/work \
  --detach \
  -p 8888:8888 -p 4040:4040 -p 4041:4041 \
  quay.io/jupyter/pyspark-notebook
```

Get the JupyterLab link with its token:

```bash
docker logs pyspark_notebook 2>&1 | grep "token="
```

The Spark UI is available at <http://localhost:4040> while a session is running.

The datasets (Project Gutenberg book, NYC TLC trip records) are downloaded by the notebooks themselves and are not committed (see `.gitignore`).
