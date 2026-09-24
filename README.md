# ECE Big Data Processing - Labs (gr-02)

- Group: gr-02
- project/lab member 1: Alexis Lainault, git username: XIXIlait
- project/lab member 2: Antoine Appourchaux, git username: Antoine924

Labs for the ECE *Big Data Processing* course, fall 2026 (Adaltas).

## Labs

| Lab | Topic | Notebook |
|---|---|---|
| Lab 1 | Unstructured data analysis with RDDs (word count) | [lab1-rdd-word-count/word_count.ipynb](lab1-rdd-word-count/word_count.ipynb) (executed, [HTML export](lab1-rdd-word-count/word_count.html)) |
| Lab 2 | Structured data analysis with DataFrames and SparkSQL (NYC taxi) | [lab2-sparksql-dataframes/lab_sparksql_and_dataframes.ipynb](lab2-sparksql-dataframes/lab_sparksql_and_dataframes.ipynb) (executed, [HTML export](lab2-sparksql-dataframes/lab_sparksql_and_dataframes.html)) |

## Running the notebooks

The labs run in the [Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/index.html) `pyspark-notebook` image. The repository is mounted in the container so the notebooks are saved straight into the repo.

```bash
./start-jupyter.sh
```

The script starts the container and prints the JupyterLab link. The repo is in the `work/` folder. The Spark UI is available at <http://localhost:4040> while a session is running.

Without Docker (Java 17+ required):

```bash
pip install -r lab1-rdd-word-count/requirements.txt
pip install -r lab2-sparksql-dataframes/requirements.txt
```

Stop the container with:

```bash
docker stop pyspark_notebook
```

Notes:

- `PYTHONPATH` is set because the latest image does not expose `pyspark` to the Jupyter kernel.
- The `CHOWN_EXTRA` options from the course instructions are not used: on macOS they fail on the `.git` folder and the container exits.

The datasets (Project Gutenberg book, NYC TLC trip records) are downloaded by the notebooks themselves and are not committed (see `.gitignore`).
