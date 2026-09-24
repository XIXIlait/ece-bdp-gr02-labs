#!/usr/bin/env bash
# Start the PySpark Jupyter container with this repo mounted in /home/jovyan/work
# and print the JupyterLab link. Stop it with: docker stop pyspark_notebook
set -e
cd "$(dirname "$0")"

if docker ps --format '{{.Names}}' | grep -qx pyspark_notebook; then
  echo "Container already running."
else
  # PYTHONPATH: the latest image does not expose pyspark to the Jupyter kernel
  docker run --name pyspark_notebook --rm \
    -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.9-src.zip \
    -v "$(pwd)":/home/jovyan/work \
    --detach \
    -p 8888:8888 -p 4040:4040 -p 4041:4041 \
    quay.io/jupyter/pyspark-notebook > /dev/null
  until docker logs pyspark_notebook 2>&1 | grep -q "token="; do sleep 1; done
fi

docker logs pyspark_notebook 2>&1 | grep -m1 -o "http://127.0.0.1:8888/lab?token=[a-f0-9]*"
