#!/bin/bash

# Nome do container do Spark
SPARK_CONTAINER_NAME="mba_stream_pipeline-spark-master-1"

# Caminho do script Python no container
PYTHON_SCRIPT_PATH="/opt/bitnami/spark/data/csv_to_parquet.py"

# Executando o script Python no container Spark
docker exec -it $SPARK_CONTAINER_NAME /opt/bitnami/spark/bin/spark-submit $PYTHON_SCRIPT_PATH
