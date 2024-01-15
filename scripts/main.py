from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import (StringType, StructField, StructType,
                               TimestampType)


def configure_spark():
    spark = SparkSession.builder \
        .appName("Spark Streaming Kafka Integration") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def main():
    spark = configure_spark()

    # Define o endereço do servidor Kafka
    kafka_bootstrap_servers = 'kafka:29092'
    kafka_topic = 'meu-topico'

    # Define o esquema dos dados baseado na estrutura enviada pelo producer
    schema = StructType([
        StructField("id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("event_type", StringType()),
    ])

    # Cria um DataFrame representando o stream do Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Feature 1: Filtrar mensagens pelo tipo de evento 'click'
    df_filtered = df.filter(df.event_type == 'click')

    # Feature 2: Agregação com função de janela
    # Exemplo: Contagem de eventos 'click' a cada intervalo de tempo
    windowedCounts = df_filtered \
        .groupBy(
            window(df_filtered.timestamp, "5 minutes", "5 minute"),
            df_filtered.event_type
        ).count()

    # Ordena as janelas pelo início da janela
    windowedCounts = windowedCounts \
        .orderBy("window.start")

    # Exibir os resultados no console
    query = windowedCounts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
