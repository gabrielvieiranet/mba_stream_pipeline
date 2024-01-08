from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from spark_utils import initialize_spark_session, read_csv


def apply_rows_based_window_function(df):
    # Define uma janela baseada em número de linhas
    windowSpec = Window.partitionBy("contributor_id").orderBy(
        "submitted").rowsBetween(-15, 15)

    # Conta o número de receitas enviadas em cada janela
    return df.withColumn("recipes_count", count("id").over(windowSpec))


def main():
    spark = initialize_spark_session("CSV_to_Parquet_with_Window_Function")

    csv_file_path = "/opt/bitnami/spark/data/dataset/RAW_recipes.csv"
    df = read_csv(spark, csv_file_path)

    print("Quantidade de registros antes do filtro:", df.count())

    # Filtra as receitas com 5 ou mais passos (n_steps) e com data de envio
    df_filtered = df \
        .filter(col("n_steps") >= 5) \
        .filter(col("submitted").isNotNull())

    print("Quantidade de registros depois do filtro:", df.count())

    # Aplica a função Window com base no tempo
    df_with_window = apply_rows_based_window_function(df_filtered)

    # Mostra o resultado
    df_with_window.select("contributor_id", "submitted",
                          "recipes_count").show()

    # Define o caminho de saída para o arquivo Parquet no container
    parquet_output_path = "/opt/bitnami/spark/data/output/PP_recipes_output"

    # Escreve os dados no formato Parquet
    df_with_window.write.mode('overwrite').parquet(parquet_output_path)

    # Encerra a sessão Spark
    spark.stop()


if __name__ == "__main__":
    main()
