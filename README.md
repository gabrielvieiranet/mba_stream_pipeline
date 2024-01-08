# MBA FIAP - Engenharia de Dados

## Stream Processing Pipelines

Trabalho para a disciplina de Stream Processing Pipelines

Objetivo: Construir um stream pipeline utilizando uma ferramenta ou plataforma.
Realizar a ingestão dos dados e o output dele em outro formato (Ex: JSON -> Parquet)
Dataset: livre

Tecnologia escolhida: Apache Spark Streaming

## Instruções

Baixe o dataset neste link:

https://www.kaggle.com/datasets/shuyangli94/food-com-recipes-and-user-interactions

Descompactar os arquivos dentro do diretorio data/dataset

Instalar o Docker no ambiente e rodar o comando abaixo para subir o Spark local:

```
docker compose up -d
```
Depois para inicializar o script Python, rode este script Shell:

```
./run_csv_to_parquet.sh
```
