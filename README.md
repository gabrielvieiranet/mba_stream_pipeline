# MBA FIAP - Engenharia de Dados

## Stream Processing Pipelines

Trabalho para a disciplina de Stream Processing Pipelines

Objetivo: Construir um stream pipeline utilizando uma ferramenta ou plataforma.

Realizar a ingestão dos dados e o output dele em outro formato (Ex: JSON -> Parquet)

Dataset: livre

Tecnologia escolhida: Apache Spark Streaming

## Instruções

Instalar o Docker no ambiente e rodar o comando abaixo:

```
docker compose up -d
```

Pode ser que demore um pouco para o producer kafka conectar. Cheque o log do container com o comando ```docker logs CONTAINER_ID```

Depois rode este script Shell para iniciar o streaming pelo Spark:

```
./run.sh
```
