# Use uma imagem Python oficial como imagem base
FROM python:3.8-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o script do produtor Kafka e o requirements.txt para o diretório de trabalho
COPY . /app/

# Instala as dependências necessárias
RUN pip install -r requirements.txt

# Define o comando para executar seu script
CMD ["python", "app.py"]
