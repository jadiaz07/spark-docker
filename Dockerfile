# 1. Usamos una imagen oficial de Spark con Python
FROM jupyter/pyspark-notebook:latest

# 2. Copiamos los archivos al contenedor
COPY eventos.csv.gz /app/eventos.csv.gz
COPY script.py /app/script.py

# 3. Configuramos el directorio de trabajo
WORKDIR /app

# 4. Ejecutamos el script cuando se inicia el contenedor
CMD ["spark-submit", "script.py"]
