#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
import time

# Creazione parser
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# Argomenti parser
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Inizializzazione SparkSession
spark = SparkSession.builder.appName("Affinity Group").getOrCreate()

# Avvia il timer
start_time = time.time()

# Lettura file di input + RDD con un record per ciascuna linea
rdd = spark.sparkContext.textFile(input_filepath)
header = rdd.first()
removeHeaderRDD = rdd.filter(lambda row: row != header)

# Mapper: Filtra prodotti con score >= 4
filtered_data = removeHeaderRDD.map(lambda line: line.split('\t')) \
                    .filter(lambda columns: int(columns[6]) >= 4) \
                    .map(lambda columns: (columns[2], columns[1]))

# Reducer: Trova gli utenti con 3 prodotti in comune
grouped_users = filtered_data.groupByKey() \
                            .filter(lambda x: len(x[1]) >= 3) \
                            .flatMap(lambda x: [(user1, user2) for user1 in x[1] for user2 in x[1] if user1 != user2]) \
                            .distinct()

# Ordina gli affinity group in base all'UserId del primo elemento del gruppo
sorted_groups = grouped_users.map(lambda x: (x[0], x[1])).sortByKey()

# Rimuovi i duplicati dai gruppi
unique_groups = sorted_groups.map(lambda x: (x[0], x[1])).distinct()

# Stampa il risultato
unique_groups.foreach(lambda x: print(x[0], x[1]))

# Chiudi la sessione di Spark
spark.stop()

# Calcola il tempo di esecuzione
execution_time = time.time() - start_time
print("Tempo di esecuzione:", execution_time)
