#!/usr/bin/env python3
"""spark application"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import time

# Creazione del parser
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parsing degli argomenti
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Inizializzazione
spark = SparkSession.builder.appName("UserAppreciation").getOrCreate()

# Lettura del file di input e ottenimento di un RDD
rdd = spark.sparkContext.textFile(input_filepath)

# Rimozione dell'intestazione e filtraggio delle righe
header = rdd.first()
removedHeaderRDD = rdd.filter(lambda row: row != header)
filteredRDD = removedHeaderRDD.filter(lambda line: len(line.strip().split("\t")) >= 6)

# Calcolo dell'apprezzamento per ogni utente
utilityRDD = filteredRDD.map(lambda line: (line.strip().split("\t")[2], float(line.strip().split("\t")[4]) / float(line.strip().split("\t")[5]) if float(line.strip().split("\t")[5]) != 0 else 0))


# Calcola la somma dei rapporti e il conteggio delle recensioni per ogni utente
userSumCountRDD = utilityRDD.aggregateByKey((0, 0),
                                            lambda acc, val: (acc[0] + val, acc[1] + 1),
                                            lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))

# Calcola la media dell'apprezzamento per ogni utente
userAppreciationRDD = userSumCountRDD.mapValues(lambda acc: acc[0] / acc[1])

# Ordina la lista di utenti in base all'apprezzamento
sortedUsersRDD = userAppreciationRDD.sortBy(lambda x: x[1], ascending=False)


# Misurazione del tempo di esecuzione e stampa dei risultati
start_time = time.time()

# Stampa dei risultati nel terminale
sorted_users = sortedUsersRDD.collect()
for user in sorted_users:
    print(user)

# Calcola il tempo di esecuzione
end_time = time.time()
print("Total execution time: {} seconds".format(end_time - start_time))
print("End")

# Salvataggio dei risultati in un file di output
sortedUsersRDD.saveAsTextFile(output_filepath)

