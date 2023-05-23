#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count
import time

# Creazione parser
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# Argomenti parser
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Inizializzazione della sessione Spark
spark = SparkSession.builder.appName("UserAppreciation").getOrCreate()

# Avvia il timer
start_time = time.time()

# Lettura del file CSV del dataset Amazon Fine Food
reviews_df = spark.read.csv(input_filepath, header=True)

# Calcolo dell'apprezzamento per ogni utente
utility = when(col("HelpfulnessDenominator") != 0, col("HelpfulnessNumerator") / col("HelpfulnessDenominator")).otherwise(0)
user_appreciation_df = reviews_df.groupBy("UserId") \
    .agg((sum(utility) / count("*")).alias("Appreciation"))

# Ordinamento della lista di utenti in base all'apprezzamento
sorted_users = user_appreciation_df.orderBy(["Appreciation", "UserId"], ascending=[False, False])

# Visualizzazione dei risultati
sorted_users.show()

# Salvataggio dei risultati in un file di output
sorted_users.write.csv(output_filepath)

# Calcola il tempo di esecuzione
execution_time = time.time() - start_time
print("Tempo di esecuzione: %.2f secondi" % execution_time)
