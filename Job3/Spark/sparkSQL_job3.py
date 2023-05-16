#!/usr/bin/env python3
"""spark application"""

import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size, concat_ws

# Creazione parser
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# Argomenti parser
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Creazione della sessione Spark
spark = SparkSession.builder.appName("Affinity Group").getOrCreate()

# Avvia il timer
start_time = time.time()

# Lettura del dataset
df = spark.read.csv(input_filepath, header=True)

# Filtraggio dei prodotti con score >= 4
filtered_df = df.filter(col("Score") >= 4)
print("Fine filtraggio dei prodotti con score >= 4")

# Raggruppamento dei prodotti per utente e collezione dei prodotti recensiti
user_products_df = filtered_df.groupBy("UserId").agg(collect_set("ProductId").alias("products"))

# Filtraggio degli utenti con almeno 3 prodotti
filtered_users_df = user_products_df.filter(size(col("products")) >= 3)
print("Fine filtraggio degli utenti con almeno 3 prodotti")

# Creazione di una vista temporanea per eseguire query SQL
filtered_users_df.createOrReplaceTempView("filtered_users")

# Trovare utenti con almeno 3 prodotti in comune
similar_users_df = spark.sql("""
    SELECT up1.userid, up2.userid AS similar_userid, array_intersect(up1.products, up2.products) AS common_products
    FROM filtered_users up1
    JOIN filtered_users up2 ON up1.userid < up2.userid
    WHERE size(array_intersect(up1.products, up2.products)) >= 3
""")
print("Trovati utenti con almeno 3 prodotti in comune")

# Generazione dei gruppi di utenti con gusti affini
groups_df = similar_users_df.groupBy("common_products").agg(collect_set("UserId").alias("users"))
print("Generati gruppi di utenti")

# Convertire la colonna common_products in una stringa
groups_df = groups_df.withColumn("common_products_str", concat_ws(",", col("common_products")))

# Convertire la colonna users in una stringa
groups_df = groups_df.withColumn("users_str", concat_ws(",", col("users")))

# Rimozione dei duplicati all'interno di ciascun gruppo
distinct_groups_df = groups_df.dropDuplicates()
print("Fine rimozione dei duplicati")

# Ordinamento del risultato in base al primo userid nel gruppo
sorted_groups_df = distinct_groups_df.orderBy(col("users").getItem(1))
print("Fine ordinamento risultato")

print("Inizio salvataggio risultati")

# Salvataggio dei risultati in un file di output nel formato CSV
sorted_groups_df.select("common_products_str", "users").write.csv(output_filepath)

print("Fine salvataggio risultati")

# Calcola il tempo di esecuzione
execution_time = time.time() - start_time
print("Tempo di esecuzione: %.2f secondi" % execution_time)

# Termina la sessione Spark
spark.stop()
