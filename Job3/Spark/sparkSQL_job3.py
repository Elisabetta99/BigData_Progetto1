#!/usr/bin/env python3
"""affinityGroupsSQL.py"""

import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, collect_set, size, explode, array, concat_ws

# Creazione parser
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# Argomenti parser
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Inizializzazione della sessione Spark
spark = SparkSession.builder.appName("Group Affinity").getOrCreate()

# Avvia il timer
start_time = time.time()

# Definizione schema
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("ProductId", StringType(), True),
    StructField("UserId", StringType(), True),
    StructField("ProfileName", StringType(), True),
    StructField("HelpfulnessNumerator", IntegerType(), True),
    StructField("HelpfulnessDenominator", IntegerType(), True),
    StructField("Score", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Summary", StringType(), True),
    StructField("Text", StringType(), True)
])

# Lettura file di input
reviews_df = spark.read.option("quote", "\"").csv(input_filepath, header=False, schema=schema).cache()

# Filtraggio recensioni con Score >=4
filtered_df = reviews_df.select("UserId", "ProductId", "Score").filter(reviews_df["Score"] >= 4)

# Coppie di utenti che hanno recensito lo stesso prodotto
joined_df = reviews_df.alias("df1").join(reviews_df.alias("df2"), ["ProductId"]) \
    .where(col("df1.userId") < col("df2.userId")) \
    .select(col("df1.userId").alias("userId1"), col("df2.userId").alias("userId2"),
            col("df1.productId").alias("productId")) \
    .distinct()

# Coppie di utenti che hanno almeno 3 prodotti in comune
couple_users_products = joined_df.groupBy("userId1", "userId2").agg(collect_set("productId").alias("CommonProducts")) \
    .where(size(col("CommonProducts")) >= 3) \
    .select("userId1", "userId2", "CommonProducts")

# Raggruppamento utenti con prodotti in comune
group_affinity = couple_users_products.withColumn(
    "CommonProducts",
    concat_ws(",", col("CommonProducts"))
).withColumn(
    "UserId",
    explode(array("userId1", "userId2"))
).groupBy("CommonProducts").agg(collect_set("userId").alias("Group"))

# Converti l'array di stringhe "Group" in una stringa separata da virgole
group_affinity = group_affinity.withColumn("Group", concat_ws(",", col("Group")))

# Visualizzazione dei risultati
group_affinity.show()

# Salvataggio dei risultati
group_affinity.write.csv(output_filepath, mode="overwrite")

# Calcola il tempo di esecuzione
execution_time = time.time() - start_time
print("Tempo di esecuzione: %.2f secondi" % execution_time)