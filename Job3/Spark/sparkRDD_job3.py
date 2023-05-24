#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import time

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Affinity Group").getOrCreate()

# Avvia il timer
start_time = time.time()

# read the input file and obtain an RDD with a record for each line
rdd = spark.sparkContext.textFile(input_filepath)
header = rdd.first()
removeHeaderRDD = rdd.filter(lambda row: row != header)

mapped_data = removeHeaderRDD.map(lambda line: line.strip().split("\t"))

# Filter scores >= 4
filtered_data = mapped_data.filter(lambda x: int(x[2]) >= 4)

grouped_data = filtered_data.map(lambda x: (x[1], x[0])). \
    groupByKey(). \
    mapValues(lambda x: set(x))

# Filter users with at least 3 products
filtered_users = grouped_data.filter(lambda x: len(x[1]) >= 3)

affinity_groups = []

# Trova gli utenti con 3 prodotti in comune
for i, user1 in enumerate(filtered_users.collect()):
    group = {user1[0]}
    commonProducts = user1[1]

    for user2 in filtered_users.collect()[i + 1:]:
        if len(commonProducts.intersection(user2[1])) >= 3:
            group.add(user2[0])
            commonProducts = commonProducts.intersection(user2[1])

    if len(group) > 1:
        affinity_groups.append((sorted(list(group)), list(commonProducts)))


# Sort groups by the first userId
sorted_groups = sorted(affinity_groups, key=lambda x: x[0][0])

# Remove duplicate groups
processed_users = set()
unique_groups = []
for group in sorted_groups:
    if not any(user in processed_users for user in group[0]):
        unique_groups.append(group)
        processed_users.update(group[0])

# Salva i risultati nel file di output
unique_groups_RDD = spark.sparkContext.parallelize(unique_groups)
unique_groups_RDD.map(lambda x: f"Group: {', '.join(x[0])}\nCommon Products: {', '.join(x[1])}"). \
    saveAsTextFile(output_filepath)

# Calcola il tempo di esecuzione
execution_time = time.time() - start_time
print("Tempo di esecuzione: %.2f secondi" % execution_time)

# stop the SparkSession
spark.stop()