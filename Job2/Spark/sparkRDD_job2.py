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
spark = SparkSession.builder.appName("User Appreciation").getOrCreate()

# Avvia il timer
start_time = time.time()

# Lettura file di input + RDD con un record per ciascuna linea
rdd = spark.sparkContext.textFile(input_filepath)
header = rdd.first()
removeHeaderRDD = rdd.filter(lambda row: row != header)

# RDD (user, (HelpfulnessNumerator, HelpfulnessDenominator))
user_helpfulness_RDD = removeHeaderRDD.map(lambda line: line.strip().split("\t")) \
    .map(lambda user_helpfulness: (user_helpfulness[0], (int(user_helpfulness[1]), int(user_helpfulness[2]))))

# RDD (user, (utility, num_reviews))
user_utility_reviews_RDD = user_helpfulness_RDD.mapValues(lambda x: (x[0] / x[1] if x[1] != 0 else 0, 1))

# RDD (user, (total_utility, total_reviews))
user_total_utility_reviews_RDD = user_utility_reviews_RDD.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# RDD (user, appreciation)
user_appreciation_RDD = user_total_utility_reviews_RDD.mapValues(lambda x: x[0] / x[1])

# RDD (user, appreciation) sorted by appreciation
sorted_user_appreciation_RDD = user_appreciation_RDD.sortBy(lambda x: (x[1], x[0]), ascending=False)

sorted_user_appreciation_RDD.saveAsTextFile(output_filepath)

# Calcola il tempo di esecuzione
execution_time = time.time() - start_time
print("Tempo di esecuzione: %.2f secondi" % execution_time)

# stop the SparkSession
spark.stop()
