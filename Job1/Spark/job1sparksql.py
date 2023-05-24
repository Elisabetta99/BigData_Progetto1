#!/usr/bin/env python3
"""job1sparksql.py"""

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, split, explode, count, from_unixtime, year, desc, length, lower, regexp_replace, col

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", help="Output file path")
# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Spark SQL JOB 1") \
    .getOrCreate()

# Define the schema structure
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("productId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("profileName", StringType(), True),
    StructField("helpfulnessNumerator", IntegerType(), True),
    StructField("helpfulnessDenominator", IntegerType(), True),
    StructField("score", StringType(), True),
    StructField("time", IntegerType(), True),
    StructField("summary", StringType(), True),
    StructField("text", StringType(), True)
])


startTime = time.time()

# read from csv file
input_df = spark.read.option("quote", "\"") \
         .csv(input_filepath, header=True, schema=schema) \
         .cache()

#converto il time
input_df = input_df.withColumn("time", input_df["time"].cast("timestamp"))
input_df = input_df.withColumn("year", year(from_unixtime(input_df["time"].cast("bigint")))).cache()


# TOP10 PER OGNI ANNO
count_reviews = input_df.groupBy("year", "productId").agg(count("*").alias("conteggio_recensioni")).cache()

window = Window.partitionBy("year").orderBy(desc("conteggio_recensioni"))

top10prod = count_reviews.withColumn("row_number", row_number().over(window))

top10prod = top10prod.filter(top10prod["row_number"] <= 10).drop("row_number").cache()

# top 5 words per ogni prodotto di ogni anno. faccio lo split di ogni parola nelle recensioni
searchWords = input_df.withColumn("words", explode(split(input_df["text"], " "))).cache()

searchWords = searchWords.filter(length(searchWords["words"]) > 3).groupBy("year", "productId", "words").agg(count("*").alias("conteggio_parole")).cache()

window = Window.partitionBy("year", "productId").orderBy(desc("conteggio_parole"))

topWords = searchWords.withColumn("row_number", row_number().over(window))
topWords = topWords.filter(topWords["row_number"] <= 5).drop("row_number").cache()

#unisco le info
outputJob = top10prod.join(topWords, ["year", "productId"]).cache()

endTime = time.time()

outputJob.write.csv(output_filepath, mode = "overwrite")
#visualizzo il tempo impiegato 
print("Time : ", endTime - startTime)

