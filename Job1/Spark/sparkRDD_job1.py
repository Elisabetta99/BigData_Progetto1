#!/usr/bin/env python3
"""spark application"""

import argparse
import string
from datetime import datetime
import re
from functools import reduce
from pyspark.sql import SparkSession
import time

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("sparkjob1") \
    .getOrCreate()



startTime = time.time()

# read the input file into an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath)

#header = lines_RDD.first()
#removeHeaderRDD = lines_RDD.filter(lambda row: row != header)

time_text_RDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

# RDD (year, words), Unix time to year
#year_cleaned_text_RDD = time_text_RDD.map(f=lambda year_productid_text: (datetime.utcfromtimestamp(int(year_productid_text[0])).strftime('%Y'), year_productid_text[1], year_productid_text[2]))

year_cleaned_text_RDD = time_text_RDD.map(f=lambda year_productid_text: (year_productid_text[0], year_productid_text[1], year_productid_text[2]))
# mi creo ((year, product), recensione)
year_product_rec = year_cleaned_text_RDD.map(lambda row: ((row[0], row[1]), row[2]))

#year_product_rec_sum_RDD =year_product_rec.groupByKey()

year_product_RDD  = year_cleaned_text_RDD.map(lambda row: ((row[0], row[1]), 1))
# RDD ((year, word), sum(1))
year_product_sum_RDD = year_product_RDD.reduceByKey(func=lambda a, b: a + b)

year_product_sum_ordered_RDD = year_product_sum_RDD.sortBy(lambda x: (x[0][0], x[1]), ascending=False)

# ordered RDD (year, (word, sum(1)))
output_RDD = year_product_sum_ordered_RDD.map(f=lambda x : (x[0][0], (x[0][1], x[1])))

output_year_list_product_RDD = output_RDD.groupByKey()

#FINO A QUI TOP 10 PRODOTTI
output_year_top10_product_RDD = output_year_list_product_RDD.map(f=lambda x : (x[0], list(x[1])[:10])).sortByKey(False)

#mi levo il valore e mi creo un file con (anno, prodotto) della top 10 per poi confrontarlo con quello delle recensioni
selected_year_product_RDD = output_year_top10_product_RDD.flatMap(lambda x: [(x[0], p) for p, _ in x[1]])


def print_parole(year_product_text):
    year_product = year_product_text[0]  # (anno, prodotto)
    words = year_product_text[1].split()  # ["parola1", "parola2"]
    out = []  # lista di coppie ((anno, prodotto), parola)
    for word in words:
        if len(word) >= 4:
            out.append((year_product, word))
    
    return out

list_word = year_product_rec.flatMap(print_parole)

list_word_counter = spark.sparkContext.parallelize(list(list_word.countByValue().items()))

word_count = list_word_counter.map(lambda x: (x[0][0],(x[0][1],x[1])))

final_out = word_count.groupByKey().mapValues(lambda x: sorted(x, key=lambda y: y[1], reverse=True)[:5])

joinRDD = selected_year_product_RDD.map(lambda x: ((x[0],x[1]), 1)).join(final_out.map(lambda x: ((x[0][0],x[0][1]),x[1])))
final_job = joinRDD.map(lambda x: ((x[0][0], x[0][1]), x[1][1]))

endTime = time.time()

#final_job.saveAsTextFile(output_filepath)

print("Time : ", endTime - startTime)

