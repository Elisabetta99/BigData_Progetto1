#!/usr/bin/env python3
"""spark application"""

import argparse
import itertools
import time
from pyspark.sql import SparkSession


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
    .appName("Similar users taste") \
    .getOrCreate()

startTime = time.time()

lines_RDD = spark.sparkContext.textFile(input_filepath)

userProductScoreRDD = lines_RDD.map(f=lambda line: line.strip().split("\t"))

# Filter score < 4
userProductFilteredScoreRDD = userProductScoreRDD.filter(lambda score: (int(score[2]) >= 4))

# RDD (product, user)
productUserRDD = userProductFilteredScoreRDD.map(f=lambda user_product: (user_product[0], user_product[1]))
# RDD (product, users)
productUsersRDD = productUserRDD.groupByKey()

productUsersOrderedRDD = productUsersRDD.map(f=lambda product_users: (product_users[0], sorted(list(product_users[1]))))

# RDD (product, couple users)
productCoupleUsersRDD = productUsersOrderedRDD.map(f=lambda product_users: (product_users[0], itertools.combinations(product_users[1], 2)))

# RDD (couple user, product)
usersProductRDD = productCoupleUsersRDD.flatMap(f=lambda product_users: [(i, product_users[0]) for i in product_users[1]])

# RDD (couple user, products)
usersProductsRDD = usersProductRDD.groupByKey()

# RDD remove duplicate in products
outputRDD = usersProductsRDD.map(f=lambda x: (x[0], set(x[1])))

# RDD output cleaned and ordered
outputCleanedRDD = outputRDD.filter(lambda users: (len(users[1]) >= 3)).sortByKey()

    
coupleRDD = outputCleanedRDD.map(lambda line: (line[0], itertools.combinations(line[1],3))).flatMap(lambda line: [(line[0], x) for x in line[1]]) 


# get affinity groups
groupAffinityRDD = coupleRDD.map(lambda line: (line[1], [line[0]])) \
  .reduceByKey(lambda a, b: set(a) | set(b)) \
    .map(lambda line: (line[0], set(line[1]))) \
    .sortBy(lambda line: list(line[1])[0], ascending=False)

endTime = time.time()
print("Time : ", endTime - startTime)

groupAffinityRDD.saveAsTextFile(output_filepath)