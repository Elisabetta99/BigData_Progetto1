#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession.builder.appName("Affinity Group").getOrCreate()


def filter_products_with_score(record):
    """Filter products with score >= 4"""
    score = int(record[2])
    return score >= 4


def get_user_products(record):
    """Map to (userId, productId)"""
    userId = record[1]
    productId = record[0]
    return (userId, productId)


def filter_users_with_common_products(records):
    """Filter users with at least 3 common products"""
    users = set(records)
    common_users = []

    for user in users:
        common_products = users.intersection(user[1])
        if len(common_products) >= 3:
            common_users.append((user[0], sorted(list(common_products))))

    return common_users


# read the input file and obtain an RDD with a record for each line
rdd = spark.sparkContext.textFile(input_filepath)

# remove csv header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

# split lines and filter products with score >= 4
filtered_rdd = rdd.map(lambda line: line.split("\t")).filter(filter_products_with_score)

# map to (userId, productId)
user_product_rdd = filtered_rdd.map(get_user_products)

# group by userId and collect products
grouped_rdd = user_product_rdd.groupByKey().mapValues(list)

# filter users with at least 3 common products
filtered_users_rdd = grouped_rdd.filter(lambda x: len(x[1]) >= 3).map(lambda x: (x[0], set(x[1])))

# find users with common products
common_users_rdd = filtered_users_rdd.cartesian(filtered_users_rdd) \
    .filter(lambda x: x[0][0] < x[1][0]) \
    .map(lambda x: ((x[0][0], x[1][0]), x[0][1].intersection(x[1][1]))) \
    .filter(lambda x: len(x[1]) >= 3)

# collect all groups of users with common products
grouped_users_rdd = common_users_rdd.groupByKey().mapValues(list)

# sort by the UserId of the first element and remove duplicates
sorted_unique_rdd = grouped_users_rdd \
    .map(lambda x: (sorted(x[1]), x[0])) \
    .sortByKey() \
    .map(lambda x: (x[1], x[0]))

# save the result
sorted_unique_rdd.saveAsTextFile(output_filepath)
