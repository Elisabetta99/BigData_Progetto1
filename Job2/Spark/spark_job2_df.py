from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count

# Inizializzazione della sessione Spark
spark = SparkSession.builder.appName("UserAppreciation").getOrCreate()

# Lettura del file CSV del dataset Amazon Fine Food
reviews_df = spark.read.csv("file:///home/elisabetta/PycharmProjects/BigData_Progetto1/Job2/Spark/Reviews.csv", header=True)

# Calcolo dell'apprezzamento per ogni utente
utility = when(col("HelpfulnessDenominator") != 0, col("HelpfulnessNumerator") / col("HelpfulnessDenominator")).otherwise(0)
user_appreciation_df = reviews_df.groupBy("UserId") \
    .agg((sum(utility) / count("*")).alias("Appreciation"))

# Ordinamento della lista di utenti in base all'apprezzamento
sorted_users = user_appreciation_df.orderBy("Appreciation", ascending=False)

# Visualizzazione dei risultati
sorted_users.show()

# Salvataggio dei risultati in un file di output
sorted_users.write.csv("file:///home/elisabetta/PycharmProjects/BigData_Progetto1/Job2/Spark/output_job2_spark_df", header=True)
