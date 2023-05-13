from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count

# Inizializzazione
spark = SparkSession.builder.appName("UserAppreciation").getOrCreate()

# Caricamento recensioni in un DataFrame
reviews_df = spark.read.csv("/home/elisabetta/Scrivania/BigData/Reviews.csv", header=True)

# Calcolo dell'apprezzamento per ogni utente
utility = when(col("helpfulnessDenominator") != 0, col("helpfulnessNumerator") / col("helpfulnessDenominator")).otherwise(0)
user_appreciation_df = reviews_df.groupBy("userId") \
    .agg((sum(utility) / count("*")).alias("appreciation"))

# Ordinamento della lista di utenti in base all'apprezzamento
sorted_users = user_appreciation_df.orderBy("appreciation", ascending=False)

# Salvataggio dei risultati in un file di testo
sorted_users.write.text("p/home/elisabetta/Scrivania/BigData/output_job2_spark.txt")