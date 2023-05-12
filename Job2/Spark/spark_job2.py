from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inizializzazione
spark = SparkSession.builder.appName("UserAppreciation").getOrCreate()

# Caricamento recensioni in un DataFrame
reviews_df = spark.read.csv("/home/elisabetta/Scrivania/BigData/Reviews.csv", header=True)

# Filtraggio delle colonne necessarie per il calcolo dell'apprezzamento
filtered_df = reviews_df.select("UserId", "HelpfulnessNumerator", "HelpfulnessDenominator")

# Calcolo dell'utilità per ogni recensione
utility_df = filtered_df.withColumn("Utility", col("HelpfulnessNumerator") / col("HelpfulnessDenominator"))

# Calcolo della media dell'utilità per ogni utente
user_appreciation_df = utility_df.groupBy("userId").avg("Utility").withColumnRenamed("avg(Utility)", "Appreciation")

# Ordinamento degli utenti in base all'apprezzamento
sorted_users_df = user_appreciation_df.orderBy(col("Appreciation").desc())

# Visualizzazione risultati
sorted_users_df.show()

# Salvataggio
sorted_users_df.write.txt("Job2/Spark/output_job2_spark", header=True)