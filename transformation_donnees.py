from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Initialisation de Spark Session
spark = SparkSession.builder.appName("Transformation Donnees").getOrCreate()

# Lecture des données
food_df = spark.read.parquet("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/food.parquet")

# Nettoyage des données
def clean_data(df):
    df = df.dropna(subset=["product_name", "brands", "proteins_100g", "carbohydrates_100g", "fat_100g"])
    for column in df.columns:
        df = df.withColumn(column, regexp_replace(col(column), "[^a-zA-Z0-9\\s]", ""))
    return df

food_clean = clean_data(food_df)


# Sauvegarde des données nettoyées
food_clean.write.mode("overwrite").parquet("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/food_clean.parquet")

# Fermeture de la session Spark
spark.stop()
