from pyspark.sql import SparkSession

# Initialisation de Spark Session
spark = SparkSession.builder.appName("Preparation Donnees").getOrCreate()

# Chemins des fichiers
chemin_food = "C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/en.openfoodfacts.org.products.csv"
chemin_regimes = "C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/regimes.csv"
chemin_utilisateurs = "C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/utilisateurs.csv"

# Chargement et sauvegarde des données OpenFoodFacts
food_df = spark.read.csv(chemin_food, header=True, sep="\t", inferSchema=True)
food_df.write.mode("overwrite").parquet("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/food.parquet")

# Affichage des données OpenFoodFacts
food_df.show()

# Chargement et sauvegarde des données des régimes
regimes_df = spark.read.csv(chemin_regimes, header=True, inferSchema=True)
regimes_df.write.mode("overwrite").parquet("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/regimes.parquet")

# Affichage des données des régimes
regimes_df.show()

# Chargement et sauvegarde des données des utilisateurs
users_df = spark.read.csv(chemin_utilisateurs, header=True, inferSchema=True)
users_df.write.mode("overwrite").parquet("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/users.parquet")

# Affichage des données des utilisateurs
users_df.show()

# Fermeture de la session Spark
spark.stop()
