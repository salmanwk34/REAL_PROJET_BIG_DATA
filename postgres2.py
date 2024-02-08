from pyspark.sql import SparkSession

# Initialisation de Spark Session
spark = SparkSession.builder \
    .appName("Chargement Données Menu Cétogène dans PostgreSQL") \
    .getOrCreate()

# Chargement du fichier CSV
file_path = "ketogenic_menu.csv"  
menu_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Affichage des premières lignes pour vérification
menu_df.show()

# Chargement dans PostgreSQL
database_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {"user": "postgres", "password": "1234", "driver": "org.postgresql.Driver"}

menu_df.write.jdbc(url=database_url, table="ketogenic_menu", mode="overwrite", properties=properties)

# Fin de la session Spark
spark.stop()
