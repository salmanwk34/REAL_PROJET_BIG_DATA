from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, regexp_replace



# Initialisation de Spark Session
spark = SparkSession.builder \
    .appName("ETL OpenFoodFacts - Regime Cetogene") \
    .getOrCreate()

# Chargement des données OpenFoodFacts
food_df = spark.read.csv("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/en.openfoodfacts.org.products.csv", 
                         header=True, sep="\t", inferSchema=True)

# Nettoyage des données
def clean_data(df):
    # Suppression des lignes avec valeurs manquantes dans les colonnes clés
    df = df.dropna(subset=["product_name", "brands", "proteins_100g", "carbohydrates_100g", "fat_100g"])
    # Suppression des caractères spéciaux dans les colonnes textuelles
    for col_name in df.columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "[^a-zA-Z0-9\s]", ""))
    return df

food_clean = clean_data(food_df)

# Filtrage pour le régime cétogène
ketogenic_df = food_clean.filter((col("carbohydrates_100g") < 10) & 
                                 (col("fat_100g") >= 55) & 
                                 (col("proteins_100g") >= 20))

# Génération de menus équilibrés
def generate_menu(df):
    menu_week = {}
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for day in days:
        menu_week[day] = df.sample(False, 0.01).limit(3).collect()  # 3 produits par jour
    return menu_week

menu_week = generate_menu(ketogenic_df)

# Conversion en DataFrame pour le chargement
menu_rows = [(day, product['product_name'], product['brands']) 
             for day, products in menu_week.items() 
             for product in products]

menu_df = spark.createDataFrame(menu_rows, ["Day", "Product_Name", "Brand"])

# Chargement dans PostgreSQL
database_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {"user": "postgres", "password": "1234", "driver": "org.postgresql.Driver"}

menu_df.write.jdbc(url=database_url, table="ketogenic_menu", mode="overwrite", properties=properties)

# Fin de la session Spark
spark.stop()
