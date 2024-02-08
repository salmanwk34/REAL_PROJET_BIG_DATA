from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialisation de Spark Session
spark = SparkSession.builder.appName("Generation Menus").getOrCreate()


food_clean = spark.read.parquet("C:/Users/Utilisateur/Desktop/PROJET_BIG_DATA_V2/food_clean.parquet")

# Critère du régime cétogène)
criteria = {
    "Cetogene": {"carbohydrates_100g": 10, "fat_100g": 55, "proteins_100g": 20},
}

# Filtrer les produits selon les critères et générer des menus
menus = {}
for regime, crit in criteria.items():
    filtered_df = food_clean.filter(
        (col("carbohydrates_100g") < crit["carbohydrates_100g"]) &
        (col("fat_100g") >= crit["fat_100g"]) &
        (col("proteins_100g") >= crit["proteins_100g"])
    )
    # Générer un menu (exemple : 3 produits aléatoires par jour pour une semaine)
    menu_week = {day: filtered_df.sample(False, 0.01).limit(3).collect() for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}
    menus[regime] = menu_week

# Sauvegarde des menus générés
# Ceci est un exemple, la structure de sauvegarde peut varier selon les besoins
for regime, menu_week in menus.items():
    for day, products in menu_week.items():
        for product in products:
            print(f"{day} - {regime}: {product['product_name']} - {product['brands']}")

# Fermeture de la session Spark
spark.stop()