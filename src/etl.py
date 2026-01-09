from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config import SCHEMA, CITIES_WITH_POWIAT_RIGHTS, VALID_CITY_CODES

# --- LOADING & CLEANING (Spark) ---
def load_and_filter_data(spark, file_path):
    # 1. Load Raw
    df = spark.read.csv(file_path, sep="\t", header=False, schema=SCHEMA)
    
    # 2. Basic Filter + VALID_CITY_CODES Enforcement
    # We strictly allow only codes defined in "places where people live"
    # This automatically handles excluding PPLX, PPLQ, etc.
    basic_df = df.filter(
        (F.col("feature_class") == "P") & 
        (F.col("population") > 0) &
        (F.col("feature_code").isin(VALID_CITY_CODES))
    )
    
    # 3. BUILD THE SHIELD (Identify Administrative Centers)
    # Strategy: Find the "True" City for each admin region that contains a Powiat City
    potential_cities = basic_df.filter(
        F.col("name").isin(CITIES_WITH_POWIAT_RIGHTS) | 
        F.col("asciiname").isin(CITIES_WITH_POWIAT_RIGHTS) |
        F.col("alternatenames").contains("Warszawa") 
    )
    
    w = Window.partitionBy("admin2_code").orderBy(F.col("population").desc())
    
    # "Shields" are the dominant cities in their admin district
    shield_df = potential_cities.withColumn("rank", F.row_number().over(w)) \
                                .filter(F.col("rank") == 1) \
                                .select(
                                    F.col("admin2_code").alias("shield_admin"), 
                                    F.col("geonameid").alias("shield_id")
                                )
    
    # 4. APPLY SHIELD (Native Spark Join)
    # Join strategy:
    # - If a row belongs to an admin code that HAS a shield city, it must BE that shield city.
    # - If the admin code has NO shield city, the row is kept (it's a normal town).
    
    joined_df = basic_df.join(
        shield_df, 
        basic_df.admin2_code == shield_df.shield_admin, 
        "left"
    )
    
    clean_df = joined_df.filter(
        F.col("shield_id").isNull() | (F.col("geonameid") == F.col("shield_id"))
    )

    return clean_df.select(
        "geonameid", "name", "population", "latitude", "longitude", "feature_code", "feature_class", "admin2_code"
    )