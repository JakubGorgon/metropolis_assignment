from pyspark.sql import functions as F
from pyspark.sql.window import Window # Kept for potential future use, though not used in simplified logic
from src.config import SCHEMA, VALID_CITY_CODES # Removed CITIES_WITH_POWIAT_RIGHTS from import as it's no longer used for filtering

# --- LOADING & CLEANING (Spark) ---
def load_and_filter_data(spark, file_path):
    # 1. Load Raw
    df = spark.read.csv(file_path, sep="\t", header=False, schema=SCHEMA)
    
    # 2. Basic Filter
    basic_df = df.filter(
        (F.col("feature_class") == "P") & 
        (F.col("population") > 0)
    )
    
    # 3. DEFINE FILTER LOGIC
    # We strictly follow the provided whitelist from config to identify "places where people live"
    # This removes PPLX (Sections), PPLQ (Abandoned), etc.
    clean_df = basic_df.filter(
        F.col("feature_code").isin(VALID_CITY_CODES)
    )

    return clean_df.select(
        "geonameid", "name", "population", "latitude", "longitude", "feature_code", "feature_class"
    )