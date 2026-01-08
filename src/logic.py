from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config import (
    SCHEMA, CITIES_WITH_POWIAT_RIGHTS,
    METRO_CITY_POPULATION_CONSTANT,
    MIN_METRO_CITY_RADIUS, MAX_METRO_CITY_RADIUS,
    METRO_CITY_POWER_CONSTANT
)

def load_and_filter_data(spark, file_path):
    """
    Loads data and applies the 'Admin Code Shield' to remove districts from major cities.
    Also removes PPLX from rural areas to prevent double counting.
    """
    # 1. Load Raw
    df = spark.read.csv(file_path, sep="\t", header=False, schema=SCHEMA)
    
    # 2. Basic Filter (Pop > 0, Class P)
    # We keep PPLX momentarily to check if they are impostors in big cities
    basic_df = df.filter(
        (F.col("feature_class") == "P") & 
        (F.col("population") > 0)
    )
    
    # 3. BUILD THE SHIELD (Identify Protected Admin Codes)
    # Find rows matching our Golden List names
    potential_cities = basic_df.filter(
        F.col("name").isin(CITIES_WITH_POWIAT_RIGHTS) | 
        F.col("asciiname").isin(CITIES_WITH_POWIAT_RIGHTS) |
        # Explicit check for Warsaw just in case alternates are tricky
        F.col("alternatenames").contains("Warszawa") 
    )
    
    # Logic: For each Admin2Code, pick the City with Max Population
    # This solves the "Chełm" problem (City vs Village)
    w = Window.partitionBy("admin2_code").orderBy(F.col("population").desc())
    
    true_cities = potential_cities.withColumn("rank", F.row_number().over(w)) \
                                  .filter(F.col("rank") == 1) \
                                  .select("admin2_code", "geonameid") \
                                  .collect()
    
    # Create Map: { admin2_code : valid_city_id }
    shield_map = {row['admin2_code']: row['geonameid'] for row in true_cities if row['admin2_code'] is not None}
    
    # Broadcast the map for performance
    shield_broadcast = spark.sparkContext.broadcast(shield_map)
    
    # 4. DEFINE FILTER LOGIC
    def is_valid_row(admin_code, current_id, feature_code):
        # Case A: Row is in a Protected Zone (e.g. Warsaw's County)
        if admin_code in shield_broadcast.value:
            # It survives ONLY if it is the Owner (The Metropolis)
            return current_id == shield_broadcast.value[admin_code]
        
        # Case B: Row is in a regular county (e.g. Lubin, Piaseczno)
        # Apply standard rules: No PPLX allowed (Rural Double Counting prevention)
        if feature_code == 'PPLX':
            return False
            
        return True # Keep valid PPL/PPLA in rural areas

    filter_udf = F.udf(is_valid_row, "boolean")
    
    # 5. APPLY FILTER
    clean_df = basic_df.filter(
        filter_udf(F.col("admin2_code"), F.col("geonameid"), F.col("feature_code"))
    )

    return clean_df.select(
        "geonameid", "name", "population", "latitude", "longitude", "feature_code", "admin2_code", "feature_class"
    )

def calculate_metro_radius(df):
    return df.withColumn(
        "radius_km",
        MIN_METRO_CITY_RADIUS + MAX_METRO_CITY_RADIUS * (
            1 - F.exp(F.lit(METRO_CITY_POPULATION_CONSTANT) * F.col("population"))
        )
    )

def calculate_distance_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_rad, lon1_rad = F.radians(lat1), F.radians(lon1)
    lat2_rad, lon2_rad = F.radians(lat2), F.radians(lon2)
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = F.sin(dlat/2)**2 + F.cos(lat1_rad)*F.cos(lat2_rad)*F.sin(dlon/2)**2
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1-a))
    return R * c

def assign_towns_to_metros(towns_df, metros_df):
    # Prepare Metros for Join
    metros_clean = metros_df.select(
        F.col("geonameid").alias("metro_id"),
        F.col("name").alias("metro_name"),
        F.col("latitude").alias("metro_lat"),
        F.col("longitude").alias("metro_lon"),
        F.col("radius_km").alias("metro_radius")
    )
    
    # Broadcast Cross Join
    joined = towns_df.crossJoin(F.broadcast(metros_clean))
    
    # Calc Distance
    joined = joined.withColumn(
        "distance_km",
        calculate_distance_km(
            F.col("latitude"), F.col("longitude"),
            F.col("metro_lat"), F.col("metro_lon")
        )
    )
    
    # Filter by Radius
    candidates = joined.filter(F.col("distance_km") <= F.col("metro_radius"))
    
    # Calc Impact
    candidates = candidates.withColumn(
        "impact",
        F.exp(F.lit(METRO_CITY_POWER_CONSTANT) * (F.col("distance_km") / F.col("metro_radius")))
    )
    
    # Select Best Match
    window_spec = Window.partitionBy("geonameid").orderBy(F.col("impact").desc())
    
    best_matches = candidates.withColumn("rank", F.row_number().over(window_spec)) \
                             .filter(F.col("rank") == 1) \
                             .drop("rank")
                             
    return best_matches.select(
        "geonameid", "name", "population", "latitude", "longitude",
        "metro_id", "metro_name", "distance_km", "impact"
    )