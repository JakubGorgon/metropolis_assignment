import sys
import os
import pandas as pd # Make sure pandas is imported

# --- CRITICAL WINDOWS FIX ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import functions as F
from src.spark_utils import get_spark_session
from src.logic import load_and_filter_data, calculate_metro_radius, assign_towns_to_metros
from src.config import METRO_POPULATION_THRESHOLD

def main():
    spark = get_spark_session("PredictX_Metropolis")
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n--- 🏙️  METROPOLIS SIMULATION START 🏙️  ---")
    
    # 1. Load & Clean
    print("Step 1: Cleaning Data...")
    all_places = load_and_filter_data(spark, "data/PL.txt").cache()
    print(f" -> {all_places.count()} valid locations loaded.")
    
    # 2. Identify Metropolises
    print(f"Step 2: Identifying Metropolises (> {METRO_POPULATION_THRESHOLD} pop)...")
    metros = all_places.filter(F.col("population") >= METRO_POPULATION_THRESHOLD)
    metros = calculate_metro_radius(metros)
    
    metro_count = metros.count()
    print(f" -> Found {metro_count} Metropolises.")
    metros.select("name", "population", "radius_km").show(5, truncate=False)
    
    # 3. Identify Towns
    towns = all_places.filter(F.col("population") < METRO_POPULATION_THRESHOLD)
    
    # 4. Run Gravity Model
    print("Step 3: Running Gravity Model (Assignment)...")
    matched = assign_towns_to_metros(towns, metros)
    
    # 5. Capture Orphans
    orphans = towns.join(matched, on="geonameid", how="left_anti") \
                   .withColumn("metro_id", F.lit(None)) \
                   .withColumn("metro_name", F.lit(None)) \
                   .withColumn("distance_km", F.lit(None)) \
                   .withColumn("impact", F.lit(0.0))
    
    # 6. Combine
    final_output = matched.unionByName(orphans, allowMissingColumns=True)
    
    # --- STEP 4: SAVE without winutils ---
    output_dir = "output/simulation_data"
    output_file = f"{output_dir}/results.csv"
    
    print(f"Step 4: Saving results to {output_file}...")
    
    # Create directory if not exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # CONVERT TO PANDAS -> SAVE TO CSV
    # bypasses the Hadoop/WinUtils error 
    pdf = final_output.toPandas()
    pdf.to_csv(output_file, index=False)
    
    print(f" -> Successfully saved {len(pdf)} rows.")
    print("\n✅ Simulation Complete. Run 'streamlit run app.py' to visualize.")
    spark.stop()

if __name__ == "__main__":
    main()