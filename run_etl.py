import sys
import os
import pandas as pd

# --- CRITICAL WINDOWS FIX ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from src.spark_utils import get_spark_session
from src.etl import load_and_filter_data

def main():
    spark = get_spark_session("PredictX_Metropolis_ETL")
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n--- 🧹 METROPOLIS DATA CLEANER (ETL) 🧹 ---")
    
    # 1. Load & Clean Raw Data using Spark
    print("Step 1: Processing PL.txt with Spark...")
    spark_df = load_and_filter_data(spark, "data/PL.txt").cache()
    
    count = spark_df.count()
    print(f" -> Found {count} valid locations.")
    
    # 2. Convert to Pandas for Export
    print("Step 2: Exporting to CSV...")
    df = spark_df.toPandas()
    
    # Ensure numeric types
    for c in ['latitude', 'longitude', 'population', 'geonameid']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    # Drop rows with bad data
    df = df.dropna(subset=['population', 'latitude', 'longitude'])
    
    # 3. Save Clean Data
    # Note: We save to a generic name now, since it's just input data
    output_dir = "output/simulation_data"
    output_path = f"{output_dir}/clean_places.csv"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    df.to_csv(output_path, index=False)
    
    print(f"✅ Success! Saved {len(df)} rows to {output_path}.")
    print("   Now run: streamlit run dashboard.py")
    
    spark.stop()

if __name__ == "__main__":
    main()