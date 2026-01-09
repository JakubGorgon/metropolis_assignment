from src.config import CITIES_WITH_POWIAT_RIGHTS
from great_expectations.dataset import SparkDFDataset
from src.etl import load_and_filter_data
from src.spark_utils import get_spark_session
import pyspark.sql.functions as F
import sys

def verify_impostor_counts(df, spark):
    """
    Test:
    Ensures that for the Top Metropolises, NO other rows exist 
    sharing the same Admin2 Code. Districts or duplicates would indicate
    a failure in the 'Admin Code Shield' logic.
    """
    print("\n--- 🕵️‍♀️ DEEP DIVE: City districts not in dataset ---")
    
    # 1. Identify Top 10 Metropolises by Population
    # Filter for names in our Golden List to ensure we target the specific Admin Zones
    # (Just taking top 10 pop might pick a village if data was broken)
    top_metros = df.filter(F.col("name").isin(CITIES_WITH_POWIAT_RIGHTS)) \
                   .orderBy(F.col("population").desc()) \
                   .limit(10) \
                   .select("name", "admin2_code", "geonameid") \
                   .collect()

    print(f"{'Metropolis':<15} | {'Admin2':<8} | {'Impostors'}")
    print("-" * 45)

    total_failures = 0
    
    # 2. Iterate and Check (Broadcast check essentially)
    for metro in top_metros:
        m_name = metro['name']
        m_code = metro['admin2_code']
        m_id = metro['geonameid']
        
        # Count rows that have SAME admin code but DIFFERENT ID
        impostor_count = df.filter(
            (F.col("admin2_code") == m_code) & 
            (F.col("geonameid") != m_id)
        ).count()
        
        print(f"{m_name:<15} | {m_code:<8} | {impostor_count}")
        
        if impostor_count > 0:
            total_failures += impostor_count

    print("-" * 45)
    
    if total_failures == 0:
        print("✅ CHECK PASSED: Top Metropolises are clean.")
        return True
    else:
        print(f"❌ CHECK FAILED: Found {total_failures} impostors hiding in cities!")
        return False

def run_data_quality_checks():
    print("\n--- 🛡️  STARTING DATA QUALITY GATE 🛡️  ---")
    
    # 1. Init
    spark = get_spark_session("GX_Test")
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Loading and cleaning data...")
    df = load_and_filter_data(spark, "data/PL.txt").cache()
    
    # 2. Run the impostor Check first
    structural_success = verify_impostor_counts(df, spark)
    
    # 3. Run Standard Great Expectations
    print("\n--- 📋 STANDARD CHECKS (Great Expectations) ---")
    gx_df = SparkDFDataset(df)
    results = []

    # Rule A: Population
    res = gx_df.expect_column_values_to_be_between("population", min_value=1)
    results.append(("Population > 0", res))

    # Rule B: PPLX Filter
    res = gx_df.expect_column_values_to_not_be_in_set("feature_code", value_set=["PPLX"])
    results.append(("No PPLX Sections", res))

    # Rule C: Critical List Presence
    res = gx_df.expect_column_values_to_be_in_set(
        column="name",
        value_set=CITIES_WITH_POWIAT_RIGHTS,
        mostly=0.01
    )
    results.append(("Critical Cities Present", res))

    # Rule E: Feature Class Must Be 'P' (Validation of cleaning pipeline)
    # Note: Requires 'feature_class' to be present in logic.py return!
    try:
        res = gx_df.expect_column_values_to_be_in_set("feature_class", value_set=["P"])
        results.append(("Feature Class == 'P'", res))
    except Exception as e:
        results.append(("Feature Class Check", {"success": False, "result": "Column missing in logic.py output"}))

    # 4. Report
    gx_success = True
    for test_name, result in results:
        if result["success"]:
            print(f"✅ PASSED: {test_name}")
        else:
            print(f"❌ FAILED: {test_name}")
            # If it failed due to missing column, print a hint
            if "Column missing" in str(result.get("result", "")):
                 print("   Hint: Did you add 'feature_class' to the select() in src/logic.py?")
            gx_success = False

    # 5. Final Verdict
    if structural_success and gx_success:
        print("\n🎉 All checks passed.")
        return True
    else:
        print("\n⛔ QUALITY GATES FAILED. Do not proceed.")
        return False

if __name__ == "__main__":
    success = run_data_quality_checks()
    if not success:
        sys.exit(1)