import pytest
import os
import pyspark.sql.functions as F
from great_expectations.dataset import SparkDFDataset
from src.config import CITIES_WITH_POWIAT_RIGHTS
from src.etl import load_and_filter_data

# --- HELPER ---
def verify_impostor_counts(df):
    """
    Test Logic:
    Ensures that for the Top Metropolises, NO other rows exist 
    sharing the same Admin2 Code. Districts or duplicates would indicate
    a failure in the 'Admin Code Shield' logic.
    """
    # 1. Identify Top 10 Metropolises by Population
    top_metros = df.filter(F.col("name").isin(CITIES_WITH_POWIAT_RIGHTS)) \
                   .orderBy(F.col("population").desc()) \
                   .limit(10) \
                   .select("name", "admin2_code", "geonameid") \
                   .collect()

    total_failures = 0
    failures_details = []

    # 2. Iterate and Check
    for metro in top_metros:
        m_name = metro['name']
        m_code = metro['admin2_code']
        m_id = metro['geonameid']
        
        # Count rows that have SAME admin code but DIFFERENT ID
        impostor_count = df.filter(
            (F.col("admin2_code") == m_code) & 
            (F.col("geonameid") != m_id)
        ).count()
        
        if impostor_count > 0:
            total_failures += impostor_count
            failures_details.append(f"{m_name} (Admin: {m_code}) has {impostor_count} impostors")

    return total_failures, failures_details

# --- TEST ---

@pytest.mark.integration
def test_full_dataset_quality(spark_session):
    """
    Integration Test:
    Runs logic against the REAL dataset (data/PL.txt).
    Skips if file not present.
    """
    data_path = "data/PL.txt"
    if not os.path.exists(data_path):
        pytest.skip(f"Data file {data_path} not found. Skipping integration test.")

    # 1. Load & Clean
    df = load_and_filter_data(spark_session, data_path).cache()
    
    # 2. Check Impostors (Custom Logic)
    fail_count, fail_details = verify_impostor_counts(df)
    assert fail_count == 0, f"Found {fail_count} impostor cities (shield failed): {fail_details}"
    
    # 3. Great Expectations Checks
    gx_df = SparkDFDataset(df)

    # Rule A: Population > 0
    res_pop = gx_df.expect_column_values_to_be_between("population", min_value=1)
    assert res_pop["success"], f"Found populations <= 0: {res_pop['result']}"

    # Rule B: No PPLX Sections
    res_pplx = gx_df.expect_column_values_to_not_be_in_set("feature_code", value_set=["PPLX"])
    assert res_pplx["success"], f"Found PPLX sections in output: {res_pplx['result']}"

    # Rule C: Critical List Presence
    res_list = gx_df.expect_column_values_to_be_in_set(
        column="name",
        value_set=CITIES_WITH_POWIAT_RIGHTS,
        mostly=0.01
    )
    assert res_list["success"], "Less than 1% of rows are Powiat Cities. Did we lose data?"

    # Rule E: Feature Class
    res_class = gx_df.expect_column_values_to_be_in_set("feature_class", value_set=["P"])
    assert res_class["success"], f"Found feature_class other than 'P': {res_class['result']}"