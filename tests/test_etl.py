"""
Unit tests for the ETL module.
Verifies strict filtering, schema adherence, and data cleaning logic.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from src.etl import load_and_filter_data
from src.config import SCHEMA, CITIES_WITH_POWIAT_RIGHTS
import tempfile
import os

def test_load_and_filter_data(spark_session):
    """
    Test the strict filtering logic:
    - Input: A mix of Valid Powiat Cities, Valid Small Towns, and Invalid Feature Codes (PPLX).
    - Expected Output: Only Valid Powiat Cities + Valid Small Towns. Invalid Codes removed.
    """
    
    # 1. Create Dummy Data
    # PPL = Populated Place (Valid)
    # PPLX = Section of Populated Place (Invalid)
    # PPLA = Seat of first-order administrative division (Valid)
    
    # We simulate the "shielding" scenario:
    # - "Warszawa" (PPLC - Capital) -> Should be KEPT and Shielded
    # - "Warszawa" (PPL - A district impostor in same Admin2) -> Should be REMOVED by Shield
    # - "SmallTown" (PPL) -> Should be KEPT
    # - "RandomSection" (PPLX) -> Should be REMOVED by Feature Code
    
    data = [
        # (id, name, latitude, longitude, feat_class, feat_code, admin2)
        (1, "Warszawa", 52.2, 21.0, "P", "PPLC", "WA_CODE"),  # TRUE WARSAW
        (2, "Warszawa", 52.3, 21.1, "P", "PPL",  "WA_CODE"),  # IMPOSTOR (District)
        (3, "SmallTown", 50.0, 20.0, "P", "PPL",  "OTHER"),    # Normal Town
        (4, "BadSection", 50.1, 20.1, "P", "PPLX", "OTHER"),   # Invalid Type
        (5, "Hotel", 50.2, 20.2, "S", "HTL", "OTHER"),         # Invalid Class (S)
    ]
    
    # Create Spark DataFrame manually to match schema partially
    # We only populate columns needed for logic
    from pyspark.sql import Row
    rows = [Row(geonameid=d[0], name=d[1], asciiname=d[1], alternatenames="", 
                latitude=d[2], longitude=d[3], feature_class=d[4], feature_code=d[5], 
                country_code="PL", cc2="", admin1_code="", admin2_code=d[6], 
                admin3_code="", admin4_code="", population=2000 if d[0]==1 else 1000, 
                elevation=0, dem="", timezone="", modification_date="") for d in data]
    
    # We need to write this to a CSV because load_and_filter_data expects a path
    # (Refactor hint: Ideally load_and_filter_data should accept a DF too, but for now we test the file interface)
    
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = os.path.join(tmpdirname, "test_data.txt")
        
        # Write tab-separated file manually to mimic PL.txt
        with open(file_path, "w", encoding='utf-8') as f:
            for r in rows:
                line = "\t".join([str(x) for x in r])
                f.write(line + "\n")
        
        # 2. Run ETL
        result_df = load_and_filter_data(spark_session, file_path)
        results = result_df.collect()
        ids = [r['geonameid'] for r in results]
        
        # 3. Assertions
        
        # Check 1: True Warsaw (1) should exist
        assert 1 in ids, "True Warsaw was lost!"
        
        # Check 2: Impostor Warsaw (2) should be filtered by Shield
        # (Because both have 'WA_CODE', and True Warsaw matches the shield map)
        assert 2 not in ids, "Impostor Warsaw was NOT filtered by Shield!"
        
        # Check 3: SmallTown (3) should exist
        assert 3 in ids, "Valid small town was lost!"
        
        # Check 4: BadSection (4) should be filtered by logic (PPLX check)
        # Note: If Shield logic is active, PPLX check is secondary.
        # But if it wasn't valid admin code, PPLX check applies.
        assert 4 not in ids, "PPLX Section was not filtered!"
        
        # Check 5: Hotel (5) should be filtered by feature_class='P' in load
        assert 5 not in ids, "Feature class S was not filtered!"

