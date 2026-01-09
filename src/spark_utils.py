"""
Utilities for managing Spark sessions.
Includes specific configurations for stability in Windows environments.
"""
from pyspark.sql import SparkSession
import os
import sys

def get_spark_session(app_name="Metropolis_Analysis"):
    """
    Initializes a Spark Session with optimized memory and Windows-specific stability settings.
    """
    # --- CRITICAL WINDOWS FIXES ---
    # 1. Point to the correct Python executable
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # 2. Handle Hadoop/Winutils (Suppress warnings)
    if sys.platform.startswith('win'):
        # os.environ['HADOOP_HOME'] = "C:/hadoop" # Uncomment if using winutils
        pass

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.python.worker.reuse", "true") 

    spark = builder.getOrCreate()
    return spark