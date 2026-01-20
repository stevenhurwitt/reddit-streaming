#!/usr/bin/env python3
"""Test Spark connection from JupyterLab to Spark Master"""

from pyspark.sql import SparkSession
import sys

print("Testing Spark connection...")
print(f"Python version: {sys.version}")

try:
    spark = SparkSession.builder \
        .appName("test_connection") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.cores", "1") \
        .config("spark.cores.max", "2") \
        .getOrCreate()
    
    print(f"✓ Spark session created successfully!")
    print(f"✓ Spark version: {spark.version}")
    print(f"✓ Spark master: {spark.sparkContext.master}")
    
    # Simple test
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data)
    result = rdd.sum()
    print(f"✓ Test computation: sum([1,2,3,4,5]) = {result}")
    
    spark.stop()
    print("✓ Test completed successfully!")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
