"""
Spark + Neo4j Single-Node Benchmark
====================================

This script benchmarks Apache Spark queries against a single Neo4j instance.
It measures query performance for various operation types and reports statistics.

PURPOSE:
--------
Unlike the cluster benchmark, this script tests a single Neo4j server setup,
which is the common configuration for development and smaller deployments.

BENCHMARK METHODOLOGY:
----------------------
Each query is run 3 times to get:
- Average time: Typical performance
- Min time: Best-case (likely cached)
- Max time: Worst-case
- Count: Number of results (for verification)

This approach accounts for:
1. Cold start effects (first query slower)
2. Caching benefits (subsequent queries faster)
3. System variability

QUERIES TESTED:
---------------
1. Load Products: Basic node loading
2. Filter High-Rated: Spark filtering on DataFrame
3. Top Products: Sorting and limiting
4. Category Analysis: Neo4j aggregation via Cypher
5. Load Relationships: Loading edges between nodes
6. Customer Reviews: Complex filtered aggregation
7. Similarity Network: Graph traversal patterns

DEPENDENCIES:
- pyspark: Apache Spark Python API
- neo4j-connector-apache-spark: Spark connector
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import time
import json
from datetime import datetime


def create_spark_session():
    """
    Create a Spark session configured for single-node Neo4j.
    
    This configuration connects to a standalone Neo4j instance
    (not a cluster) using the Bolt protocol directly.
    
    CONFIGURATION:
    - bolt://localhost:7687: Direct connection to single server
    - local[*]: Use all local CPU cores for Spark processing
    - spark.jars.packages: Auto-download Neo4j connector from Maven
    
    Returns:
    --------
    SparkSession
        Configured Spark session ready for Neo4j queries
    """
    return SparkSession.builder \
        .appName("Neo4j-Spark-Single-Node-Benchmark") \
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.1_for_spark_3") \
        .config("neo4j.url", "bolt://localhost:7687") \
        .config("neo4j.authentication.basic.username", "neo4j") \
        .config("neo4j.authentication.basic.password", "Password") \
        .config("neo4j.database", "neo4j") \
        .master("local[*]") \
        .getOrCreate()


def benchmark_spark_queries(spark):
    """
    Run comprehensive benchmark suite testing various query patterns.
    
    Each query is executed 3 times to gather statistics.
    Results include timing (avg/min/max) and result counts.
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session with Neo4j configuration
    
    Returns:
    --------
    dict
        Dictionary mapping query names to performance statistics
    """
    results = {}
    
    
    # ================================================
    # QUERY 1: Load Products
    # ================================================
    # Baseline test: Load all Product nodes from Neo4j into Spark
    # This tests raw data transfer performance
    
    times = []
    for i in range(3):  # Run 3 times for statistics
        start = time.time()
        products_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("labels", "Product") \
            .load()
        count = products_df.count()  # Forces execution
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Load Products'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # ================================================
    # QUERY 2: Filter High-Rated Products
    # ================================================
    # Tests Spark's DataFrame filtering capabilities
    # Data is loaded from Neo4j, then filtered in Spark
    
    times = []
    for i in range(3):
        start = time.time()
        high_rated = products_df.filter(
            (col("avg_rating") >= 4.5) & (col("total_reviews") >= 100)
        )
        count = high_rated.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Filter High-Rated'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # ================================================
    # QUERY 3: Top Products by Reviews
    # ================================================
    # Tests sorting and limiting operations in Spark
    
    times = []
    for i in range(3):
        start = time.time()
        top_products = products_df \
            .filter(col("total_reviews") > 0) \
            .orderBy(desc("total_reviews")) \
            .limit(100)
        count = top_products.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Top Products'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # ================================================
    # QUERY 4: Category Analysis (Cypher Aggregation)
    # ================================================
    # Tests Neo4j's aggregation capabilities via Cypher
    # The aggregation (count, avg) happens in Neo4j
    
    times = []
    for i in range(3):
        start = time.time()
        category_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("query", """
                MATCH (cat:Category)<-[:BELONGS_TO]-(p:Product)
                RETURN cat.name as category, 
                       count(p) as products, 
                       avg(p.avg_rating) as avg_rating
            """) \
            .load()
        count = category_df.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Category Analysis'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # ================================================
    # QUERY 5: Load Relationships
    # ================================================
    # Tests loading graph edges (relationships) into Spark
    # Uses special relationship loading syntax
    
    times = []
    for i in range(3):
        start = time.time()
        similar_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("relationship", "SIMILAR") \
            .option("relationship.source.labels", "Product") \
            .option("relationship.target.labels", "Product") \
            .load()
        count = similar_df.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Load Relationships'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # ================================================
    # QUERY 6: Customer Reviews Aggregation
    # ================================================
    # Complex query: find top reviewers with high ratings
    # Tests filtering, aggregation, and sorting in Neo4j
    
    times = []
    for i in range(3):
        start = time.time()
        reviews_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("query", """
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.rating >= 4
                RETURN c.id as customer_id, count(r) as reviews
                ORDER BY reviews DESC
                LIMIT 100
            """) \
            .load()
        count = reviews_df.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Customer Reviews'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # ================================================
    # QUERY 7: Similarity Network (Graph Pattern)
    # ================================================
    # Tests graph traversal: find connected similar products
    # This leverages Neo4j's strength in graph patterns
    
    times = []
    for i in range(3):
        start = time.time()
        network_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("query", """
                MATCH (p1:Product)-[:SIMILAR]->(p2:Product)
                WHERE p1.total_reviews > 100 AND p2.total_reviews > 100
                RETURN p1.asin as product1, p1.title as title1,
                       p2.asin as product2, p2.title as title2
                LIMIT 500
            """) \
            .load()
        count = network_df.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    avg_time = sum(times) / len(times)
    results['Similarity Network'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    return results


def main():
    """
    Main entry point - runs benchmark suite and saves results.
    
    WORKFLOW:
    1. Create Spark session with Neo4j connection
    2. Run all benchmark queries (3x each)
    3. Compile results with metadata
    4. Save to JSON file for analysis
    """
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce logging noise
    
    # Run benchmarks
    results = benchmark_spark_queries(spark)
    
    # Compile output with metadata
    output = {
        'timestamp': datetime.now().isoformat(),
        'architecture': 'single-node',
        'spark_version': spark.version,
        'neo4j_version': '4.4.46',
        'connector_version': '5.3.0',
        'benchmarks': results
    }
    
    # Save results to file
    with open('spark_neo4j_single_benchmark.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    # Clean up
    spark.stop()


if __name__ == "__main__":
    main()
