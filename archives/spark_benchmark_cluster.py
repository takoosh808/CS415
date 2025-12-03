"""
Spark + Neo4j Cluster Benchmark
================================

This script benchmarks Apache Spark queries against a Neo4j Enterprise cluster.
It tests query performance across multiple Neo4j cluster nodes to measure
distributed database performance.

CLUSTER ARCHITECTURE:
---------------------
Neo4j Enterprise supports clustering for:
- High Availability: If one server fails, others continue working
- Read Scaling: Multiple servers can handle read queries
- Leader/Follower model: One leader handles writes, followers replicate

Typical cluster setup:
  - Core 1 (Follower): bolt://localhost:7687
  - Core 2 (Follower): bolt://localhost:7688
  - Core 3 (Leader):   bolt://localhost:7689

WHY BENCHMARK CLUSTERS?
-----------------------
1. Measure if queries are properly distributed
2. Identify bottlenecks in specific nodes
3. Compare performance across leader vs. followers
4. Validate cluster health and consistency

SPARK + NEO4J:
--------------
The neo4j-connector-apache-spark allows Spark to:
- Read data directly from Neo4j into Spark DataFrames
- Execute Cypher queries and process results in Spark
- Leverage Spark's distributed processing with Neo4j's graph data

DEPENDENCIES:
- pyspark: Apache Spark Python API
- neo4j-connector-apache-spark: Spark connector for Neo4j
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import time
import json
from datetime import datetime


def create_spark_session_cluster():
    """
    Create a Spark session configured for Neo4j Enterprise cluster.
    
    CONFIGURATION EXPLAINED:
    ------------------------
    - spark.jars.packages: Automatically downloads Neo4j connector from Maven
    - neo4j.url: Connection URL (neo4j:// for cluster routing)
    - neo4j.authentication: Username/password for database
    - master("local[*]"): Use all available local CPU cores
    
    Note: "neo4j://" protocol enables automatic routing to cluster nodes,
    while "bolt://" connects to a specific node directly.
    
    Returns:
    --------
    SparkSession configured for Neo4j cluster access
    """
    # Create Spark session with Neo4j connector
    return SparkSession.builder \
        .appName("Neo4j-Spark-Cluster-Benchmark") \
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.1_for_spark_3") \
        .config("neo4j.url", "neo4j://localhost:7687") \
        .config("neo4j.authentication.basic.username", "neo4j") \
        .config("neo4j.authentication.basic.password", "Password") \
        .config("neo4j.database", "neo4j") \
        .master("local[*]") \
        .getOrCreate()


def benchmark_on_node(spark, node_uri, node_name):
    """
    Run a suite of benchmark queries against a specific Neo4j node.
    
    This function tests different types of queries:
    1. Simple node loading (Products)
    2. Aggregation queries (Category analysis)
    3. Relationship loading (SIMILAR relationships)
    4. Complex filtered queries (Customer reviews)
    
    Parameters:
    -----------
    spark : SparkSession
        Active Spark session
    node_uri : str
        Bolt URI of the specific node to test (e.g., "bolt://localhost:7687")
    node_name : str
        Human-readable name for reporting (e.g., "Core 1 (Follower)")
    
    Returns:
    --------
    dict
        Dictionary mapping query names to {time, count} results
    """
    # Temporarily reconfigure Spark to connect to this specific node
    spark.conf.set("neo4j.url", node_uri)
    
    results = {}
    
    # QUERY 1: Load All Products
    # --------------------------
    # Simple test: load all Product nodes into Spark
    # Uses "labels" option to load nodes by their label
    start = time.time()
    products_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("labels", "Product") \
        .load()
    count = products_df.count()  # Forces execution (Spark is lazy)
    elapsed = time.time() - start
    results['Load Products'] = {'time': round(elapsed, 3), 'count': count}
    
    
    # QUERY 2: Category Analysis with Aggregation
    # -------------------------------------------
    # Count products per category using Cypher
    # This tests aggregation performance in Neo4j
    start = time.time()
    category_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("query", """
            MATCH (cat:Category)<-[:BELONGS_TO]-(p:Product)
            RETURN cat.name as category, count(p) as products
        """) \
        .load()
    # Apply Spark operations for ordering and limiting
    category_df = category_df.orderBy(desc("products")).limit(50)
    count = category_df.count()
    elapsed = time.time() - start
    results['Category Analysis'] = {'time': round(elapsed, 3), 'count': count}
    
    
    # QUERY 3: Load Relationships
    # ---------------------------
    # Load SIMILAR relationships between Products
    # Tests relationship traversal performance
    start = time.time()
    similar_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("relationship", "SIMILAR") \
        .option("relationship.source.labels", "Product") \
        .option("relationship.target.labels", "Product") \
        .load()
    count = similar_df.count()
    elapsed = time.time() - start
    results['Load Relationships'] = {'time': round(elapsed, 3), 'count': count}
    
    
    # QUERY 4: Customer Reviews Analysis
    # ----------------------------------
    # Find top customers by number of high-rating reviews
    # Tests complex pattern matching with filtering
    start = time.time()
    reviews_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("query", """
            MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
            WHERE r.rating >= 4
            RETURN c.id as customer, count(r) as reviews
        """) \
        .load()
    # Sort and limit in Spark
    reviews_df = reviews_df.orderBy(desc("reviews")).limit(100)
    count = reviews_df.count()
    elapsed = time.time() - start
    results['Customer Reviews'] = {'time': round(elapsed, 3), 'count': count}
    
    
    return results


def main():
    """
    Main benchmark execution - tests all nodes in the cluster.
    
    WORKFLOW:
    1. Create Spark session with Neo4j connector
    2. Iterate through each cluster node
    3. Run benchmark suite on each node
    4. Collect and compare results
    5. Save benchmark report to JSON
    """
    # Initialize Spark
    spark = create_spark_session_cluster()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    
    # Define cluster nodes to benchmark
    # In a real cluster, these would be different physical/virtual machines
    nodes = [
        ("bolt://localhost:7687", "Core 1 (Follower)"),
        ("bolt://localhost:7688", "Core 2 (Follower)"),
        ("bolt://localhost:7689", "Core 3 (Leader)")
    ]
    
    # Run benchmarks on each node
    all_results = {}
    for uri, name in nodes:
        try:
            results = benchmark_on_node(spark, uri, name)
            all_results[name] = results
        except Exception as e:
            # Record errors for nodes that fail (e.g., not running)
            all_results[name] = {"error": str(e)}
    
    # Determine successful nodes
    successful_nodes = [k for k, v in all_results.items() if "error" not in v]
    
    # Prepare output report
    output = {
        'timestamp': datetime.now().isoformat(),
        'architecture': 'cluster' if len(successful_nodes) > 1 else 'single-node',
        'spark_version': spark.version,
        'neo4j_version': '4.4.46',
        'connector_version': '5.3.0',
        'active_nodes': len(successful_nodes),
        'benchmarks': all_results
    }
    
    # Save results to JSON file
    filename = 'spark_neo4j_cluster_benchmark.json'
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    
    # Clean up Spark resources
    spark.stop()


if __name__ == "__main__":
    main()
