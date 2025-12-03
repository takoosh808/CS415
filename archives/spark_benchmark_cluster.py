from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import time
import json
from datetime import datetime

def create_spark_session_cluster():
    # Neo4j Enterprise cluster with 3 cores on different ports
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
    
    # Temporarily reconfigure for this node
    spark.conf.set("neo4j.url", node_uri)
    
    results = {}
    
    # Query 1: Load Products
    
    start = time.time()
    products_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("labels", "Product") \
        .load()
    count = products_df.count()
    elapsed = time.time() - start
    results['Load Products'] = {'time': round(elapsed, 3), 'count': count}
    
    
    # Query 2: Category Analysis
    
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
    
    
    # Query 3: Load Relationships
    
    start = time.time()
    similar_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("relationship", "SIMILAR") \
        .option("relationship.source.labels", "Product") \
        .option("relationship.target.labels", "Product") \
        .load()
    count = similar_df.count()
    elapsed = time.time() - start
    results['Load Relationships'] = {'time': round(elapsed, 3), 'count': count}
    
    
    # Query 4: Customer Reviews
    
    start = time.time()
    reviews_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("query", """
            MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
            WHERE r.rating >= 4
            RETURN c.id as customer, count(r) as reviews
        """) \
        .load()
    # Apply Spark operations for ordering and limiting
    reviews_df = reviews_df.orderBy(desc("reviews")).limit(100)
    count = reviews_df.count()
    elapsed = time.time() - start
    results['Customer Reviews'] = {'time': round(elapsed, 3), 'count': count}
    
    
    return results

def main():
    spark = create_spark_session_cluster()
    spark.sparkContext.setLogLevel("WARN")
    nodes = [
        ("bolt://localhost:7687", "Core 1 (Follower)"),
        ("bolt://localhost:7688", "Core 2 (Follower)"),
        ("bolt://localhost:7689", "Core 3 (Leader)")
    ]
    all_results = {}
    for uri, name in nodes:
        try:
            results = benchmark_on_node(spark, uri, name)
            all_results[name] = results
        except Exception as e:
            all_results[name] = {"error": str(e)}
    successful_nodes = [k for k, v in all_results.items() if "error" not in v]
    output = {
        'timestamp': datetime.now().isoformat(),
        'architecture': 'cluster' if len(successful_nodes) > 1 else 'single-node',
        'spark_version': spark.version,
        'neo4j_version': '4.4.46',
        'connector_version': '5.3.0',
        'active_nodes': len(successful_nodes),
        'benchmarks': all_results
    }
    filename = 'spark_neo4j_cluster_benchmark.json'
    with open(filename, 'w') as f:
        json.dump(output, f, indent=2)
    spark.stop()

if __name__ == "__main__":
    main()
