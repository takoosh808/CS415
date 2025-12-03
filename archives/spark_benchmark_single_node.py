from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import time
import json
from datetime import datetime

def create_spark_session():
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
    results = {}
    
    
    
    # Query 1: Load Products and analyze
    
    times = []
    for i in range(3):
        start = time.time()
        products_df = spark.read.format("org.neo4j.spark.DataSource") \
            .option("labels", "Product") \
            .load()
        count = products_df.count()
        elapsed = time.time() - start
        times.append(elapsed)
        
    
    avg_time = sum(times) / len(times)
    results['Load Products'] = {
        'avg': round(avg_time, 3),
        'min': round(min(times), 3),
        'max': round(max(times), 3),
        'count': count
    }
    
    
    # Query 2: Filter high-rated products using Spark
    
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
    
    
    # Query 3: Top products by reviews using Spark
    
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
    
    
    # Query 4: Load and analyze relationships using Cypher in Spark
    
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
    
    
    # Query 5: Load relationships
    
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
    
    
    # Query 6: Customer review aggregation
    
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
    
    
    # Query 7: Complex graph pattern using Spark + Neo4j
    
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
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    results = benchmark_spark_queries(spark)
    output = {
        'timestamp': datetime.now().isoformat(),
        'architecture': 'single-node',
        'spark_version': spark.version,
        'neo4j_version': '4.4.46',
        'connector_version': '5.3.0',
        'benchmarks': results
    }
    with open('spark_neo4j_single_benchmark.json', 'w') as f:
        json.dump(output, f, indent=2)
    spark.stop()

if __name__ == "__main__":
    main()
