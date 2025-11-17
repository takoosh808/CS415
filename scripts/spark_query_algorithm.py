from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from neo4j import GraphDatabase
import time


class SparkQueryAlgorithm:
    
    def __init__(self, neo4j_uri="bolt://localhost:7687", neo4j_user="neo4j", neo4j_password="Password"):
        self.spark = SparkSession.builder \
            .appName("Amazon Co-Purchase Query Analysis") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    def load_products_from_neo4j(self):
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (p:Product)
                RETURN p.id as id, p.asin as asin, p.title as title,
                       p.group as product_group, p.salesrank as salesrank,
                       p.avg_rating as avg_rating, p.total_reviews as total_reviews
            """)
            
            data = []
            for record in result:
                data.append((
                    record['id'],
                    record['asin'],
                    record['title'],
                    record['product_group'],
                    record['salesrank'] if record['salesrank'] else 0,
                    float(record['avg_rating']) if record['avg_rating'] else 0.0,
                    record['total_reviews'] if record['total_reviews'] else 0
                ))
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("asin", StringType(), True),
            StructField("title", StringType(), True),
            StructField("product_group", StringType(), True),
            StructField("salesrank", IntegerType(), True),
            StructField("avg_rating", FloatType(), True),
            StructField("total_reviews", IntegerType(), True)
        ])
        
        products_df = self.spark.createDataFrame(data, schema)
        return products_df
    
    def execute_query(self, conditions, k=10):
        start_time = time.time()
        
        products_df = self.load_products_from_neo4j()
        
        filtered_df = products_df
        
        if 'min_rating' in conditions:
            filtered_df = filtered_df.filter(col("avg_rating") >= conditions['min_rating'])
        
        if 'max_rating' in conditions:
            filtered_df = filtered_df.filter(col("avg_rating") <= conditions['max_rating'])
        
        if 'min_reviews' in conditions:
            filtered_df = filtered_df.filter(col("total_reviews") >= conditions['min_reviews'])
        
        if 'group' in conditions:
            filtered_df = filtered_df.filter(col("product_group") == conditions['group'])
        
        if 'max_salesrank' in conditions:
            filtered_df = filtered_df.filter(
                (col("salesrank") <= conditions['max_salesrank']) & 
                (col("salesrank") > 0)
            )
        
        result_df = filtered_df.orderBy(
            col("avg_rating").desc(),
            col("total_reviews").desc()
        ).limit(k)
        
        results = result_df.collect()
        products = [row.asDict() for row in results]
        
        elapsed = time.time() - start_time
        return products, elapsed
    
    def find_products_by_active_customers(self, min_customer_reviews=5, k=10):
        start_time = time.time()
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                RETURN c.id as customer_id, p.id as product_id, p.asin as asin,
                       p.title as title, p.avg_rating as product_avg_rating,
                       p.total_reviews as total_reviews, r.rating as rating
            """)
            
            data = []
            for record in result:
                data.append((
                    record['customer_id'],
                    record['product_id'],
                    record['asin'],
                    record['title'],
                    float(record['product_avg_rating']) if record['product_avg_rating'] else 0.0,
                    record['total_reviews'] if record['total_reviews'] else 0,
                    record['rating']
                ))
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("asin", StringType(), True),
            StructField("title", StringType(), True),
            StructField("product_avg_rating", FloatType(), True),
            StructField("total_reviews", IntegerType(), True),
            StructField("rating", IntegerType(), True)
        ])
        
        reviews_df = self.spark.createDataFrame(data, schema)
        
        customer_review_counts = reviews_df.groupBy("customer_id") \
            .agg(count("*").alias("review_count")) \
            .filter(col("review_count") >= min_customer_reviews)
        
        active_customer_products = reviews_df.join(
            customer_review_counts,
            "customer_id"
        )
        
        result_df = active_customer_products.groupBy(
            "product_id", "asin", "title", "product_avg_rating", "total_reviews"
        ).agg(
            count("customer_id").alias("active_customer_count"),
            avg("rating").alias("rating_from_active_customers")
        ).orderBy(
            col("active_customer_count").desc(),
            col("rating_from_active_customers").desc()
        ).limit(k)
        
        results = result_df.collect()
        products = [row.asDict() for row in results]
        
        elapsed = time.time() - start_time
        return products, elapsed
    
    def close(self):
        if self.driver:
            self.driver.close()
        if self.spark:
            self.spark.stop()


def main():
    query_algo = SparkQueryAlgorithm()
    
    try:
        print("\n=== Spark Query Results ===\n")
        
        results1, time1 = query_algo.execute_query({
            'min_rating': 4.5,
            'min_reviews': 100
        }, k=10)
        print(f"Query 1: {len(results1)} results in {time1:.2f}s")
        
        results2, time2 = query_algo.execute_query({
            'group': 'Book',
            'min_rating': 4.0,
            'max_salesrank': 50000
        }, k=10)
        print(f"Query 2: {len(results2)} results in {time2:.2f}s")
        
        results3, time3 = query_algo.execute_query({
            'group': 'Music',
            'min_rating': 4.0,
            'min_reviews': 50
        }, k=10)
        print(f"Query 3: {len(results3)} results in {time3:.2f}s")
        
        results4, time4 = query_algo.find_products_by_active_customers(
            min_customer_reviews=5, k=10
        )
        print(f"Query 4: {len(results4)} results in {time4:.2f}s")
        
        print(f"\nAverage: {(time1+time2+time3+time4)/4:.2f}s")
        
    finally:
        query_algo.close()


if __name__ == "__main__":
    main()
