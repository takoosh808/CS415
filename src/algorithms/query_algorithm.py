"""
Complex Query Algorithm using PySpark
This algorithm handles complex queries over Amazon product data
"""

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc
import time


class QueryAlgorithm:
    """
    Algorithm to answer complex queries on Amazon product dataset
    Input: Query conditions (filters), number of results k
    Output: k products matching the query conditions
    """
    
    def __init__(self):
        self.spark = None
        self.products_df = None
        self.reviews_df = None
        
    def initialize_spark(self):
        """Initialize Spark session"""
        print("Initializing Spark session...")
        
        self.spark = SparkSession.builder \
            .appName("AmazonQueryAlgorithm") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session initialized")
        
    def load_data_from_neo4j(self):
        """Load data from Neo4j into Spark DataFrames"""
        print("Loading data from Neo4j...")
        
        from neo4j import GraphDatabase
        
        # Connect to Neo4j
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                     auth=("neo4j", "Password"))
        
        # Get products
        with driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (p:Product)
                OPTIONAL MATCH (p)<-[r:REVIEWED]-()
                RETURN p.id as id, p.asin as asin, p.title as title, 
                       p.group as group, p.salesrank as salesrank,
                       p.avg_rating as avg_rating, p.total_reviews as total_reviews,
                       count(r) as review_count
            """)
            
            products_data = []
            for record in result:
                products_data.append({
                    'id': record['id'],
                    'asin': record['asin'],
                    'title': record['title'],
                    'group': record['group'],
                    'salesrank': record['salesrank'],
                    'avg_rating': record['avg_rating'],
                    'total_reviews': record['total_reviews'],
                    'review_count': record['review_count']
                })
        
        # Get reviews
        with driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                RETURN p.id as product_id, c.customer_id as customer_id,
                       r.rating as rating, r.date as date,
                       r.votes as votes, r.helpful_votes as helpful_votes
            """)
            
            reviews_data = []
            for record in result:
                reviews_data.append({
                    'product_id': record['product_id'],
                    'customer_id': record['customer_id'],
                    'rating': record['rating'],
                    'date': record['date'],
                    'votes': record['votes'],
                    'helpful_votes': record['helpful_votes']
                })
        
        driver.close()
        
        # Create Spark DataFrames
        self.products_df = self.spark.createDataFrame(products_data)
        self.reviews_df = self.spark.createDataFrame(reviews_data)
        
        print(f"Loaded {self.products_df.count()} products")
        print(f"Loaded {self.reviews_df.count()} reviews")
        
    def execute_query(self, conditions, k=10):
        """
        Execute a complex query with given conditions
        
        Args:
            conditions: dict with query filters
                - min_rating: minimum average rating
                - max_rating: maximum average rating
                - min_reviews: minimum number of reviews
                - group: product group filter
                - max_salesrank: maximum sales rank
            k: number of results to return
            
        Returns:
            List of products matching the conditions
        """
        print(f"\nExecuting query with conditions: {conditions}")
        start_time = time.time()
        
        # Start with full dataset
        filtered_df = self.products_df
        
        # Apply filters based on conditions
        if 'min_rating' in conditions:
            filtered_df = filtered_df.filter(col('avg_rating') >= conditions['min_rating'])
            
        if 'max_rating' in conditions:
            filtered_df = filtered_df.filter(col('avg_rating') <= conditions['max_rating'])
            
        if 'min_reviews' in conditions:
            filtered_df = filtered_df.filter(col('review_count') >= conditions['min_reviews'])
            
        if 'group' in conditions:
            filtered_df = filtered_df.filter(col('group') == conditions['group'])
            
        if 'max_salesrank' in conditions:
            filtered_df = filtered_df.filter(
                (col('salesrank').isNotNull()) & 
                (col('salesrank') <= conditions['max_salesrank'])
            )
        
        # Sort by rating and review count, then limit to k results
        result_df = filtered_df.orderBy(
            desc('avg_rating'), 
            desc('review_count')
        ).limit(k)
        
        # Collect results
        results = result_df.collect()
        
        execution_time = time.time() - start_time
        
        print(f"Query returned {len(results)} results in {execution_time:.3f} seconds")
        
        return results, execution_time
    
    def find_products_by_customer_reviews(self, min_customer_reviews, k=10):
        """
        Find products reviewed by customers who have reviewed at least N products
        This is a non-searchable attribute query
        
        Args:
            min_customer_reviews: minimum number of products a customer must have reviewed
            k: number of results to return
            
        Returns:
            List of products
        """
        print(f"\nFinding products reviewed by active customers (min {min_customer_reviews} reviews)...")
        start_time = time.time()
        
        # Find active customers
        active_customers = self.reviews_df.groupBy('customer_id') \
            .agg(count('*').alias('customer_review_count')) \
            .filter(col('customer_review_count') >= min_customer_reviews)
        
        # Join with reviews to find products
        products_by_active = self.reviews_df \
            .join(active_customers, 'customer_id') \
            .select('product_id', 'rating') \
            .groupBy('product_id') \
            .agg(
                avg('rating').alias('avg_rating_from_active'),
                count('*').alias('active_customer_reviews')
            )
        
        # Join with products and sort
        result_df = self.products_df \
            .join(products_by_active, self.products_df.id == products_by_active.product_id) \
            .select(
                'id', 'asin', 'title', 'group', 
                'avg_rating_from_active', 'active_customer_reviews'
            ) \
            .orderBy(desc('active_customer_reviews'), desc('avg_rating_from_active')) \
            .limit(k)
        
        results = result_df.collect()
        execution_time = time.time() - start_time
        
        print(f"Found {len(results)} products in {execution_time:.3f} seconds")
        
        return results, execution_time
    
    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    # Test the algorithm
    algo = QueryAlgorithm()
    algo.initialize_spark()
    algo.load_data_from_neo4j()
    
    # Test query 1: High rated products
    results1, time1 = algo.execute_query({
        'min_rating': 4.5,
        'min_reviews': 3
    }, k=10)
    
    print("\nTop products with rating >= 4.5 and at least 3 reviews:")
    for r in results1:
        print(f"  {r.title[:50]} - Rating: {r.avg_rating}")
    
    # Test query 2: Books with good sales
    results2, time2 = algo.execute_query({
        'group': 'Book',
        'min_rating': 4.0,
        'max_salesrank': 100000
    }, k=5)
    
    print("\nTop books with rating >= 4.0 and salesrank <= 100000:")
    for r in results2:
        print(f"  {r.title[:50]} - Rating: {r.avg_rating}, Rank: {r.salesrank}")
    
    algo.stop_spark()
