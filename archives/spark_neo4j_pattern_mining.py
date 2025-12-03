"""
Spark + Neo4j Pattern Mining System
====================================

This module implements a complete FP-Growth pattern mining pipeline using
Apache Spark to process customer review data stored in Neo4j.

PURPOSE:
--------
Find "co-purchase" patterns - products that customers frequently buy/review
together. These patterns power recommendation systems ("Customers who bought
X also bought Y").

KEY CONCEPTS:
-------------
1. TRANSACTION: A customer's purchase/review history as a list of products
2. SUPPORT: How many customers bought a product set (higher = more popular)
3. CONFIDENCE: If customer bought A, probability they bought B
4. LIFT: How much more likely A→B is vs random chance (lift > 1 = correlated)

FP-GROWTH ALGORITHM:
--------------------
Traditional association rule mining (Apriori) is slow because it generates
many candidate itemsets. FP-Growth is faster:
1. Build a compact tree structure from transactions
2. Mine patterns directly from tree (no candidate generation)
3. Much more memory-efficient for large datasets

TRAIN/TEST SPLIT:
-----------------
To validate patterns are real (not noise), we:
1. TRAIN: Mine patterns from 70% of data
2. TEST: Verify patterns appear in remaining 30%
3. Only keep patterns that appear in BOTH sets

DEPENDENCIES:
- pyspark: Apache Spark with MLlib (FP-Growth)
- Neo4j Spark Connector: Bridge between Spark and Neo4j

USAGE:
------
    miner = SparkNeo4jPatternMining()
    reviews = miner.load_reviews_from_neo4j()
    patterns = miner.mine_patterns_fpgrowth(reviews)
    miner.close()
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    collect_list, col, explode, size, array_distinct, 
    count, lit, rand, when, array, struct, array_contains
)
from pyspark.ml.fpm import FPGrowth
from pyspark import StorageLevel
import time
import json
import sys


class SparkNeo4jPatternMining:
    """
    Pattern mining system connecting Spark ML to Neo4j graph database.
    
    This class provides a complete pipeline for:
    1. Loading review data from Neo4j
    2. Splitting into train/test sets
    3. Mining patterns using FP-Growth
    4. Validating patterns across datasets
    5. Enriching patterns with product information
    
    Attributes:
    -----------
    spark : SparkSession
        Configured Spark session with Neo4j connector
    neo4j_uri : str
        Neo4j connection URI (bolt://host:port)
    neo4j_user : str
        Neo4j authentication username
    neo4j_password : str
        Neo4j authentication password
    """
    
    def __init__(self, neo4j_uri="bolt://localhost:7687", neo4j_user="neo4j", neo4j_password="Password"):
        """
        Initialize Spark session with Neo4j connector and memory optimization.
        
        Parameters:
        -----------
        neo4j_uri : str
            Neo4j database connection URI
        neo4j_user : str
            Username for Neo4j authentication
        neo4j_password : str
            Password for Neo4j authentication
        
        SPARK CONFIGURATION EXPLAINED:
        ------------------------------
        - spark.driver.memory: 4GB for driver (coordinates work)
        - spark.executor.memory: 4GB per executor (does work)
        - spark.sql.shuffle.partitions: 4 partitions for shuffles (small dataset)
        - spark.default.parallelism: 4 parallel tasks
        - spark.sql.adaptive.enabled: Dynamically optimize query plans
        - spark.driver.maxResultSize: Max data sent back to driver
        - spark.memory.fraction: 60% of heap for Spark processing
        - spark.memory.storageFraction: 20% of Spark memory for caching
        
        These settings are tuned for a single-machine deployment with
        moderate memory. Adjust for larger clusters.
        """
        
        # Build Spark session with all necessary configurations
        self.spark = SparkSession.builder \
            .appName("Neo4j Co-Purchase Pattern Mining") \
            .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.1_for_spark_3") \
            .config("neo4j.url", neo4j_uri) \
            .config("neo4j.authentication.basic.username", neo4j_user) \
            .config("neo4j.authentication.basic.password", neo4j_password) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.2") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.files.maxPartitionBytes", "64m") \
            .getOrCreate()
        
        # Store connection parameters for later queries
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
    
    def load_reviews_from_neo4j(self):
        """
        Load all customer review relationships from Neo4j.
        
        This query traverses the graph pattern:
        (Customer)-[REVIEWED]->(Product)
        
        and extracts the necessary fields for pattern mining.
        
        CYPHER QUERY EXPLAINED:
        -----------------------
        MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
        - MATCH: Find all instances of this pattern
        - (c:Customer): Node with Customer label
        - -[r:REVIEWED]->: Outgoing relationship named REVIEWED
        - (p:Product): Node with Product label
        
        WHERE c.id IS NOT NULL AND p.asin IS NOT NULL
        - Filter out incomplete data
        
        RETURN ...
        - c.id: Customer identifier
        - p.asin: Product identifier (Amazon Standard ID Number)
        - r.rating: Star rating (1-5)
        - r.dataset: Pre-assigned train/test split (if exists)
        
        STORAGE:
        --------
        Data is persisted to DISK_ONLY to handle large datasets
        without running out of memory.
        
        Returns:
        --------
        DataFrame
            Spark DataFrame with columns: customer_id, product_id, rating, dataset
        """
        start_time = time.time()
        
        # Execute Cypher query via Spark connector
        reviews_df = self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_uri) \
            .option("authentication.basic.username", self.neo4j_user) \
            .option("authentication.basic.password", self.neo4j_password) \
            .option("query", """
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE c.id IS NOT NULL AND p.asin IS NOT NULL
                RETURN c.id as customer_id, 
                       p.asin as product_id, 
                       r.rating as rating,
                       r.dataset as dataset
            """) \
            .load()
        
        # Persist to disk for reuse (prevents re-loading from Neo4j)
        reviews_df.persist(StorageLevel.DISK_ONLY)
        return reviews_df
    
    def split_train_test_spark(self, reviews_df, train_ratio=0.7):
        """
        Split reviews into training and test sets using random assignment.
        
        This is crucial for pattern validation: we mine patterns from training
        data, then verify they also appear in test data (proving they're real
        patterns, not statistical noise).
        
        Parameters:
        -----------
        reviews_df : DataFrame
            Reviews DataFrame from load_reviews_from_neo4j()
        train_ratio : float
            Fraction for training (default 0.7 = 70% train, 30% test)
        
        Returns:
        --------
        DataFrame
            Same DataFrame with 'dataset' column populated ('train' or 'test')
        
        Notes:
        ------
        If data already has split assignments, they're preserved.
        Uses seed=42 for reproducibility.
        """
        # Check if data already has train/test split
        has_split = reviews_df.filter(col("dataset").isNotNull()).count() > 0
        if has_split:
            return reviews_df  # Keep existing split
        
        # Assign random split using deterministic seed
        reviews_with_split = reviews_df.withColumn(
            "dataset",
            when(rand(seed=42) < train_ratio, lit("train")).otherwise(lit("test"))
        )
        return reviews_with_split
    
    def mine_patterns_fpgrowth(self, reviews_df, min_support=0.002, min_confidence=0.1, dataset='train', max_items_per_customer=30, max_transactions=50000):
        """
        Mine co-purchase patterns using FP-Growth algorithm.
        
        FP-GROWTH OVERVIEW:
        -------------------
        1. Each customer's review history becomes a "transaction"
        2. FP-Growth finds itemsets (product combinations) that appear
           in at least min_support fraction of transactions
        3. Association rules show "if customer bought A, likely bought B"
        
        Parameters:
        -----------
        reviews_df : DataFrame
            Reviews with train/test split
        min_support : float
            Minimum fraction of customers that must have itemset (0.002 = 0.2%)
            Lower values find more patterns but include more noise
        min_confidence : float
            Minimum probability for association rules (0.1 = 10%)
        dataset : str
            Which dataset to mine ('train' or 'test')
        max_items_per_customer : int
            Limit products per customer (prevents power-users from dominating)
        max_transactions : int
            Limit total transactions (prevents memory issues)
        
        Returns:
        --------
        dict or None
            Dictionary containing:
            - 'transactions': DataFrame of customer baskets
            - 'itemsets': Frequent product pairs with support counts
            - 'rules': Association rules with confidence/lift
            - 'num_transactions': Total transactions processed
            - 'elapsed_time': Processing time in seconds
            Returns None if mining fails.
        """
        start_time = time.time()
        
        # Filter to requested dataset (train or test)
        dataset_reviews = reviews_df.filter(col("dataset") == dataset)
        dataset_count = dataset_reviews.count()
        
        # Fallback: if dataset is too small, use all data
        if dataset_count < 100:
            dataset_reviews = reviews_df
        
        # Import windowing functions for limiting items per customer
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        # Remove duplicate reviews (same customer reviewing same product)
        deduplicated = dataset_reviews.dropDuplicates(["customer_id", "product_id"])
        
        # Limit items per customer using window function
        # This prevents power-users from skewing patterns
        window_spec = Window.partitionBy("customer_id").orderBy(rand())
        limited_reviews = deduplicated.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") <= max_items_per_customer) \
            .drop("row_num")
        
        # Create transactions: group products by customer
        # Each row becomes: (customer_id, [product1, product2, ...])
        transactions = limited_reviews.groupBy("customer_id") \
            .agg(collect_list("product_id").alias("items")) \
            .filter((size("items") >= 2) & (size("items") <= max_items_per_customer))
        
        # Sample if too many transactions (memory management)
        num_transactions = transactions.count()
        if num_transactions > max_transactions:
            sample_fraction = max_transactions / num_transactions
            transactions = transactions.sample(False, sample_fraction, seed=42)
            num_transactions = transactions.count()
        
        # Cache transactions to disk for FP-Growth algorithm
        transactions = transactions.persist(StorageLevel.DISK_ONLY)
        transactions.count()  # Force materialization
        
        # Configure and run FP-Growth
        fpGrowth = FPGrowth(
            itemsCol="items", 
            minSupport=min_support, 
            minConfidence=min_confidence
        )
        
        try:
            model = fpGrowth.fit(transactions)
        except Exception as e:
            # Clean up on failure
            transactions.unpersist()
            return None
        
        # Extract frequent itemsets (focus on pairs only)
        # size("items") == 2 filters to product pairs
        frequent_itemsets = model.freqItemsets \
            .filter(size("items") == 2) \
            .orderBy(col("freq").desc()) \
            .persist(StorageLevel.DISK_ONLY)
        num_patterns = frequent_itemsets.count()
        
        # Extract association rules (single item → single item only)
        # This gives rules like: "bought A → likely bought B"
        rules = model.associationRules \
            .filter(size("antecedent") == 1) \
            .filter(size("consequent") == 1) \
            .orderBy(col("confidence").desc(), col("lift").desc()) \
            .persist(StorageLevel.DISK_ONLY)
        
        elapsed = time.time() - start_time
        
        return {
            'transactions': transactions,
            'itemsets': frequent_itemsets,
            'rules': rules,
            'num_transactions': num_transactions,
            'elapsed_time': elapsed
        }
    
    def validate_patterns_cross_dataset(self, train_patterns, test_reviews_df, min_support=5):
        """
        Validate training patterns exist in test dataset.
        
        PURPOSE:
        --------
        Prevent overfitting: patterns that only appear in training data
        might be noise. Patterns appearing in BOTH train and test data
        are more likely to be real purchasing behaviors.
        
        Parameters:
        -----------
        train_patterns : dict
            Results from mine_patterns_fpgrowth()
        test_reviews_df : DataFrame
            Reviews DataFrame with dataset column
        min_support : int
            Minimum occurrences in test set to validate
        
        Returns:
        --------
        list
            List of validated patterns with train and test support counts
        """
        # Get top 50 training patterns
        train_itemsets = train_patterns['itemsets'].limit(50).collect()
        
        # Build test transactions
        test_transactions = test_reviews_df \
            .filter(col("dataset") == "test") \
            .groupBy("customer_id") \
            .agg(collect_list("product_id").alias("items"))
        
        validated_patterns = []
        
        # Check each training pattern in test data
        for itemset_row in train_itemsets:
            products = sorted(itemset_row['items'])
            train_support = itemset_row['freq']
            
            # Build filter: check if transaction contains ALL products
            filter_expr = array_contains(col("items"), products[0])
            for prod in products[1:]:
                filter_expr = filter_expr & array_contains(col("items"), prod)
            
            # Count occurrences in test data
            test_support = test_transactions.filter(filter_expr).count()
            
            # Only keep if meets minimum test support
            if test_support >= min_support:
                validated_patterns.append({
                    'products': products,
                    'train_support': train_support,
                    'test_support': test_support,
                    'total_support': train_support + test_support
                })
        
        return validated_patterns
    
    def enrich_patterns_with_product_info(self, patterns):
        """
        Add product titles and ratings to pattern results.
        
        Patterns contain only product ASINs (IDs). This method
        queries Neo4j to get human-readable information.
        
        Parameters:
        -----------
        patterns : list
            Validated patterns from validate_patterns_cross_dataset()
        
        Returns:
        --------
        list
            Enriched patterns with titles, ratings, and support metrics
        """
        enriched = []
        
        # Process top 25 patterns
        for pattern in patterns[:25]:
            products = pattern['products']
            
            # Query Neo4j for product details
            product_df = self.spark.read.format("org.neo4j.spark.DataSource") \
                .option("url", self.neo4j_uri) \
                .option("authentication.basic.username", self.neo4j_user) \
                .option("authentication.basic.password", self.neo4j_password) \
                .option("query", f"""
                    MATCH (p:Product)
                    WHERE p.asin IN {products}
                    RETURN p.asin as asin, p.title as title, p.avg_rating as rating
                """) \
                .load() \
                .collect()
            
            # Build lookup dictionary
            product_info = {row['asin']: {'title': row['title'], 'rating': row['rating']} for row in product_df}
            
            # Combine pattern data with product info
            enriched.append({
                'products': products,
                'titles': [product_info.get(asin, {}).get('title', 'Unknown') for asin in products],
                'ratings': [product_info.get(asin, {}).get('rating', 0.0) for asin in products],
                'train_support': pattern['train_support'],
                'test_support': pattern['test_support'],
                'total_support': pattern['total_support'],
                'confidence': pattern.get('confidence', 0.0)
            })
        
        return enriched
    
    def save_results(self, results, output_file='spark_neo4j_results.json'):
        """
        Save pattern mining results to JSON file.
        
        Parameters:
        -----------
        results : dict
            Dictionary with 'stats' and 'patterns' keys
        output_file : str
            Output filename
        """
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
    
    def close(self):
        """
        Clean up Spark session and release resources.
        
        Always call this when done to free cluster resources.
        """
        self.spark.stop()


def main():
    """
    Main execution pipeline - demonstrates complete pattern mining workflow.
    
    WORKFLOW STAGES:
    ----------------
    1. INITIALIZATION: Create miner with Neo4j connection
    2. DATA LOADING: Pull customer reviews from graph database
    3. TRAIN/TEST SPLIT: Divide data for validation
    4. PATTERN MINING: Run FP-Growth on training data
    5. VALIDATION: Verify patterns in test data
    6. ENRICHMENT: Add product names/ratings to results
    7. OUTPUT: Save results to JSON file
    
    This demonstrates the complete pipeline for finding and
    validating co-purchase patterns.
    """
    # Stage 1: Initialize pattern miner
    miner = SparkNeo4jPatternMining()
    
    # Stage 2: Load review data from Neo4j
    reviews_df = miner.load_reviews_from_neo4j()
    
    # Stage 3: Split into train/test (70/30)
    reviews_df = miner.split_train_test_spark(reviews_df, train_ratio=0.7)
    
    # Stage 4: Mine patterns from training data
    # Parameters tuned for moderate dataset size
    train_results = miner.mine_patterns_fpgrowth(
        reviews_df, 
        min_support=0.002,      # 0.2% of customers must have pattern
        min_confidence=0.1,     # 10% confidence for rules
        dataset='train',
        max_items_per_customer=30,
        max_transactions=15000
    )
    
    # Handle mining failure
    if train_results is None:
        miner.spark.stop()
        return
    
    # Display top 10 patterns from training
    top_patterns = train_results['itemsets'].limit(10).collect()
    for i, row in enumerate(top_patterns, 1):
        products = sorted(row['items'])
        support = row['freq']
        print(f"{i}. Products: {products}")
        print(f"   Support: {support} customers")
        print()
    
    # Prepare patterns for cross-dataset validation
    patterns_for_validation = []
    for row in train_results['itemsets'].limit(50).collect():
        patterns_for_validation.append({
            'products': sorted(row['items']),
            'train_support': row['freq'],
            'test_support': 0,
            'total_support': row['freq']
        })
    
    # Stage 5: Validate patterns exist in test data
    validated_patterns = miner.validate_patterns_cross_dataset(
        train_results,
        reviews_df,
        min_support=5  # Must appear in at least 5 test transactions
    )
    
    # Display validated patterns
    for i, pattern in enumerate(sorted(validated_patterns, key=lambda x: x['total_support'], reverse=True)[:10], 1):
        print(f"{i}. Products: {pattern['products']}")
        print(f"   Train Support: {pattern['train_support']}")
        print(f"   Test Support: {pattern['test_support']}")
        print(f"   Total Support: {pattern['total_support']}")
        print()
    
    # Stage 6: Enrich with product information
    enriched_patterns = miner.enrich_patterns_with_product_info(validated_patterns)
    
    # Compile final results
    results = {
        'stats': {
            'total_reviews': reviews_df.count(),
            'train_reviews': reviews_df.filter(col("dataset") == "train").count(),
            'test_reviews': reviews_df.filter(col("dataset") == "test").count(),
            'num_patterns': len(validated_patterns),
            'mining_time': train_results['elapsed_time']
        },
        'patterns': enriched_patterns
    }
    
    # Stage 7: Save results to file
    miner.save_results(results, 'spark_neo4j_patterns.json')
    
    # Clean up
    miner.close()


if __name__ == "__main__":
    main()
