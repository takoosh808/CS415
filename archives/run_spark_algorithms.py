"""
Spark-Based Query and Pattern Mining Runner
===========================================

This script demonstrates running query and pattern mining algorithms using
Apache Spark with Neo4j integration. It's a runner/orchestrator that uses
external Spark algorithm modules.

PURPOSE:
--------
This script ties together:
1. SparkQueryAlgorithm: Run filtered queries using Spark + Neo4j
2. SparkPatternMining: Mine co-purchase patterns using Spark's FP-Growth

WHY USE SPARK?
--------------
Apache Spark provides:
- Distributed processing across multiple CPU cores
- Efficient handling of large datasets (millions of records)
- Built-in machine learning algorithms (like FP-Growth)
- Optimized data structures (DataFrames) for analytical queries

WORKFLOW:
---------
1. Execute various product queries to test query performance
2. Split data into train/test sets for pattern validation
3. Mine patterns on training data
4. Validate patterns against test data

Note: This script requires spark_query_algorithm.py and spark_pattern_mining.py
modules to be available in the Python path.

DEPENDENCIES:
- PySpark (Apache Spark Python API)
- neo4j-connector-apache-spark
- spark_query_algorithm module
- spark_pattern_mining module
"""

from spark_query_algorithm import SparkQueryAlgorithm  # Spark-based query execution
from spark_pattern_mining import SparkPatternMining    # Spark-based FP-Growth mining
import time


def main():
    """
    Main execution function demonstrating Spark query and pattern mining.
    
    STEPS:
    1. Query Phase: Run multiple filtered queries to benchmark performance
    2. Mining Phase: Train/test split, mine patterns, validate results
    
    The train/test split approach validates that discovered patterns are
    genuine (not artifacts of random chance) by verifying they appear in
    held-out data.
    """
    overall_start = time.time()
    
    # =====================
    # QUERY PHASE
    # =====================
    # Test various query types to measure performance
    
    query_algo = SparkQueryAlgorithm()
    
    try:
        # Query 1: High-rated products with many reviews
        # Finds products that are both well-rated and popular
        results1, time1 = query_algo.execute_query({
            'min_rating': 4.5,   # At least 4.5 stars
            'min_reviews': 100   # At least 100 reviews
        }, k=10)
        
        # Query 2: Top books with good sales rank
        # Finds quality books that are actually selling well
        results2, time2 = query_algo.execute_query({
            'group': 'Book',
            'min_rating': 4.0,
            'max_salesrank': 50000  # Top 50,000 selling books
        }, k=10)
        
        # Query 3: Popular music with reviews
        # Finds music products with active customer engagement
        results3, time3 = query_algo.execute_query({
            'group': 'Music',
            'min_rating': 4.0,
            'min_reviews': 50
        }, k=10)
        
        # Query 4: Products reviewed by active customers
        # Finds products that attract engaged reviewers
        results4, time4 = query_algo.find_products_by_active_customers(
            min_customer_reviews=5,  # Customers who've reviewed 5+ products
            k=10
        )
        
        # Calculate average query time for benchmarking
        avg_query_time = (time1 + time2 + time3 + time4) / 4
        
    finally:
        # Always close resources even if errors occur
        query_algo.close()
    
    # =====================
    # PATTERN MINING PHASE
    # =====================
    # Mine co-purchase patterns using FP-Growth algorithm
    
    pattern_miner = SparkPatternMining()
    
    try:
        # Split data into training (70%) and test (30%) sets
        # Training: Used to discover patterns
        # Testing: Used to validate patterns are real
        train_count, test_count = pattern_miner.split_train_test_spark(train_ratio=0.7)
        
        # Mine patterns from training data using FP-Growth
        # Parameters:
        # - min_support=0.02: Pattern must appear in 2% of transactions
        # - min_confidence=0.1: 10% probability threshold for rules
        # - max_customers=500: Limit for memory management
        train_patterns = pattern_miner.mine_patterns_with_fpgrowth(
            min_support=0.02,
            min_confidence=0.1,
            max_customers=500,
            dataset='train'  # Use training data only
        )
        
        # Validate patterns against test data
        # This confirms patterns aren't just noise in the training data
        validated = pattern_miner.validate_patterns(train_patterns, max_customers=500)
        
    finally:
        pattern_miner.close()
    
    overall_time = time.time() - overall_start


# Only run if executed directly (not imported as module)
if __name__ == "__main__":
    main()
