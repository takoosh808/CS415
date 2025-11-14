"""
Co-purchasing Pattern Mining Algorithm using PySpark MLlib
This algorithm finds frequent co-purchasing patterns in the dataset
"""

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, collect_list, size, explode, array
from pyspark.ml.fpm import FPGrowth
import time


class CoPurchasingAlgorithm:
    """
    Algorithm to find frequent co-purchasing patterns
    Input: Transaction data (products bought together), minimum support threshold
    Output: Frequent patterns and their frequencies
    """
    
    def __init__(self):
        self.spark = None
        self.transactions_df = None
        
    def initialize_spark(self):
        """Initialize Spark session"""
        print("Initializing Spark session...")
        
        self.spark = SparkSession.builder \
            .appName("CoPurchasingPattern") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session initialized")
        
    def load_data_from_neo4j(self):
        """Load co-purchasing data from Neo4j"""
        print("Loading co-purchasing data from Neo4j...")
        
        from neo4j import GraphDatabase
        
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                     auth=("neo4j", "Password"))
        
        # Get customer purchase transactions (products reviewed by same customer)
        with driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[:REVIEWED]->(p:Product)
                RETURN c.customer_id as customer_id, 
                       collect(p.asin) as products
                ORDER BY customer_id
            """)
            
            transactions_data = []
            for record in result:
                # Only include customers who reviewed multiple products
                if len(record['products']) > 1:
                    transactions_data.append({
                        'customer_id': record['customer_id'],
                        'products': record['products']
                    })
        
        # Get co-purchase relationships
        with driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (p1:Product)-[co:CO_PURCHASED_WITH]-(p2:Product)
                RETURN p1.asin as product1, p2.asin as product2, 
                       co.count as count
                ORDER BY count DESC
                LIMIT 100
            """)
            
            copurchase_data = []
            for record in result:
                copurchase_data.append({
                    'product1': record['product1'],
                    'product2': record['product2'],
                    'count': record['count']
                })
        
        driver.close()
        
        # Create Spark DataFrames and ensure unique items per transaction
        # FPGrowth requires unique items in each transaction
        from pyspark.sql.functions import array_distinct
        
        transactions_df_raw = self.spark.createDataFrame(transactions_data)
        self.transactions_df = transactions_df_raw.withColumn("products", array_distinct("products"))
        self.copurchase_df = self.spark.createDataFrame(copurchase_data) if copurchase_data else None
        
        print(f"Loaded {self.transactions_df.count()} customer transactions")
        if self.copurchase_df:
            print(f"Loaded {self.copurchase_df.count()} co-purchase relationships")
        
    def find_frequent_patterns(self, min_support=0.05, min_confidence=0.1):
        """
        Find frequent co-purchasing patterns using FP-Growth algorithm
        
        Args:
            min_support: minimum support threshold (default 0.05 = 5%)
            min_confidence: minimum confidence for association rules
            
        Returns:
            frequent patterns and association rules
        """
        print(f"\nFinding frequent patterns (min_support={min_support}, min_confidence={min_confidence})...")
        start_time = time.time()
        
        # Use FP-Growth algorithm from Spark MLlib
        fp_growth = FPGrowth(
            itemsCol="products",
            minSupport=min_support,
            minConfidence=min_confidence
        )
        
        # Fit the model
        model = fp_growth.fit(self.transactions_df)
        
        # Get frequent itemsets
        frequent_itemsets = model.freqItemsets.collect()
        
        # Get association rules
        association_rules = model.associationRules.collect()
        
        execution_time = time.time() - start_time
        
        print(f"Found {len(frequent_itemsets)} frequent patterns in {execution_time:.3f} seconds")
        print(f"Generated {len(association_rules)} association rules")
        
        return frequent_itemsets, association_rules, execution_time
    
    def analyze_copurchasing_by_customers(self):
        """
        Analyze which products are frequently co-purchased
        Returns products that appear together in customer transactions
        Uses native Spark operations - no UDFs for better stability
        """
        print("\nAnalyzing co-purchasing patterns by customer transactions...")
        start_time = time.time()
        
        # Simplified approach using existing co-purchase data
        # Group by product pairs and count occurrences
        if self.copurchase_df:
            pairs_counted = self.copurchase_df.groupBy("product1", "product2") \
                .agg({"count": "sum"}) \
                .withColumnRenamed("sum(count)", "frequency")
            
            top_patterns = pairs_counted.orderBy(desc("frequency")).limit(20).collect()
        else:
            # Fallback: Use self-join on transactions to find co-purchases
            trans_alias1 = self.transactions_df.alias("t1")
            trans_alias2 = self.transactions_df.alias("t2")
            
            # Explode both and join on same customer_id
            from pyspark.sql.functions import explode_outer, min as spark_min
            
            exploded1 = trans_alias1.select(
                col("t1.customer_id"),
                explode_outer(col("t1.products")).alias("product1")
            )
            
            exploded2 = trans_alias2.select(
                col("t2.customer_id"),
                explode_outer(col("t2.products")).alias("product2")
            )
            
            # Join and filter
            pairs_counted = exploded1.join(exploded2, "customer_id") \
                .filter(col("product1") < col("product2")) \
                .groupBy("product1", "product2") \
                .agg(count("*").alias("frequency"))
            
            top_patterns = pairs_counted.orderBy(desc("frequency")).limit(20).collect()
        
        execution_time = time.time() - start_time
        
        total_pairs = pairs_counted.count() if pairs_counted else 0
        print(f"Found {total_pairs} unique product pairs")
        print(f"Analysis completed in {execution_time:.3f} seconds")
        
        return top_patterns, execution_time
    
    def split_data_train_test(self, train_ratio=0.7):
        """
        Split transaction data into training and testing sets
        
        Args:
            train_ratio: ratio of data to use for training
            
        Returns:
            training and testing DataFrames
        """
        print(f"\nSplitting data: {train_ratio*100}% training, {(1-train_ratio)*100}% testing...")
        
        train_df, test_df = self.transactions_df.randomSplit([train_ratio, 1-train_ratio], seed=42)
        
        print(f"Training set: {train_df.count()} transactions")
        print(f"Testing set: {test_df.count()} transactions")
        
        return train_df, test_df
    
    def validate_patterns(self, train_df, test_df, min_support=0.05):
        """
        Validate patterns found in training set against test set
        
        Args:
            train_df: training transactions
            test_df: testing transactions
            min_support: minimum support threshold
            
        Returns:
            validation results
        """
        print("\nValidating patterns in training vs testing data...")
        start_time = time.time()
        
        # Find patterns in training set
        fp_growth = FPGrowth(
            itemsCol="products",
            minSupport=min_support,
            minConfidence=0.1
        )
        
        train_model = fp_growth.fit(train_df)
        train_patterns = train_model.freqItemsets
        
        print(f"Found {train_patterns.count()} patterns in training set")
        
        # Check patterns in test set
        test_model = fp_growth.fit(test_df)
        test_patterns = test_model.freqItemsets
        
        print(f"Found {test_patterns.count()} patterns in test set")
        
        # Get top patterns from both
        top_train = train_patterns.orderBy(desc("freq")).limit(10).collect()
        top_test = test_patterns.orderBy(desc("freq")).limit(10).collect()
        
        execution_time = time.time() - start_time
        
        print(f"Validation completed in {execution_time:.3f} seconds")
        
        return top_train, top_test, execution_time
    
    def get_most_significant_patterns(self):
        """
        Identify the most significant co-purchasing patterns
        Based on frequency and support
        """
        print("\nIdentifying most significant co-purchasing patterns...")
        
        if self.copurchase_df:
            # Use existing co-purchase relationships
            top_copurchase = self.copurchase_df.orderBy(desc("count")).limit(10).collect()
            return top_copurchase
        
        return []
    
    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped")


if __name__ == "__main__":
    # Test the algorithm
    algo = CoPurchasingAlgorithm()
    algo.initialize_spark()
    algo.load_data_from_neo4j()
    
    # Find frequent patterns
    patterns, rules, time1 = algo.find_frequent_patterns(min_support=0.05)
    
    print("\nTop 10 Frequent Patterns:")
    for i, pattern in enumerate(patterns[:10]):
        print(f"{i+1}. {pattern.items} - Frequency: {pattern.freq}")
    
    print("\nTop 10 Association Rules:")
    for i, rule in enumerate(rules[:10]):
        print(f"{i+1}. {rule.antecedent} => {rule.consequent} "
              f"(confidence: {rule.confidence:.3f}, lift: {rule.lift:.3f})")
    
    # Analyze co-purchasing by customers
    top_pairs, time2 = algo.analyze_copurchasing_by_customers()
    
    print("\nTop 10 Co-purchased Product Pairs:")
    for i, pair in enumerate(top_pairs[:10]):
        print(f"{i+1}. {pair.product1} + {pair.product2} - Frequency: {pair.frequency}")
    
    # Split and validate
    train_df, test_df = algo.split_data_train_test(0.7)
    train_patterns, test_patterns, time3 = algo.validate_patterns(train_df, test_df)
    
    print("\nTop Training Set Patterns:")
    for pattern in train_patterns[:5]:
        print(f"  {pattern.items} - Frequency: {pattern.freq}")
    
    print("\nTop Testing Set Patterns:")
    for pattern in test_patterns[:5]:
        print(f"  {pattern.items} - Frequency: {pattern.freq}")
    
    algo.stop_spark()
