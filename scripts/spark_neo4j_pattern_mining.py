from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    collect_list, col, explode, size, array_distinct, 
    count, lit, rand, when, array, struct
)
from pyspark.ml.fpm import FPGrowth
import time
import json
import sys

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')



class SparkNeo4jPatternMining:
    
    def __init__(self, neo4j_uri="bolt://localhost:7687", neo4j_user="neo4j", neo4j_password="Password"):
        """Initialize Spark session with Neo4j connector"""
        print("Initializing Spark with Neo4j connector...")
        
        self.spark = SparkSession.builder \
            .appName("Neo4j Co-Purchase Pattern Mining") \
            .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3") \
            .config("neo4j.url", neo4j_uri) \
            .config("neo4j.authentication.basic.username", neo4j_user) \
            .config("neo4j.authentication.basic.password", neo4j_password) \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.sql.shuffle.partitions", "16") \
            .config("spark.default.parallelism", "16") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.maxResultSize", "2g") \
            .getOrCreate()
        
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
        print(f"Spark version: {self.spark.version}")
        print(f"Connected to Neo4j at {neo4j_uri}")
    
    def load_reviews_from_neo4j(self):
        """Load all reviews from Neo4j using Spark connector"""
        print("\nLoading reviews from Neo4j using Spark connector...")
        print("⚠ TESTING MODE: Will limit to ~10,000 reviews for validation")
        start_time = time.time()
        
        # Read reviews directly from Neo4j into Spark DataFrame
        # Note: LIMIT not supported in connector query, so we filter after loading
        reviews_df = self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_uri) \
            .option("authentication.basic.username", self.neo4j_user) \
            .option("authentication.basic.password", self.neo4j_password) \
            .option("query", """
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                RETURN c.id as customer_id, 
                       p.asin as product_id, 
                       r.rating as rating,
                       r.dataset as dataset
            """) \
            .load()
        
        # TESTING MODE: Limit to first 10,000 reviews
        reviews_df = reviews_df.limit(10000)
        
        # Cache for performance
        reviews_df.cache()
        
        total_count = reviews_df.count()
        elapsed = time.time() - start_time
        
        print(f"✓ Loaded {total_count:,} reviews in {elapsed:.2f}s")
        print(f"  Throughput: {total_count/elapsed:,.0f} reviews/sec")
        
        return reviews_df
    
    def split_train_test_spark(self, reviews_df, train_ratio=0.7):
        """Split reviews using Spark (if not already split in Neo4j)"""
        print(f"\nChecking train/test split...")
        
        # Check if already split
        has_split = reviews_df.filter(col("dataset").isNotNull()).count() > 0
        
        if has_split:
            train_count = reviews_df.filter(col("dataset") == "train").count()
            test_count = reviews_df.filter(col("dataset") == "test").count()
            print(f"✓ Using existing split:")
            print(f"  Training: {train_count:,} reviews ({train_count/(train_count+test_count)*100:.1f}%)")
            print(f"  Testing: {test_count:,} reviews ({test_count/(train_count+test_count)*100:.1f}%)")
            return reviews_df
        
        # Create new split
        print(f"Creating {train_ratio*100:.0f}/{(1-train_ratio)*100:.0f} train/test split...")
        reviews_with_split = reviews_df.withColumn(
            "dataset",
            when(rand(seed=42) < train_ratio, lit("train")).otherwise(lit("test"))
        )
        
        train_count = reviews_with_split.filter(col("dataset") == "train").count()
        test_count = reviews_with_split.filter(col("dataset") == "test").count()
        
        print(f"✓ Split complete:")
        print(f"  Training: {train_count:,} reviews ({train_count/(train_count+test_count)*100:.1f}%)")
        print(f"  Testing: {test_count:,} reviews ({test_count/(train_count+test_count)*100:.1f}%)")
        
        return reviews_with_split
    
    def mine_patterns_fpgrowth(self, reviews_df, min_support=0.005, min_confidence=0.1, dataset='train', max_items_per_customer=50):
        """Mine frequent patterns using Spark MLlib FP-Growth"""
        print(f"\n{'='*70}")
        print(f"MINING PATTERNS WITH FP-GROWTH (Dataset: {dataset.upper()})")
        print(f"{'='*70}")
        
        start_time = time.time()
        
        # Filter to specified dataset (unless testing with small data)
        dataset_reviews = reviews_df.filter(col("dataset") == dataset)
        dataset_count = dataset_reviews.count()
        
        # TESTING MODE: If too few reviews in the filtered set, use all reviews
        if dataset_count < 100:
            print(f"⚠ Only {dataset_count} reviews in '{dataset}' set. Using all {reviews_df.count()} reviews for testing.")
            dataset_reviews = reviews_df
        
        # Group by customer to create transactions - ensure unique products and limit items
        print(f"Creating customer transactions (unique products, max {max_items_per_customer} items per customer)...")
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        # First deduplicate products within each customer
        deduplicated = dataset_reviews.dropDuplicates(["customer_id", "product_id"])
        
        # Add row number within each customer partition to limit items
        window_spec = Window.partitionBy("customer_id").orderBy(rand())
        limited_reviews = deduplicated.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") <= max_items_per_customer) \
            .drop("row_num")
        
        # Count items per customer to filter out extreme cases
        transactions = limited_reviews.groupBy("customer_id") \
            .agg(collect_list("product_id").alias("items")) \
            .filter((size("items") >= 2) & (size("items") <= max_items_per_customer))
        
        num_transactions = transactions.count()
        print(f"✓ Created {num_transactions:,} customer transactions")
        
        # TESTING MODE: Disabled sampling for small dataset
        # Sample transactions if too many to avoid memory issues
        # if num_transactions > 500000:
        #     sample_fraction = 500000 / num_transactions
        #     print(f"⚠ Sampling {sample_fraction:.1%} of transactions to avoid memory issues...")
        #     transactions = transactions.sample(False, sample_fraction, seed=42)
        #     num_transactions = transactions.count()
        #     print(f"✓ Using {num_transactions:,} sampled transactions")
        
        # Cache transactions to speed up FP-Growth
        transactions = transactions.cache()
        transactions.count()  # Force cache
        
        # Run FP-Growth with higher min_support for large datasets
        print(f"\nRunning FP-Growth algorithm...")
        print(f"  Min Support: {min_support} ({int(min_support * num_transactions)} transactions)")
        print(f"  Min Confidence: {min_confidence}")
        
        fpGrowth = FPGrowth(
            itemsCol="items", 
            minSupport=min_support, 
            minConfidence=min_confidence
        )
        
        try:
            model = fpGrowth.fit(transactions)
        except Exception as e:
            print(f"\n✗ FP-Growth failed: {str(e)}")
            print("⚠ This can happen with very large or skewed datasets")
            transactions.unpersist()
            return None
        
        # Get frequent itemsets (filter for pairs only)
        print("\nExtracting frequent itemsets...")
        frequent_itemsets = model.freqItemsets \
            .filter(size("items") == 2) \
            .orderBy(col("freq").desc())
        
        num_patterns = frequent_itemsets.count()
        
        # Get association rules
        print("Generating association rules...")
        rules = model.associationRules \
            .filter(size("antecedent") == 1) \
            .filter(size("consequent") == 1) \
            .orderBy(col("confidence").desc(), col("lift").desc())
        
        elapsed = time.time() - start_time
        
        print(f"\n✓ Pattern mining complete in {elapsed:.2f}s")
        print(f"  Found {num_patterns:,} frequent pairs")
        print(f"  Generated {rules.count():,} association rules")
        
        return {
            'transactions': transactions,
            'itemsets': frequent_itemsets,
            'rules': rules,
            'num_transactions': num_transactions,
            'elapsed_time': elapsed
        }
    
    def validate_patterns_cross_dataset(self, train_patterns, test_reviews_df, min_support=5):
        """Validate training patterns in test set"""
        print(f"\n{'='*70}")
        print("VALIDATING PATTERNS IN TEST SET")
        print(f"{'='*70}")
        
        # Get top patterns from training
        train_itemsets = train_patterns['itemsets'].limit(50).collect()
        
        # Create test transactions
        test_transactions = test_reviews_df \
            .filter(col("dataset") == "test") \
            .groupBy("customer_id") \
            .agg(collect_list("product_id").alias("items"))
        
        validated_patterns = []
        
        print(f"\nValidating top {len(train_itemsets)} patterns...")
        
        for itemset_row in train_itemsets:
            products = sorted(itemset_row['items'])
            train_support = itemset_row['freq']
            
            # Count occurrences in test set
            test_support = test_transactions.filter(
                array(lit(products[0]), lit(products[1])).isSubsetOf(col("items"))
            ).count()
            
            if test_support >= min_support:
                validated_patterns.append({
                    'products': products,
                    'train_support': train_support,
                    'test_support': test_support,
                    'total_support': train_support + test_support
                })
        
        print(f"✓ {len(validated_patterns)} patterns validated in test set (min_support={min_support})")
        
        return validated_patterns
    
    def enrich_patterns_with_product_info(self, patterns):
        """Fetch product titles and ratings from Neo4j"""
        print("\nEnriching patterns with product information from Neo4j...")
        
        enriched = []
        
        for pattern in patterns[:25]:  # Top 25 patterns
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
            
            product_info = {row['asin']: {'title': row['title'], 'rating': row['rating']} for row in product_df}
            
            enriched.append({
                'products': products,
                'titles': [product_info.get(asin, {}).get('title', 'Unknown') for asin in products],
                'ratings': [product_info.get(asin, {}).get('rating', 0.0) for asin in products],
                'train_support': pattern['train_support'],
                'test_support': pattern['test_support'],
                'total_support': pattern['total_support'],
                'confidence': pattern.get('confidence', 0.0)
            })
        
        print(f"✓ Enriched {len(enriched)} patterns with product details")
        
        return enriched
    
    def save_results(self, results, output_file='spark_neo4j_results.json'):
        """Save results to JSON file"""
        print(f"\nSaving results to {output_file}...")
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"✓ Results saved to {output_file}")
    
    def close(self):
        """Stop Spark session"""
        print("\nStopping Spark session...")
        self.spark.stop()
        print("✓ Spark session stopped")


def main():
    """Main execution pipeline"""
    print("="*70)
    print("CO-PURCHASE PATTERN MINING WITH NEO4J CONNECTOR FOR SPARK")
    print("="*70)
    
    # Initialize
    miner = SparkNeo4jPatternMining()
    
    # Load all reviews from Neo4j
    reviews_df = miner.load_reviews_from_neo4j()
    
    # Ensure train/test split
    reviews_df = miner.split_train_test_spark(reviews_df, train_ratio=0.7)
    
    # Mine patterns in training set using FP-Growth
    # TESTING MODE: Lower threshold for small dataset
    train_results = miner.mine_patterns_fpgrowth(
        reviews_df, 
        min_support=0.001,  # Lower threshold for small test dataset
        min_confidence=0.1,
        dataset='train'
    )
    
    if train_results is None:
        print("\n✗ Pattern mining failed. Exiting.")
        miner.spark.stop()
        return
    
    print("\n=== TOP 10 FREQUENT PAIRS (Training Set) ===")
    top_patterns = train_results['itemsets'].limit(10).collect()
    for i, row in enumerate(top_patterns, 1):
        products = sorted(row['items'])
        support = row['freq']
        print(f"{i}. Products: {products}")
        print(f"   Support: {support} customers")
        print()
    
    # Convert to validation format
    patterns_for_validation = []
    for row in train_results['itemsets'].limit(50).collect():
        patterns_for_validation.append({
            'products': sorted(row['items']),
            'train_support': row['freq'],
            'test_support': 0,
            'total_support': row['freq']
        })
    
    # Validate in test set
    validated_patterns = miner.validate_patterns_cross_dataset(
        train_results,
        reviews_df,
        min_support=5
    )
    
    print("\n=== TOP 10 VALIDATED PATTERNS (Test Set) ===")
    for i, pattern in enumerate(sorted(validated_patterns, key=lambda x: x['total_support'], reverse=True)[:10], 1):
        print(f"{i}. Products: {pattern['products']}")
        print(f"   Train Support: {pattern['train_support']}")
        print(f"   Test Support: {pattern['test_support']}")
        print(f"   Total Support: {pattern['total_support']}")
        print()
    
    # Enrich with product information
    enriched_patterns = miner.enrich_patterns_with_product_info(validated_patterns)
    
    # Save results
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
    
    miner.save_results(results, 'spark_neo4j_patterns.json')
    
    # Clean up
    miner.close()
    
    print("\n" + "="*70)
    print("PATTERN MINING COMPLETE")
    print("="*70)


if __name__ == "__main__":
    main()
