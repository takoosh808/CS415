from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, collect_set, size, array_contains, slice as array_slice
from pyspark.ml.fpm import FPGrowth
from pyspark import StorageLevel
import time
import json
import os

class PatternPregeneration:
    
    def __init__(self, neo4j_uri="bolt://localhost:7687", neo4j_user="neo4j", neo4j_password="Password"):
        """Initialize Spark session with Neo4j connector"""
        print("Initializing Spark with Neo4j connector...")
        
        self.spark = SparkSession.builder \
            .appName("Pregenerate Pattern Mining Results") \
            .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.13:5.3.1_for_spark_3") \
            .config("neo4j.url", neo4j_uri) \
            .config("neo4j.authentication.basic.username", neo4j_user) \
            .config("neo4j.authentication.basic.password", neo4j_password) \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.maxResultSize", "4g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
        print(f"Spark version: {self.spark.version}")
        print(f"Connected to Neo4j at {neo4j_uri}\n")
    
    def load_reviews_from_neo4j(self):
        """Load all reviews from Neo4j using Spark connector"""
        print("Loading reviews from Neo4j cluster...")
        start_time = time.time()
        
        reviews_df = self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_uri) \
            .option("authentication.basic.username", self.neo4j_user) \
            .option("authentication.basic.password", self.neo4j_password) \
            .option("query", """
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE c.id IS NOT NULL AND p.asin IS NOT NULL
                RETURN c.id as customer_id, 
                       p.asin as product_id, 
                       r.rating as rating
            """) \
            .load()
        
        reviews_df.persist(StorageLevel.DISK_ONLY)
        total_count = reviews_df.count()
        elapsed = time.time() - start_time
        
        print(f"✓ Loaded {total_count:,} reviews in {elapsed:.2f}s")
        print(f"  Throughput: {total_count/elapsed:,.0f} reviews/sec\n")
        
        return reviews_df
    
    def mine_patterns_fpgrowth(self, reviews_df, min_support=0.002, min_confidence=0.1, 
                               max_items_per_customer=30, max_transactions=50000):
        """Mine patterns using FP-Growth with memory optimization"""
        print("="*70)
        print("MINING FREQUENT CO-PURCHASE PATTERNS")
        print("="*70)
        
        start_time = time.time()
        
        # Create customer transactions
        print("Creating customer transactions (unique products per customer)...")
        transactions = reviews_df \
            .groupBy("customer_id") \
            .agg(collect_set("product_id").alias("items")) \
            .filter(size(col("items")) >= 2) \
            .withColumn("items", array_slice(col("items"), 1, max_items_per_customer))
        
        num_transactions = transactions.count()
        print(f"✓ Created {num_transactions:,} customer transactions")
        
        # Sample if needed
        if num_transactions > max_transactions:
            sample_fraction = max_transactions / num_transactions
            print(f"⚠ Sampling {sample_fraction:.1%} of transactions to avoid memory issues...")
            transactions = transactions.sample(False, sample_fraction, seed=42)
            num_transactions = transactions.count()
            print(f"✓ Using {num_transactions:,} sampled transactions")
        
        
        
        # Run FP-Growth
        print(f"\nRunning FP-Growth algorithm...")
        print(f"  Min Support: {min_support} ({int(min_support * num_transactions)} transactions)")
        
        fpGrowth = FPGrowth(
            itemsCol="items", 
            minSupport=min_support, 
            minConfidence=min_confidence
        )
        
        model = fpGrowth.fit(transactions)
        
        print("Extracting frequent itemsets...")
        frequent_itemsets = model.freqItemsets
        
        association_rules = model.associationRules
        
        # Get frequent pairs
        pair_patterns = frequent_itemsets.filter(size(col("items")) == 2) \
            .orderBy(col("freq").desc())
        
        transactions.persist(StorageLevel.DISK_ONLY)
        elapsed = time.time() - start_time
        print(f"\n✓ Pattern mining complete in {elapsed:.2f}s")
        
        return {
            'itemsets': pair_patterns,
            'rules': association_rules,
            'transactions': transactions,
            'num_transactions': num_transactions,
            'elapsed': elapsed
        }
    
    def get_sample_customers(self, reviews_df, product1, product2, limit=10):
        """Get sample customers who bought both products"""
        
        # Filter for customers who bought both products
        customers_with_p1 = reviews_df.filter(col("product_id") == product1) \
            .select("customer_id", col("rating").alias("rating1"))
        
        customers_with_p2 = reviews_df.filter(col("product_id") == product2) \
            .select("customer_id", col("rating").alias("rating2"))
        
        # Join to find customers who bought both
        both = customers_with_p1.join(customers_with_p2, "customer_id") \
            .limit(limit)
        
        samples = []
        for row in both.collect():
            samples.append({
                'customer_id': row['customer_id'],
                'rating1': float(row['rating1']),
                'rating2': float(row['rating2'])
            })
        
        return samples
    
    def enrich_patterns_with_product_info(self, patterns, reviews_df):
        """Fetch product titles and get sample customers for each pattern"""
        
        # First get the total count of patterns
        total_patterns = patterns['itemsets'].count()
        print(f"\nFound {total_patterns:,} total frequent patterns")
        print(f"Enriching all {total_patterns:,} patterns with product details and sample customers...")
        
        enriched = []
        
        for i, itemset_row in enumerate(patterns['itemsets'].collect(), 1):
            products = sorted(itemset_row['items'])
            support = itemset_row['freq']
            confidence = support / patterns['num_transactions']
            
            # Show progress
            if i == 1 or i % 10 == 0 or i == total_patterns:
                print(f"  [{i:,}/{total_patterns:,}] Processing pattern: {products[0][:15]}... & {products[1][:15]}...")
            
            # Query Neo4j for product details
            product_df = self.spark.read.format("org.neo4j.spark.DataSource") \
                .option("url", self.neo4j_uri) \
                .option("authentication.basic.username", self.neo4j_user) \
                .option("authentication.basic.password", self.neo4j_password) \
                .option("query", f"""
                    MATCH (p:Product)
                    WHERE p.asin IN ['{products[0]}', '{products[1]}']
                    RETURN p.asin as asin, p.title as title, p.avgRating as avgRating
                """) \
                .load()
            
            product_info = {}
            for row in product_df.collect():
                product_info[row['asin']] = {
                    'title': row['title'],
                    'avgRating': float(row['avgRating']) if row['avgRating'] else None
                }
            
            # Get sample customers
            sample_customers = self.get_sample_customers(reviews_df, products[0], products[1], limit=10)
            
            enriched.append({
                'rank': i,
                'products': {
                    'asin1': products[0],
                    'asin2': products[1],
                    'title1': product_info.get(products[0], {}).get('title', 'Unknown'),
                    'title2': product_info.get(products[1], {}).get('title', 'Unknown'),
                    'avgRating1': product_info.get(products[0], {}).get('avgRating'),
                    'avgRating2': product_info.get(products[1], {}).get('avgRating')
                },
                'support': support,
                'confidence': confidence,
                'sample_customers': sample_customers
            })
            
        print(f"✓ Enriched {len(enriched):,} patterns\n")
        
        return enriched
    
    def save_results(self, enriched_patterns, output_file='gui_patterns.json'):
        """Save enriched patterns to JSON file"""
        print(f"Saving results to {output_file}...")
        
        # Prepare output data
        output = {
            'metadata': {
                'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_patterns': len(enriched_patterns),
                'algorithm': 'FP-Growth (Spark MLlib)'
            },
            'patterns': enriched_patterns
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Results saved to {output_file}\n")
    
    def display_sample_patterns(self, enriched_patterns, top_n=10):
        """Display sample of patterns"""
        print("="*70)
        print(f"TOP {top_n} CO-PURCHASE PATTERNS")
        print("="*70)
        
        for pattern in enriched_patterns[:top_n]:
            print(f"\n{pattern['rank']}. Support: {pattern['support']:,} | "
                  f"Confidence: {pattern['confidence']:.4f}")
            
            rating1 = pattern['products']['avgRating1']
            rating1_str = f"{rating1:.2f}" if rating1 is not None else "N/A"
            rating2 = pattern['products']['avgRating2']
            rating2_str = f"{rating2:.2f}" if rating2 is not None else "N/A"
            
            print(f"   Product 1: {pattern['products']['title1'][:60]} "
                  f"(Avg Rating: {rating1_str})")
            print(f"   Product 2: {pattern['products']['title2'][:60]} "
                  f"(Avg Rating: {rating2_str})")
            print(f"   Sample Customers: {len(pattern['sample_customers'])}")
    def close(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()

def main():
    """Main execution function"""
    print("="*70)
    print("PREGENERATE PATTERN MINING RESULTS")
    print("="*70)
    print()
    
    miner = PatternPregeneration(
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="Password"
    )
    
    try:
        # Load data
        reviews_df = miner.load_reviews_from_neo4j()
        
        # Mine patterns
        patterns = miner.mine_patterns_fpgrowth(
            reviews_df,
            min_support=0.0005,  # 0.05% - lower threshold for more patterns
            min_confidence=0.1,
            max_items_per_customer=20,  # Reduced from 30 to optimize memory
            max_transactions=2000000  # Allow up to 2M transactions (all data)
        )
        
        # Enrich with product info and sample customers
        enriched_patterns = miner.enrich_patterns_with_product_info(
            patterns, 
            reviews_df
        )
        
        # Display sample patterns
        miner.display_sample_patterns(enriched_patterns, top_n=10)
        
        # Save results
        miner.save_results(enriched_patterns, output_file='gui_patterns.json')
        
        print("="*70)
        print("PREGENERATION COMPLETE")
        print("="*70)
        print(f"Total patterns generated: {len(enriched_patterns)}")
        print(f"Output file: gui_patterns.json")
        print()
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        miner.close()


if __name__ == "__main__":
    main()