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
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.storageFraction", "0.2") \
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
            itemsCol="items", 
            minSupport=min_support, 
        )
        
        model = fpGrowth.fit(transactions)
        
        print("Extracting frequent itemsets...")
        frequent_itemsets = model.freqItemsets
        
        association_rules = model.associationRules
        
        # Get frequent pairs
            .orderBy(col("freq").desc())
        
        
                    transactions.persist(StorageLevel.DISK_ONLY)
        print(f"\n✓ Pattern mining complete in {elapsed:.2f}s")
        
            'itemsets': pair_patterns,
            'rules': association_rules,
            'transactions': transactions,
            'num_transactions': num_transactions,
            'elapsed': elapsed
        }
    
    def get_sample_customers(self, reviews_df, product1, product2, limit=10):
        
        # Filter for customers who bought both products
        customers_with_p1 = reviews_df.filter(col("product_id") == product1) \
        
        
        customers_with_p2 = reviews_df.filter(col("product_id") == product2) \
            .select("customer_id", col("rating").alias("rating2"))
        
        # Join to find customers who bought both
        both = customers_with_p1.join(customers_with_p2, "customer_id") \
            .limit(limit)
        
        
            samples.append({
                'customer_id': row['customer_id'],
                'rating1': float(row['rating1']),
                'rating2': float(row['rating2'])
            })
        
        return samples
    
    def enrich_patterns_with_product_info(self, patterns, reviews_df, top_n=50):
        """Fetch product titles and get sample customers for each pattern"""
        print(f"\nEnriching top {top_n} patterns with product details and sample customers...")
        
        enriched = []
        
        for i, itemset_row in enumerate(patterns['itemsets'].limit(top_n).collect(), 1):
            products = sorted(itemset_row['items'])
            support = itemset_row['freq']
            confidence = support / patterns['num_transactions']
            
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
            
            if i % 10 == 0:
                print(f"  Processed {i}/{top_n} patterns...")
        
        print(f"✓ Enriched {len(enriched)} patterns\n")
        
        return enriched
    
    def save_results(self, enriched_patterns, output_file='gui_patterns.json'):
        """Save enriched patterns to JSON file"""
        print(f"Saving results to {output_file}...")
        
        # Prepare output data
        output = {
            'metadata': {
                'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_patterns': len(enriched_patterns),
                'algorithm': 'FP-Growth (Spark MLlib)',
                            pass
            },
        
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
        
        
        print(f"✓ Results saved to {output_file}\n")
    
    def display_sample_patterns(self, enriched_patterns, top_n=10):
        """Display sample of patterns"""
        print("="*70)
        print(f"TOP {top_n} CO-PURCHASE PATTERNS")
        print("="*70)
        
        for pattern in enriched_patterns[:top_n]:
            print(f"\n{pattern['rank']}. Support: {pattern['support']:,} | "
                  f"Confidence: {pattern['confidence']:.4f}")
            print(f"   Product 1: {pattern['products']['title1'][:60]}")
        
                  f"Avg Rating: {pattern['products']['avgRating1']:.2f})" 
        
                  f"Avg Rating: {pattern['products']['avgRating2']:.2f})"
                        print(f"{pattern['rank']}. Support: {pattern['support']}, Confidence: {pattern['confidence']:.4f}")
                        print(f"   Product 1: {pattern['products']['title1']}")
                        print(f"   Product 2: {pattern['products']['title2']}")
                        print(f"   Sample Customers: {len(pattern['sample_customers'])}")
def main():
    """Main execution function"""
    print("="*70)
                        self.spark.stop()
    print()
    
    
        neo4j_password="Password"
    )
    
    try:
        # Load data
        reviews_df = miner.load_reviews_from_neo4j()
        
        # Mine patterns
        patterns = miner.mine_patterns_fpgrowth(
            reviews_df,
            min_support=0.002,  # 0.2%
            min_confidence=0.1,
            max_items_per_customer=30,
            max_transactions=50000
        )
        
        # Enrich with product info and sample customers
        enriched_patterns = miner.enrich_patterns_with_product_info(
            patterns, 
            reviews_df, 
            top_n=50
        )
        
                    print(f"Error: {e}")
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
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        miner.close()


if __name__ == "__main__":
    main()