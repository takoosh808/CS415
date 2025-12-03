"""
Pattern Pre-generation Script for Amazon Co-Purchase Analytics
==============================================================

This script uses Apache Spark and the FP-Growth algorithm to discover products
that are frequently purchased together. The results are saved to a JSON file
that can be loaded by the GUI application for fast display without re-computation.

KEY CONCEPTS FOR BEGINNERS:
---------------------------

1. Apache Spark:
   A distributed computing framework that can process massive datasets by
   splitting work across multiple CPU cores or machines. Even on a single
   computer, Spark efficiently uses all available cores.

2. FP-Growth (Frequent Pattern Growth):
   A data mining algorithm that finds "frequent itemsets" - combinations of
   items that appear together often. In our case, we're finding product pairs
   that customers frequently buy together.
   
   Example: If many customers buy "Harry Potter Book" AND "Harry Potter DVD",
   these form a frequent pattern.

3. Key Pattern Mining Terms:
   - Transaction: A collection of items (here, all products a customer bought)
   - Support: How often a pattern appears (number of customers with both products)
   - Confidence: P(B|A) - probability of buying B given they bought A
   - Itemset: A set of items that appear together

4. Neo4j Graph Database:
   We store customer-product relationships in Neo4j, where:
   - Nodes: Products, Customers
   - Relationships: REVIEWED (customer reviewed/bought a product)

WORKFLOW:
---------
1. Load review data from Neo4j into Spark DataFrames
2. Create transactions (group products by customer)
3. Run FP-Growth algorithm to find frequent product pairs
4. Enrich patterns with product titles and sample customers
5. Save results to JSON for the GUI

DEPENDENCIES:
- pyspark: Apache Spark Python API
- neo4j-connector-apache-spark: Spark connector for Neo4j
"""

from pyspark.sql import SparkSession  # Main entry point for Spark SQL functionality
from pyspark.sql.functions import col, collect_list, collect_set, size, array_contains, slice as array_slice
from pyspark.ml.fpm import FPGrowth  # Machine learning implementation of FP-Growth
from pyspark import StorageLevel  # Options for how/where to cache data
import time
import json
import os


class PatternPregeneration:
    """
    Handles the complete workflow for mining co-purchase patterns from Neo4j data.
    
    This class:
    1. Sets up a Spark session with Neo4j connectivity
    2. Loads customer review data from the graph database
    3. Runs FP-Growth pattern mining algorithm
    4. Enriches patterns with product details
    5. Saves results for the GUI to display
    
    Attributes:
    -----------
    spark : SparkSession
        The active Spark session for data processing
    neo4j_uri : str
        Connection URI for Neo4j (e.g., "bolt://localhost:7687")
    neo4j_user : str
        Neo4j username for authentication
    neo4j_password : str
        Neo4j password for authentication
    """
    
    def __init__(self, neo4j_uri="bolt://localhost:7687", neo4j_user="neo4j", neo4j_password="Password"):
        """
        Initialize the Spark session with Neo4j connector configuration.
        
        This sets up Spark with:
        - Neo4j connector for reading graph data directly into Spark DataFrames
        - Memory settings optimized for pattern mining on a single machine
        - Adaptive query execution for better performance
        
        Parameters:
        -----------
        neo4j_uri : str
            Bolt URI for Neo4j connection (default: localhost)
        neo4j_user : str
            Database username (default: "neo4j")
        neo4j_password : str
            Database password (default: "Password")
        
        SPARK CONFIGURATION EXPLAINED:
        ------------------------------
        - spark.driver.memory: Memory for the main Spark process (8GB)
        - spark.executor.memory: Memory for worker processes (8GB)
        - spark.sql.shuffle.partitions: Number of partitions for shuffled data
        - spark.default.parallelism: Default number of parallel tasks
        - spark.sql.adaptive.enabled: Enables dynamic optimization
        - spark.memory.fraction: Portion of heap for Spark storage/execution
        - spark.serializer: KryoSerializer is faster than default Java serialization
        """
        print("Initializing Spark with Neo4j connector...")
        
        # Build the Spark session with all necessary configurations
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
        
        # Store connection parameters for later use
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
        print(f"Spark version: {self.spark.version}")
        print(f"Connected to Neo4j at {neo4j_uri}\n")
    
    def load_reviews_from_neo4j(self):
        """
        Load all customer review data from Neo4j into a Spark DataFrame.
        
        This method executes a Cypher query that finds all REVIEWED relationships
        between Customers and Products, returning the customer ID, product ID,
        and rating for each review.
        
        CYPHER QUERY EXPLAINED:
        -----------------------
        MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
        
        This pattern matches:
        - (c:Customer): A node with label "Customer", assigned to variable 'c'
        - -[r:REVIEWED]->: A relationship of type "REVIEWED", assigned to 'r'
        - (p:Product): A node with label "Product", assigned to 'p'
        
        The arrow -> indicates direction: customer reviewed the product.
        
        Returns:
        --------
        DataFrame
            Spark DataFrame with columns: customer_id, product_id, rating
            The data is persisted to disk for efficient reuse.
        """
        print("Loading reviews from Neo4j cluster...")
        start_time = time.time()
        
        # Use Spark's Neo4j DataSource to execute a Cypher query
        # and load results directly into a DataFrame
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
        
        # Persist (cache) the DataFrame to disk for faster access later
        # DISK_ONLY means data is stored on disk, freeing up memory
        reviews_df.persist(StorageLevel.DISK_ONLY)
        
        # Count triggers actual loading (Spark uses lazy evaluation)
        total_count = reviews_df.count()
        elapsed = time.time() - start_time
        
        print(f"✓ Loaded {total_count:,} reviews in {elapsed:.2f}s")
        print(f"  Throughput: {total_count/elapsed:,.0f} reviews/sec\n")
        
        return reviews_df
    
    def mine_patterns_fpgrowth(self, reviews_df, min_support=0.002, min_confidence=0.1, 
                               max_items_per_customer=30, max_transactions=50000):
        """
        Mine frequent co-purchase patterns using the FP-Growth algorithm.
        
        FP-GROWTH ALGORITHM EXPLAINED:
        ------------------------------
        FP-Growth is an efficient algorithm for finding frequent patterns:
        
        1. First Pass: Count item frequencies and remove infrequent items
        2. Build FP-Tree: A compact tree structure storing transactions
        3. Mine Patterns: Extract frequent itemsets from the tree
        
        Unlike older algorithms (like Apriori), FP-Growth doesn't generate
        candidate patterns, making it much faster for large datasets.
        
        PARAMETERS EXPLAINED:
        ---------------------
        min_support : float (default: 0.002 = 0.2%)
            Minimum fraction of transactions containing the pattern.
            Pattern must appear in at least (min_support * num_transactions) transactions.
            Example: 0.002 with 50,000 transactions = 100 minimum occurrences
            
        min_confidence : float (default: 0.1 = 10%)
            Minimum confidence for association rules.
            Confidence(A→B) = P(B|A) = Support(A,B) / Support(A)
            Example: 10% confidence means if you buy A, there's at least
            10% chance you'll also buy B.
            
        max_items_per_customer : int (default: 30)
            Limit products per customer to avoid noise from bulk buyers.
            Some customers review hundreds of products, which can skew patterns.
            
        max_transactions : int (default: 50000)
            Maximum number of customer transactions to process.
            Sampling is used if we have more transactions than this limit.
        
        Returns:
        --------
        dict
            Contains:
            - 'itemsets': DataFrame of frequent product pairs
            - 'rules': DataFrame of association rules
            - 'transactions': DataFrame of customer transactions
            - 'num_transactions': Count of transactions processed
            - 'elapsed': Time taken for mining
        """
        print("="*70)
        print("MINING FREQUENT CO-PURCHASE PATTERNS")
        print("="*70)
        
        start_time = time.time()
        
        # STEP 1: Create customer transactions
        # ------------------------------------
        # A "transaction" is all the products a single customer purchased.
        # We use collect_set to get unique products per customer (no duplicates).
        print("Creating customer transactions (unique products per customer)...")
        
        transactions = reviews_df \
            .groupBy("customer_id") \
            .agg(collect_set("product_id").alias("items")) \
            .filter(size(col("items")) >= 2) \
            .withColumn("items", array_slice(col("items"), 1, max_items_per_customer))
        
        # groupBy: Group all rows by customer_id
        # collect_set: Aggregate product_ids into a set (unique values)
        # filter: Keep only customers with 2+ products (need pairs for patterns)
        # array_slice: Limit to first N products per customer
        
        num_transactions = transactions.count()
        print(f"✓ Created {num_transactions:,} customer transactions")
        
        # STEP 2: Sample if dataset is too large
        # --------------------------------------
        # Pattern mining is computationally expensive; sampling helps manage memory
        if num_transactions > max_transactions:
            sample_fraction = max_transactions / num_transactions
            print(f"⚠ Sampling {sample_fraction:.1%} of transactions to avoid memory issues...")
            transactions = transactions.sample(False, sample_fraction, seed=42)
            num_transactions = transactions.count()
            print(f"✓ Using {num_transactions:,} sampled transactions")
        
        
        
        # STEP 3: Run FP-Growth algorithm
        # -------------------------------
        print(f"\nRunning FP-Growth algorithm...")
        print(f"  Min Support: {min_support} ({int(min_support * num_transactions)} transactions)")
        
        # Configure the FP-Growth model
        fpGrowth = FPGrowth(
            itemsCol="items",        # Column containing item lists
            minSupport=min_support,   # Minimum support threshold
            minConfidence=min_confidence  # Minimum confidence for rules
        )
        
        # Fit the model (this runs the actual mining)
        model = fpGrowth.fit(transactions)
        
        # STEP 4: Extract results
        # -----------------------
        print("Extracting frequent itemsets...")
        
        # Get all frequent itemsets
        frequent_itemsets = model.freqItemsets
        
        # Get association rules (A → B patterns with confidence and lift)
        association_rules = model.associationRules
        
        # Filter to only keep product PAIRS (itemsets of size 2)
        # We're interested in "bought X and Y together" patterns
        pair_patterns = frequent_itemsets.filter(size(col("items")) == 2) \
            .orderBy(col("freq").desc())  # Sort by frequency (support)
        
        # Cache transactions for later use
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
        """
        Find sample customers who purchased both products in a pattern.
        
        For each pattern (Product1, Product2), this method finds up to 'limit'
        customers who reviewed/purchased both products and returns their ratings.
        
        This information is useful for:
        - Validating that patterns represent real co-purchases
        - Showing how customers rated each product in the pair
        - Providing concrete examples in the GUI
        
        HOW IT WORKS:
        -------------
        1. Filter reviews for customers who bought Product1
        2. Filter reviews for customers who bought Product2
        3. JOIN these datasets on customer_id to find customers who bought BOTH
        4. Return customer IDs with their ratings for each product
        
        Parameters:
        -----------
        reviews_df : DataFrame
            The full reviews DataFrame with customer_id, product_id, rating
        product1 : str
            ASIN of the first product
        product2 : str
            ASIN of the second product
        limit : int
            Maximum number of sample customers to return
        
        Returns:
        --------
        list of dict
            Each dict contains: customer_id, rating1 (for product1), rating2 (for product2)
        """
        # Find customers who reviewed product1 with their ratings
        customers_with_p1 = reviews_df.filter(col("product_id") == product1) \
            .select("customer_id", col("rating").alias("rating1"))
        
        # Find customers who reviewed product2 with their ratings
        customers_with_p2 = reviews_df.filter(col("product_id") == product2) \
            .select("customer_id", col("rating").alias("rating2"))
        
        # JOIN: Find customers present in BOTH datasets (bought both products)
        # This is like finding the intersection of two sets
        both = customers_with_p1.join(customers_with_p2, "customer_id") \
            .limit(limit)
        
        # Convert to Python list of dictionaries
        samples = []
        for row in both.collect():
            samples.append({
                'customer_id': row['customer_id'],
                'rating1': float(row['rating1']),
                'rating2': float(row['rating2'])
            })
        
        return samples
    
    def enrich_patterns_with_product_info(self, patterns, reviews_df):
        """
        Enhance pattern data with product titles and sample customer ratings.
        
        The raw FP-Growth output only contains product ASINs and support counts.
        This method adds human-readable information:
        - Product titles (so users can see what products they are)
        - Average ratings (to assess product quality)
        - Sample customers (to show real examples of co-purchases)
        
        This enriched data is what gets displayed in the GUI.
        
        Parameters:
        -----------
        patterns : dict
            Output from mine_patterns_fpgrowth() containing itemsets DataFrame
        reviews_df : DataFrame
            The reviews DataFrame for finding sample customers
        
        Returns:
        --------
        list of dict
            Enriched pattern data with structure:
            - rank: Pattern ranking (1 = highest support)
            - products: {asin1, asin2, title1, title2, avgRating1, avgRating2}
            - support: Number of customers who bought both
            - confidence: Support / total transactions
            - sample_customers: List of example customers
        """
        # Count total patterns to process
        total_patterns = patterns['itemsets'].count()
        print(f"\nFound {total_patterns:,} total frequent patterns")
        print(f"Enriching all {total_patterns:,} patterns with product details and sample customers...")
        
        enriched = []
        
        # Process each pattern (product pair)
        for i, itemset_row in enumerate(patterns['itemsets'].collect(), 1):
            # Extract products from the itemset (sorted for consistency)
            products = sorted(itemset_row['items'])
            support = itemset_row['freq']  # 'freq' is FP-Growth's support count
            confidence = support / patterns['num_transactions']
            
            # Show progress during long processing
            if i == 1 or i % 10 == 0 or i == total_patterns:
                print(f"  [{i:,}/{total_patterns:,}] Processing pattern: {products[0][:15]}... & {products[1][:15]}...")
            
            # Query Neo4j for product details (title, rating)
            # This runs a separate query for each pattern's products
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
            
            # Build lookup dictionary: ASIN -> {title, avgRating}
            product_info = {}
            for row in product_df.collect():
                product_info[row['asin']] = {
                    'title': row['title'],
                    'avgRating': float(row['avgRating']) if row['avgRating'] else None
                }
            
            # Get sample customers who bought both products
            sample_customers = self.get_sample_customers(reviews_df, products[0], products[1], limit=10)
            
            # Add enriched pattern to results
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
        """
        Save enriched pattern data to a JSON file for the GUI to load.
        
        The JSON structure is optimized for fast loading and display:
        - metadata: Timestamp and algorithm info for provenance
        - patterns: Array of all enriched pattern objects
        
        Parameters:
        -----------
        enriched_patterns : list
            List of enriched pattern dictionaries
        output_file : str
            Path to the output JSON file (default: gui_patterns.json)
        """
        print(f"Saving results to {output_file}...")
        
        # Structure the output with metadata for documentation
        output = {
            'metadata': {
                'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_patterns': len(enriched_patterns),
                'algorithm': 'FP-Growth (Spark MLlib)'
            },
            'patterns': enriched_patterns
        }
        
        # Write to JSON with indentation for readability
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Results saved to {output_file}\n")
    
    def display_sample_patterns(self, enriched_patterns, top_n=10):
        """
        Print a sample of top patterns to the console for verification.
        
        Useful for quick validation that pattern mining worked correctly.
        
        Parameters:
        -----------
        enriched_patterns : list
            List of enriched pattern dictionaries
        top_n : int
            Number of top patterns to display (default: 10)
        """
        print("="*70)
        print(f"TOP {top_n} CO-PURCHASE PATTERNS")
        print("="*70)
        
        for pattern in enriched_patterns[:top_n]:
            print(f"\n{pattern['rank']}. Support: {pattern['support']:,} | "
                  f"Confidence: {pattern['confidence']:.4f}")
            
            # Format ratings, handling None values
            rating1 = pattern['products']['avgRating1']
            rating1_str = f"{rating1:.2f}" if rating1 is not None else "N/A"
            rating2 = pattern['products']['avgRating2']
            rating2_str = f"{rating2:.2f}" if rating2 is not None else "N/A"
            
            # Display product details (truncated to 60 chars)
            print(f"   Product 1: {pattern['products']['title1'][:60]} "
                  f"(Avg Rating: {rating1_str})")
            print(f"   Product 2: {pattern['products']['title2'][:60]} "
                  f"(Avg Rating: {rating2_str})")
            print(f"   Sample Customers: {len(pattern['sample_customers'])}")

    def close(self):
        """
        Shut down the Spark session and release resources.
        
        Always call this when finished to avoid resource leaks.
        Spark maintains connections to cluster managers and temporary
        files that need to be properly cleaned up.
        """
        if self.spark:
            self.spark.stop()


def main():
    """
    Main execution function - orchestrates the complete pattern mining workflow.
    
    WORKFLOW:
    1. Initialize Spark + Neo4j connection
    2. Load review data from Neo4j
    3. Run FP-Growth pattern mining
    4. Enrich patterns with product info and samples
    5. Display sample results
    6. Save to JSON file
    
    This function is designed to be run once to pre-generate all patterns,
    which are then loaded quickly by the GUI application.
    """
    print("="*70)
    print("PREGENERATE PATTERN MINING RESULTS")
    print("="*70)
    print()
    
    # Initialize the pattern mining system
    miner = PatternPregeneration(
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="Password"
    )
    
    try:
        # STEP 1: Load all review data from Neo4j
        reviews_df = miner.load_reviews_from_neo4j()
        
        # STEP 2: Run FP-Growth pattern mining
        # Parameters tuned for comprehensive pattern discovery:
        # - min_support=0.0005 (0.05%): Very low threshold to find more patterns
        # - max_transactions=2M: Process all available data
        patterns = miner.mine_patterns_fpgrowth(
            reviews_df,
            min_support=0.0005,  # 0.05% - lower threshold for more patterns
            min_confidence=0.1,
            max_items_per_customer=20,  # Reduced from 30 to optimize memory
            max_transactions=2000000  # Allow up to 2M transactions (all data)
        )
        
        # STEP 3: Enrich patterns with product details and sample customers
        enriched_patterns = miner.enrich_patterns_with_product_info(
            patterns, 
            reviews_df
        )
        
        # STEP 4: Display sample patterns for verification
        miner.display_sample_patterns(enriched_patterns, top_n=10)
        
        # STEP 5: Save all results to JSON for GUI
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
        # Always clean up Spark resources
        miner.close()


# Entry point: Run main() only if script is executed directly
if __name__ == "__main__":
    main()