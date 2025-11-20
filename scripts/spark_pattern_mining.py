from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.fpm import FPGrowth
from neo4j import GraphDatabase
import time
import os


class SparkPatternMining:
    
    def __init__(self, neo4j_uri=None, neo4j_user="neo4j", neo4j_password="Password"):
        self.spark = SparkSession.builder \
            .appName("Amazon Co-Purchase Pattern Mining") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Use environment variable or default to localhost
        if neo4j_uri is None:
            neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    def split_train_test_spark(self, train_ratio=0.7):
        start_time = time.time()
        
        print("Loading review relationships from Neo4j...")
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                RETURN elementId(r) as rel_id
            """)
            rel_data = []
            count = 0
            for record in result:
                rel_data.append((record['rel_id'],))
                count += 1
                if count % 500000 == 0:
                    print(f"Loaded {count:,} review relationships...")
        
        print(f"Total review relationships loaded: {count:,}")
        
        schema = StructType([StructField("rel_id", StringType(), True)])
        reviews_df = self.spark.createDataFrame(rel_data, schema)
        
        # Random split using Spark
        train_df, test_df = reviews_df.randomSplit([train_ratio, 1-train_ratio], seed=42)
        
        train_count = train_df.count()
        test_count = test_df.count()
        
        # Mark relationships in Neo4j
        with self.driver.session(database="amazon-analysis") as session:
            # Mark train relationships
            train_ids = [row['rel_id'] for row in train_df.collect()]
            for i in range(0, len(train_ids), 10000):
                batch = train_ids[i:i+10000]
                session.run("""
                    UNWIND $ids as rel_id
                    MATCH ()-[r:REVIEWED]->()
                    WHERE elementId(r) = rel_id
                    SET r.dataset = 'train'
                """, ids=batch)
            
            # Mark test relationships
            test_ids = [row['rel_id'] for row in test_df.collect()]
            for i in range(0, len(test_ids), 10000):
                batch = test_ids[i:i+10000]
                session.run("""
                    UNWIND $ids as rel_id
                    MATCH ()-[r:REVIEWED]->()
                    WHERE elementId(r) = rel_id
                    SET r.dataset = 'test'
                """, ids=batch)
        
        elapsed = time.time() - start_time
        print(f"Split completed in {elapsed:.2f}s: {train_count} train, {test_count} test")
        
        return train_count, test_count
    
    def mine_patterns_with_fpgrowth(self, min_support=0.01, min_confidence=0.1, 
                                    max_customers=1000, dataset='train'):
        """
        Mine frequent co-purchasing patterns using Spark MLlib FP-Growth.
        
        Args:
            min_support: Minimum support as fraction (0-1)
            min_confidence: Minimum confidence for association rules
            max_customers: Maximum customers to analyze
            dataset: 'train' or 'test'
        
        Returns:
            Dictionary with patterns, rules, and metadata
        """
        start_time = time.time()
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run(f"""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.dataset = '{dataset}'
                WITH c, collect(DISTINCT p.asin) as products
                WHERE size(products) >= 2
                RETURN c.id as customer_id, products as items
                LIMIT {max_customers}
            """)
            trans_data = [(record['customer_id'], record['items']) for record in result]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("items", ArrayType(StringType()), True)
        ])
        
        transactions_df = self.spark.createDataFrame(trans_data, schema)
        
        fpGrowth = FPGrowth(
            itemsCol="items",
            minSupport=min_support,
            minConfidence=min_confidence
        )
        
        model = fpGrowth.fit(transactions_df)
        num_transactions = transactions_df.count()
        
        frequent_itemsets = model.freqItemsets
        association_rules = model.associationRules
        
        pair_patterns = frequent_itemsets.filter(size(col("items")) == 2) \
            .orderBy(col("freq").desc()) \
            .limit(20)
        
        patterns = []
        for row in pair_patterns.collect():
            asins = row['items']
            support = row['freq']
            
            with self.driver.session(database="amazon-analysis") as session:
                result = session.run("""
                    MATCH (p1:Product {asin: $asin1})
                    MATCH (p2:Product {asin: $asin2})
                    RETURN p1.title as title1, p2.title as title2
                """, asin1=asins[0], asin2=asins[1])
                record = result.single()
                if record:
                    patterns.append({
                        'products': tuple(asins),
                        'titles': (record['title1'], record['title2']),
                        'support': support,
                        'confidence': support / num_transactions
                    })
        
        rules = []
        for row in association_rules.filter(
            (size(col("antecedent")) == 1) & (size(col("consequent")) == 1)
        ).orderBy(col("confidence").desc()).limit(20).collect():
            rules.append({
                'antecedent': row['antecedent'][0],
                'consequent': row['consequent'][0],
                'confidence': row['confidence'],
                'lift': row['lift']
            })
        
        elapsed = time.time() - start_time
        
        return {
            'patterns': patterns,
            'rules': rules,
            'num_transactions': num_transactions,
            'elapsed_time': elapsed
        }
    
    def validate_patterns(self, train_patterns, max_customers=1000):
        test_results = self.mine_patterns_with_fpgrowth(
            min_support=0.01,
            min_confidence=0.1,
            max_customers=max_customers,
            dataset='test'
        )
        
        validated = []
        train_pattern_dict = {p['products']: p for p in train_patterns['patterns']}
        test_pattern_dict = {p['products']: p for p in test_results['patterns']}
        
        for pattern_key, train_pattern in train_pattern_dict.items():
            test_pattern = test_pattern_dict.get(pattern_key)
            validated.append({
                'products': pattern_key,
                'titles': train_pattern['titles'],
                'train_support': train_pattern['support'],
                'test_support': test_pattern['support'] if test_pattern else 0,
                'confidence': train_pattern['confidence']
            })
        
        validated.sort(key=lambda x: x['train_support'], reverse=True)
        
        return validated[:10]
    
    def find_customers_for_pattern(self, product_pair, dataset='train'):
        """Find customers who purchased both products in a pattern"""
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r1:REVIEWED]->(p1:Product {asin: $asin1})
                MATCH (c)-[r2:REVIEWED]->(p2:Product {asin: $asin2})
                WHERE r1.dataset = $dataset AND r2.dataset = $dataset
                RETURN c.id as customer_id, 
                       r1.rating as rating1, r2.rating as rating2
                LIMIT 100
            """, asin1=product_pair[0], asin2=product_pair[1], dataset=dataset)
            
            customers = []
            for record in result:
                customers.append({
                    'customer_id': record['customer_id'],
                    'rating1': record['rating1'],
                    'rating2': record['rating2']
                })
        
        return customers
    
    def close(self):
        """Stop Spark session and close Neo4j driver"""
        if self.driver:
            self.driver.close()
        if self.spark:
            self.spark.stop()


def main():
    miner = SparkPatternMining()
    
    try:
        train_count, test_count = miner.split_train_test_spark(train_ratio=0.7)
        
        train_patterns = miner.mine_patterns_with_fpgrowth(
            min_support=0.02,
            min_confidence=0.1,
            max_customers=500,
            dataset='train'
        )
        
        print("\n=== Top Co-Purchasing Patterns (Training Set) ===\n")
        for i, pattern in enumerate(train_patterns['patterns'][:5], 1):
            print(f"{i}. Support: {pattern['support']:,} | Confidence: {pattern['confidence']:.3f}")
            print(f"   {pattern['titles'][0][:60]}")
            print(f"   {pattern['titles'][1][:60]}\n")
        
        validated = miner.validate_patterns(train_patterns, max_customers=500)
        
        print("=== Pattern Validation (Test Set) ===\n")
        for i, pattern in enumerate(validated[:5], 1):
            print(f"{i}. Train: {pattern['train_support']:,} | Test: {pattern['test_support']:,}")
            print(f"   {pattern['titles'][0][:50]}")
            print(f"   {pattern['titles'][1][:50]}\n")
        
        print(f"Mining time: {train_patterns['elapsed_time']:.2f}s")
        print(f"Transactions: {train_patterns['num_transactions']:,}")
        print(f"Patterns: {len(train_patterns['patterns'])}")
        
    finally:
        miner.close()


if __name__ == "__main__":
    main()
