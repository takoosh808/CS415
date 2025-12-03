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
    
    def __init__(self, neo4j_uri="bolt://localhost:7687", neo4j_user="neo4j", neo4j_password="Password"):
        """Initialize Spark session with Neo4j connector"""
        
        
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
        
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        
        
    
    def load_reviews_from_neo4j(self):
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
                       r.rating as rating,
                       r.dataset as dataset
            """) \
            .load()
        reviews_df.persist(StorageLevel.DISK_ONLY)
        return reviews_df
    
    def split_train_test_spark(self, reviews_df, train_ratio=0.7):
        has_split = reviews_df.filter(col("dataset").isNotNull()).count() > 0
        if has_split:
            return reviews_df
        reviews_with_split = reviews_df.withColumn(
            "dataset",
            when(rand(seed=42) < train_ratio, lit("train")).otherwise(lit("test"))
        )
        return reviews_with_split
    
    def mine_patterns_fpgrowth(self, reviews_df, min_support=0.002, min_confidence=0.1, dataset='train', max_items_per_customer=30, max_transactions=50000):
        start_time = time.time()
        dataset_reviews = reviews_df.filter(col("dataset") == dataset)
        dataset_count = dataset_reviews.count()
        if dataset_count < 100:
            dataset_reviews = reviews_df
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        deduplicated = dataset_reviews.dropDuplicates(["customer_id", "product_id"])
        window_spec = Window.partitionBy("customer_id").orderBy(rand())
        limited_reviews = deduplicated.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") <= max_items_per_customer) \
            .drop("row_num")
        transactions = limited_reviews.groupBy("customer_id") \
            .agg(collect_list("product_id").alias("items")) \
            .filter((size("items") >= 2) & (size("items") <= max_items_per_customer))
        num_transactions = transactions.count()
        if num_transactions > max_transactions:
            sample_fraction = max_transactions / num_transactions
            transactions = transactions.sample(False, sample_fraction, seed=42)
            num_transactions = transactions.count()
        transactions = transactions.persist(StorageLevel.DISK_ONLY)
        transactions.count()
        fpGrowth = FPGrowth(
            itemsCol="items", 
            minSupport=min_support, 
            minConfidence=min_confidence
        )
        try:
            model = fpGrowth.fit(transactions)
        except Exception as e:
            transactions.unpersist()
            return None
        frequent_itemsets = model.freqItemsets \
            .filter(size("items") == 2) \
            .orderBy(col("freq").desc()) \
            .persist(StorageLevel.DISK_ONLY)
        num_patterns = frequent_itemsets.count()
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
        train_itemsets = train_patterns['itemsets'].limit(50).collect()
        test_transactions = test_reviews_df \
            .filter(col("dataset") == "test") \
            .groupBy("customer_id") \
            .agg(collect_list("product_id").alias("items"))
        validated_patterns = []
        for itemset_row in train_itemsets:
            products = sorted(itemset_row['items'])
            train_support = itemset_row['freq']
            filter_expr = array_contains(col("items"), products[0])
            for prod in products[1:]:
                filter_expr = filter_expr & array_contains(col("items"), prod)
            test_support = test_transactions.filter(filter_expr).count()
            if test_support >= min_support:
                validated_patterns.append({
                    'products': products,
                    'train_support': train_support,
                    'test_support': test_support,
                    'total_support': train_support + test_support
                })
        return validated_patterns
    
    def enrich_patterns_with_product_info(self, patterns):
        enriched = []
        for pattern in patterns[:25]:
            products = pattern['products']
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
        return enriched
    
    def save_results(self, results, output_file='spark_neo4j_results.json'):
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
    
    def close(self):
        self.spark.stop()


def main():
    """Main execution pipeline"""
    miner = SparkNeo4jPatternMining()
    reviews_df = miner.load_reviews_from_neo4j()
    reviews_df = miner.split_train_test_spark(reviews_df, train_ratio=0.7)
    train_results = miner.mine_patterns_fpgrowth(
        reviews_df, 
        min_support=0.002,
        min_confidence=0.1,
        dataset='train',
        max_items_per_customer=30,
        max_transactions=15000
    )
    if train_results is None:
        miner.spark.stop()
        return
    top_patterns = train_results['itemsets'].limit(10).collect()
    for i, row in enumerate(top_patterns, 1):
        products = sorted(row['items'])
        support = row['freq']
        print(f"{i}. Products: {products}")
        print(f"   Support: {support} customers")
        print()
    patterns_for_validation = []
    for row in train_results['itemsets'].limit(50).collect():
        patterns_for_validation.append({
            'products': sorted(row['items']),
            'train_support': row['freq'],
            'test_support': 0,
            'total_support': row['freq']
        })
    validated_patterns = miner.validate_patterns_cross_dataset(
        train_results,
        reviews_df,
        min_support=5
    )
    for i, pattern in enumerate(sorted(validated_patterns, key=lambda x: x['total_support'], reverse=True)[:10], 1):
        print(f"{i}. Products: {pattern['products']}")
        print(f"   Train Support: {pattern['train_support']}")
        print(f"   Test Support: {pattern['test_support']}")
        print(f"   Total Support: {pattern['total_support']}")
        print()
    enriched_patterns = miner.enrich_patterns_with_product_info(validated_patterns)
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
    miner.close()


if __name__ == "__main__":
    main()
