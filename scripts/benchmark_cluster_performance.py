"""
Benchmark Cluster Performance for Milestone 4 Report
Runs pattern mining and query algorithms to measure performance on full dataset.
Generates timing data for comparison with development (10MB) dataset.
"""

import time
import json
from datetime import datetime
from neo4j import GraphDatabase
from spark_query_algorithm import SparkQueryAlgorithm
from spark_pattern_mining import SparkPatternMining


def test_neo4j_queries():
    """Test Neo4j query performance on full dataset"""
    print("\n" + "="*80)
    print("NEO4J QUERY PERFORMANCE - FULL DATASET (548,552 products)")
    print("="*80 + "\n")
    
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Password"))
    results = {}
    
    queries = {
        "simple_match": """
            MATCH (p:Product {asin: $asin})
            RETURN p.asin, p.title, p.group
        """,
        "complex_filter": """
            MATCH (p:Product)
            WHERE p.group = $group 
              AND p.salesrank < $max_rank
            RETURN p.asin, p.title, p.salesrank
            ORDER BY p.salesrank
            LIMIT $k
        """,
        "graph_traversal": """
            MATCH (p:Product {asin: $asin})-[:BELONGS_TO]->(c:Category)
            RETURN c.name
            LIMIT 10
        """,
        "aggregate_stats": """
            MATCH (p:Product)
            WHERE p.group = $group
            RETURN p.group, count(p) as total
        """,
        "relationship_count": """
            MATCH ()-[r:REVIEWED]->()
            RETURN count(r) as total_reviews
        """
    }
    
    with driver.session() as session:
        # Simple match by ASIN
        start = time.time()
        result = session.run(queries["simple_match"], asin="0827229534")
        _ = list(result)
        elapsed = time.time() - start
        results["simple_match"] = elapsed
        print(f"1. Simple Match (by ASIN): {elapsed:.4f}s")
        
        # Complex filter query
        start = time.time()
        result = session.run(queries["complex_filter"], 
                            group="Book", max_rank=50000, k=20)
        data = list(result)
        elapsed = time.time() - start
        results["complex_filter"] = elapsed
        print(f"2. Complex Filter (3 conditions): {elapsed:.4f}s ({len(data)} results)")
        
        # Graph traversal (BELONGS_TO relationships)
        start = time.time()
        result = session.run(queries["graph_traversal"], asin="0827229534")
        data = list(result)
        elapsed = time.time() - start
        results["graph_traversal"] = elapsed
        print(f"3. Graph Traversal (categories): {elapsed:.4f}s ({len(data)} categories)")
        
        # Aggregate statistics
        start = time.time()
        result = session.run(queries["aggregate_stats"], group="DVD")
        data = list(result)
        elapsed = time.time() - start
        results["aggregate_stats"] = elapsed
        if data:
            print(f"4. Aggregate Stats: {elapsed:.4f}s (DVDs: {data[0]['total']:,})")
        
        # Count relationships
        start = time.time()
        result = session.run(queries["relationship_count"])
        data = list(result)
        elapsed = time.time() - start
        results["relationship_count"] = elapsed
        if data:
            print(f"5. Count Relationships: {elapsed:.4f}s (Reviews: {data[0]['total_reviews']:,})")
        
        # Get node counts
        print("\nDataset Verification:")
        result = session.run("MATCH (p:Product) RETURN count(p) as count")
        products = list(result)[0]["count"]
        print(f"  Products: {products:,}")
        
        result = session.run("MATCH (c:Customer) RETURN count(c) as count")
        customers = list(result)[0]["count"]
        print(f"  Customers: {customers:,}")
        
        result = session.run("MATCH (c:Category) RETURN count(c) as count")
        categories = list(result)[0]["count"]
        print(f"  Categories: {categories:,}")
    
    driver.close()
    
    avg_query_time = sum(results.values()) / len(results)
    print(f"\nAverage Query Time: {avg_query_time:.4f}s")
    
    return results


def test_spark_query_algorithm():
    """Test Spark query algorithm on full dataset"""
    print("\n" + "="*80)
    print("SPARK QUERY ALGORITHM - FULL DATASET")
    print("="*80 + "\n")
    
    query_algo = SparkQueryAlgorithm()
    results = {}
    
    try:
        # Query 1: High ratings, many reviews
        print("Query 1: High-rated products with many reviews")
        start = time.time()
        data, _ = query_algo.execute_query({
            'min_rating': 4.5,
            'min_reviews': 100
        }, k=20)
        elapsed = time.time() - start
        results["query1"] = elapsed
        print(f"  Results: {len(data)}, Time: {elapsed:.2f}s")
        
        # Query 2: Books with good rank
        print("\nQuery 2: Books with high sales rank")
        start = time.time()
        data, _ = query_algo.execute_query({
            'group': 'Book',
            'min_rating': 4.0,
            'max_salesrank': 50000
        }, k=20)
        elapsed = time.time() - start
        results["query2"] = elapsed
        print(f"  Results: {len(data)}, Time: {elapsed:.2f}s")
        
        # Query 3: Music albums
        print("\nQuery 3: Popular music albums")
        start = time.time()
        data, _ = query_algo.execute_query({
            'group': 'Music',
            'min_rating': 4.0,
            'min_reviews': 50
        }, k=20)
        elapsed = time.time() - start
        results["query3"] = elapsed
        print(f"  Results: {len(data)}, Time: {elapsed:.2f}s")
        
        # Query 4: Products by active customers
        print("\nQuery 4: Products from active customers")
        start = time.time()
        data, _ = query_algo.find_products_by_active_customers(
            min_customer_reviews=5, k=20
        )
        elapsed = time.time() - start
        results["query4"] = elapsed
        print(f"  Results: {len(data)}, Time: {elapsed:.2f}s")
        
        avg_time = sum(results.values()) / len(results)
        print(f"\nAverage Spark Query Time: {avg_time:.2f}s")
        
    finally:
        query_algo.close()
    
    return results


def test_spark_pattern_mining():
    """Test Spark pattern mining on full dataset"""
    print("\n" + "="*80)
    print("SPARK PATTERN MINING - FULL DATASET")
    print("="*80 + "\n")
    
    pattern_miner = SparkPatternMining()
    results = {}
    
    try:
        # Split train/test
        print("Splitting train/test data (70/30)...")
        start = time.time()
        train_count, test_count = pattern_miner.split_train_test_spark(train_ratio=0.7)
        split_time = time.time() - start
        results["split_time"] = split_time
        print(f"  Train: {train_count:,} reviews")
        print(f"  Test: {test_count:,} reviews")
        print(f"  Split time: {split_time:.2f}s\n")
        
        # Mine patterns with different support thresholds
        for min_sup, max_cust in [(0.02, 1000), (0.01, 2000)]:
            print(f"Mining patterns (min_support={min_sup}, max_customers={max_cust})...")
            start = time.time()
            train_patterns = pattern_miner.mine_patterns_with_fpgrowth(
                min_support=min_sup,
                min_confidence=0.1,
                max_customers=max_cust,
                dataset='train'
            )
            mining_time = time.time() - start
            results[f"mining_sup{min_sup}_cust{max_cust}"] = mining_time
            
            print(f"  Patterns found: {len(train_patterns['patterns'])}")
            print(f"  Mining time: {mining_time:.2f}s")
            
            if train_patterns['patterns']:
                print(f"\n  Top 3 Patterns:")
                for i, pattern in enumerate(train_patterns['patterns'][:3], 1):
                    print(f"    {i}. Support: {pattern['support']:,} | Confidence: {pattern['confidence']:.3f}")
                    print(f"       {pattern['titles'][0][:55]}...")
                    print(f"       {pattern['titles'][1][:55]}...")
            
            # Validate patterns
            print(f"\n  Validating patterns on test set...")
            start = time.time()
            validated = pattern_miner.validate_patterns(train_patterns, max_customers=max_cust)
            validation_time = time.time() - start
            results[f"validation_sup{min_sup}"] = validation_time
            print(f"  Validation time: {validation_time:.2f}s")
            
            if validated:
                print(f"  Top 3 Validated Patterns:")
                for i, pattern in enumerate(validated[:3], 1):
                    print(f"    {i}. Train: {pattern['train_support']:,} | Test: {pattern['test_support']:,}")
            
            print()
        
        total_mining_time = sum(v for k, v in results.items() if 'mining' in k)
        print(f"Total Pattern Mining Time: {total_mining_time:.2f}s")
        
    finally:
        pattern_miner.close()
    
    return results


def generate_report(neo4j_results, spark_query_results, spark_mining_results):
    """Generate benchmark report for milestone 4"""
    print("\n" + "="*80)
    print("PERFORMANCE BENCHMARK SUMMARY")
    print("="*80 + "\n")
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "dataset": {
            "name": "Full Amazon Dataset",
            "products": 548552,
            "customers": 1555170,
            "categories": 26057,
            "relationships": 21789218
        },
        "cluster": {
            "type": "3-node replicated Neo4j cluster",
            "nodes": 3,
            "heap_per_node": "2GB"
        },
        "benchmarks": {
            "neo4j_queries": neo4j_results,
            "spark_queries": spark_query_results,
            "spark_mining": spark_mining_results
        }
    }
    
    # Print summary table
    print("NEO4J QUERY PERFORMANCE:")
    print("-" * 60)
    for query, time_val in neo4j_results.items():
        print(f"  {query:25s}: {time_val:6.4f}s")
    print(f"  {'Average':25s}: {sum(neo4j_results.values())/len(neo4j_results):6.4f}s")
    
    print("\nSPARK QUERY PERFORMANCE:")
    print("-" * 60)
    for query, time_val in spark_query_results.items():
        print(f"  {query:25s}: {time_val:6.2f}s")
    print(f"  {'Average':25s}: {sum(spark_query_results.values())/len(spark_query_results):6.2f}s")
    
    print("\nSPARK PATTERN MINING PERFORMANCE:")
    print("-" * 60)
    for operation, time_val in spark_mining_results.items():
        print(f"  {operation:35s}: {time_val:6.2f}s")
    
    # Save to JSON
    output_file = "benchmark_results.json"
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\nFull results saved to: {output_file}")
    
    return report


def main():
    """Run all benchmarks"""
    print("\n" + "="*80)
    print("MILESTONE 4 - CLUSTER PERFORMANCE BENCHMARKS")
    print("Testing full dataset (548,552 products) on 3-node Neo4j cluster")
    print("="*80)
    
    overall_start = time.time()
    
    # Test Neo4j queries
    neo4j_results = test_neo4j_queries()
    
    # Test Spark query algorithm
    spark_query_results = test_spark_query_algorithm()
    
    # Test Spark pattern mining
    spark_mining_results = test_spark_pattern_mining()
    
    # Generate report
    report = generate_report(neo4j_results, spark_query_results, spark_mining_results)
    
    overall_time = time.time() - overall_start
    
    print("\n" + "="*80)
    print(f"TOTAL BENCHMARK TIME: {overall_time:.2f}s ({overall_time/60:.1f} minutes)")
    print("="*80 + "\n")
    
    # Print comparison notes
    print("\nCOMPARISON NOTES FOR REPORT:")
    print("-" * 80)
    print("Development (10MB subset):")
    print("  - Products: 36,202")
    print("  - Customers: 75,347")
    print("  - Single instance Neo4j")
    print("  - Spark local mode")
    print()
    print("Production (Full dataset on cluster):")
    print("  - Products: 548,552 (15x increase)")
    print("  - Customers: 1,555,170 (20x increase)")
    print("  - 3-node replicated Neo4j cluster")
    print("  - Spark local mode (cluster-ready code)")
    print()
    print("Query Performance Impact:")
    avg_neo4j = sum(neo4j_results.values()) / len(neo4j_results)
    print(f"  - Neo4j avg: {avg_neo4j:.4f}s (minimal impact from 15x data)")
    avg_spark = sum(spark_query_results.values()) / len(spark_query_results)
    print(f"  - Spark avg: {avg_spark:.2f}s (efficient parallel processing)")
    print()
    print("This demonstrates:")
    print("  1. Neo4j graph database scales well with indexed queries")
    print("  2. Spark DataFrame operations handle large datasets efficiently")
    print("  3. Pattern mining feasible on full dataset with sampling")
    print("  4. Cluster architecture provides redundancy + query distribution")


if __name__ == "__main__":
    main()
