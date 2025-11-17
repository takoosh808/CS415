from spark_query_algorithm import SparkQueryAlgorithm
from spark_pattern_mining import SparkPatternMining
import time


def main():
    print("\n=== Spark-Based Algorithms ===\n")
    
    overall_start = time.time()
    
    print("Algorithm 1: Complex Query (Spark DataFrames)\n")
    
    query_algo = SparkQueryAlgorithm()
    
    try:
        results1, time1 = query_algo.execute_query({
            'min_rating': 4.5,
            'min_reviews': 100
        }, k=10)
        print(f"Query 1: {len(results1)} results in {time1:.2f}s")
        
        results2, time2 = query_algo.execute_query({
            'group': 'Book',
            'min_rating': 4.0,
            'max_salesrank': 50000
        }, k=10)
        print(f"Query 2: {len(results2)} results in {time2:.2f}s")
        
        results3, time3 = query_algo.execute_query({
            'group': 'Music',
            'min_rating': 4.0,
            'min_reviews': 50
        }, k=10)
        print(f"Query 3: {len(results3)} results in {time3:.2f}s")
        
        results4, time4 = query_algo.find_products_by_active_customers(
            min_customer_reviews=5, k=10
        )
        print(f"Query 4: {len(results4)} results in {time4:.2f}s")
        
        avg_query_time = (time1 + time2 + time3 + time4) / 4
        print(f"\nAverage: {avg_query_time:.2f}s\n")
        
    finally:
        query_algo.close()
    
    print("Algorithm 2: Pattern Mining (Spark MLlib FP-Growth)\n")
    
    pattern_miner = SparkPatternMining()
    
    try:
        train_count, test_count = pattern_miner.split_train_test_spark(train_ratio=0.7)
        print(f"Split: {train_count:,} train, {test_count:,} test")
        
        train_patterns = pattern_miner.mine_patterns_with_fpgrowth(
            min_support=0.02,
            min_confidence=0.1,
            max_customers=500,
            dataset='train'
        )
        
        print(f"\nTop {min(5, len(train_patterns['patterns']))} Patterns (Training Set):\n")
        for i, pattern in enumerate(train_patterns['patterns'][:5], 1):
            print(f"{i}. Support: {pattern['support']:,} | Confidence: {pattern['confidence']:.3f}")
            print(f"   {pattern['titles'][0][:60]}")
            print(f"   {pattern['titles'][1][:60]}\n")
        
        validated = pattern_miner.validate_patterns(train_patterns, max_customers=500)
        
        print("Validation (Test Set):\n")
        for i, pattern in enumerate(validated[:3], 1):
            print(f"{i}. Train: {pattern['train_support']:,} | Test: {pattern['test_support']:,}")
        
        print(f"\nMining time: {train_patterns['elapsed_time']:.2f}s")
        
    finally:
        pattern_miner.close()
    
    overall_time = time.time() - overall_start
    print(f"\nTotal execution time: {overall_time:.2f}s\n")


if __name__ == "__main__":
    main()
