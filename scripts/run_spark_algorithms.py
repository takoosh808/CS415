from spark_query_algorithm import SparkQueryAlgorithm
from spark_pattern_mining import SparkPatternMining
import time


def main():
    overall_start = time.time()
    query_algo = SparkQueryAlgorithm()
    try:
        results1, time1 = query_algo.execute_query({
            'min_rating': 4.5,
            'min_reviews': 100
        }, k=10)
        results2, time2 = query_algo.execute_query({
            'group': 'Book',
            'min_rating': 4.0,
            'max_salesrank': 50000
        }, k=10)
        results3, time3 = query_algo.execute_query({
            'group': 'Music',
            'min_rating': 4.0,
            'min_reviews': 50
        }, k=10)
        results4, time4 = query_algo.find_products_by_active_customers(
            min_customer_reviews=5, k=10
        )
        avg_query_time = (time1 + time2 + time3 + time4) / 4
    finally:
        query_algo.close()
    pattern_miner = SparkPatternMining()
    try:
        train_count, test_count = pattern_miner.split_train_test_spark(train_ratio=0.7)
        train_patterns = pattern_miner.mine_patterns_with_fpgrowth(
            min_support=0.02,
            min_confidence=0.1,
            max_customers=500,
            dataset='train'
        )
        validated = pattern_miner.validate_patterns(train_patterns, max_customers=500)
    finally:
        pattern_miner.close()
    overall_time = time.time() - overall_start


if __name__ == "__main__":
    main()
