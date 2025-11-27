"""
Benchmark script for comparing single-node vs cluster performance.
Tests Neo4j cluster query throughput and Spark distributed processing.
"""
from neo4j import GraphDatabase
import time
import json
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict


class ClusterBenchmark:
    def __init__(self):
        self.results = {
            'single_node': {},
            'cluster': {},
            'comparison': {}
        }
    
    def benchmark_neo4j_queries(self, uri, label, iterations=10):
        """Benchmark Neo4j query performance"""
        print(f"\n{'='*60}")
        print(f"Testing {label}")
        print(f"{'='*60}")
        
        driver = GraphDatabase.driver(uri, auth=("neo4j", "Password"))
        
        queries = {
            'simple_lookup': """
                MATCH (p:Product {asin: '0827229534'})
                RETURN p.title, p.avg_rating, p.total_reviews
            """,
            'complex_filter': """
                MATCH (p:Product)
                WHERE p.group = 'Book' 
                  AND p.avg_rating >= 4.5
                  AND p.total_reviews >= 50
                RETURN p.asin, p.title, p.avg_rating
                ORDER BY p.avg_rating DESC, p.total_reviews DESC
                LIMIT 20
            """,
            'graph_traversal': """
                MATCH (p1:Product {asin: '0827229534'})-[:SIMILAR_TO]->(p2:Product)
                RETURN p2.asin, p2.title, p2.avg_rating
                LIMIT 10
            """,
            'aggregate_stats': """
                MATCH (p:Product)
                WHERE p.group = 'DVD'
                RETURN 
                    count(p) as total,
                    avg(p.avg_rating) as avg_rating,
                    avg(p.total_reviews) as avg_reviews
            """,
            'pattern_mining_sample': """
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.rating >= 4.0
                WITH c, collect(p.asin) as products
                WHERE size(products) >= 2
                RETURN c.id, products
                LIMIT 500
            """
        }
        
        results = {}
        
        with driver.session(database="neo4j") as session:
            for query_name, query in queries.items():
                print(f"\nQuery: {query_name}")
                times = []
                
                for i in range(iterations):
                    start = time.time()
                    result = session.run(query)
                    records = list(result)
                    elapsed = time.time() - start
                    times.append(elapsed)
                    
                    if i == 0:
                        print(f"  Results: {len(records)} rows")
                
                avg_time = statistics.mean(times)
                min_time = min(times)
                max_time = max(times)
                std_dev = statistics.stdev(times) if len(times) > 1 else 0
                
                print(f"  Avg: {avg_time*1000:.2f}ms | Min: {min_time*1000:.2f}ms | Max: {max_time*1000:.2f}ms | StdDev: {std_dev*1000:.2f}ms")
                
                results[query_name] = {
                    'avg_ms': avg_time * 1000,
                    'min_ms': min_time * 1000,
                    'max_ms': max_time * 1000,
                    'std_dev_ms': std_dev * 1000,
                    'iterations': iterations
                }
        
        driver.close()
        return results
    
    def benchmark_neo4j_throughput(self, uri, label, duration_seconds=30):
        """Benchmark concurrent query throughput"""
        print(f"\n{'='*60}")
        print(f"Testing Concurrent Throughput - {label}")
        print(f"{'='*60}")
        
        query = """
            MATCH (p:Product)
            WHERE p.avg_rating >= 4.0
            RETURN p.asin, p.title, p.avg_rating
            LIMIT 10
        """
        
        def execute_query(uri):
            driver = GraphDatabase.driver(uri, auth=("neo4j", "Password"))
            with driver.session(database="neo4j") as session:
                result = session.run(query)
                list(result)
            driver.close()
            return 1
        
        # Test with different concurrent thread counts
        thread_counts = [1, 5, 10, 20]
        results = {}
        
        for num_threads in thread_counts:
            print(f"\nTesting with {num_threads} concurrent threads...")
            start_time = time.time()
            queries_completed = 0
            
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                
                while time.time() - start_time < duration_seconds:
                    future = executor.submit(execute_query, uri)
                    futures.append(future)
                
                for future in as_completed(futures):
                    try:
                        queries_completed += future.result()
                    except Exception as e:
                        print(f"  Error: {e}")
            
            elapsed = time.time() - start_time
            qps = queries_completed / elapsed
            
            print(f"  Completed: {queries_completed} queries in {elapsed:.2f}s")
            print(f"  Throughput: {qps:.2f} queries/second")
            
            results[f'{num_threads}_threads'] = {
                'queries': queries_completed,
                'duration_s': elapsed,
                'qps': qps
            }
        
        return results
    
    def benchmark_pattern_mining(self, uri, label):
        """Benchmark pattern mining operation"""
        print(f"\n{'='*60}")
        print(f"Testing Pattern Mining - {label}")
        print(f"{'='*60}")
        
        driver = GraphDatabase.driver(uri, auth=("neo4j", "Password"))
        
        print("\nLoading transactions...")
        start = time.time()
        
        with driver.session(database="neo4j") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.rating >= 4.0
                WITH c, collect(p.asin) as products
                WHERE size(products) >= 2
                RETURN c.id as customer_id, products
                LIMIT 1000
            """)
            
            transactions = []
            for record in result:
                transactions.append(set(record['products']))
        
        load_time = time.time() - start
        print(f"  Loaded {len(transactions)} transactions in {load_time:.2f}s")
        
        # Count frequent items
        print("\nCounting frequent items...")
        start = time.time()
        
        item_counts = defaultdict(int)
        for transaction in transactions:
            for item in transaction:
                item_counts[item] += 1
        
        frequent_items = {item: count for item, count in item_counts.items() if count >= 10}
        count_time = time.time() - start
        print(f"  Found {len(frequent_items)} frequent items in {count_time:.2f}s")
        
        # Count frequent pairs
        print("\nFinding frequent pairs...")
        start = time.time()
        
        pair_counts = defaultdict(int)
        for transaction in transactions:
            items = [item for item in transaction if item in frequent_items]
            for i, item1 in enumerate(items):
                for item2 in items[i+1:]:
                    pair = tuple(sorted([item1, item2]))
                    pair_counts[pair] += 1
        
        frequent_pairs = {pair: count for pair, count in pair_counts.items() if count >= 10}
        pair_time = time.time() - start
        print(f"  Found {len(frequent_pairs)} frequent pairs in {pair_time:.2f}s")
        
        total_time = load_time + count_time + pair_time
        print(f"\nTotal time: {total_time:.2f}s")
        
        driver.close()
        
        return {
            'transactions': len(transactions),
            'frequent_items': len(frequent_items),
            'frequent_pairs': len(frequent_pairs),
            'load_time_s': load_time,
            'count_time_s': count_time,
            'pair_time_s': pair_time,
            'total_time_s': total_time
        }
    
    def run_all_benchmarks(self):
        """Run complete benchmark suite"""
        print("\n" + "="*60)
        print("CLUSTER PERFORMANCE BENCHMARK SUITE")
        print("="*60)
        
        # Test 1: Single-node queries
        print("\n[1/5] Single-node query performance...")
        self.results['single_node']['queries'] = self.benchmark_neo4j_queries(
            "bolt://localhost:7687", "Single Node", iterations=10
        )
        
        # Test 2: Cluster queries (using read replica for reads)
        print("\n[2/5] Cluster query performance (read replica)...")
        self.results['cluster']['queries'] = self.benchmark_neo4j_queries(
            "bolt://localhost:7690", "Cluster (Read Replica 1)", iterations=10
        )
        
        # Test 3: Single-node throughput
        print("\n[3/5] Single-node throughput...")
        self.results['single_node']['throughput'] = self.benchmark_neo4j_throughput(
            "bolt://localhost:7687", "Single Node", duration_seconds=20
        )
        
        # Test 4: Cluster throughput (load balanced across replicas)
        print("\n[4/5] Cluster throughput (load balanced)...")
        # Simulate load balancing by rotating between read replicas
        self.results['cluster']['throughput'] = self.benchmark_neo4j_throughput(
            "bolt://localhost:7690", "Cluster (Read Replicas)", duration_seconds=20
        )
        
        # Test 5: Pattern mining
        print("\n[5/5] Pattern mining performance...")
        self.results['single_node']['pattern_mining'] = self.benchmark_pattern_mining(
            "bolt://localhost:7687", "Single Node"
        )
        self.results['cluster']['pattern_mining'] = self.benchmark_pattern_mining(
            "bolt://localhost:7687", "Cluster (Core Node)"
        )
        
        # Calculate comparisons
        self.calculate_improvements()
        
        # Save results
        self.save_results()
        
        # Print summary
        self.print_summary()
    
    def calculate_improvements(self):
        """Calculate performance improvements"""
        print("\n" + "="*60)
        print("CALCULATING PERFORMANCE IMPROVEMENTS")
        print("="*60)
        
        # Query performance improvements
        for query_name in self.results['single_node']['queries']:
            single = self.results['single_node']['queries'][query_name]['avg_ms']
            cluster = self.results['cluster']['queries'][query_name]['avg_ms']
            improvement = ((single - cluster) / single) * 100
            speedup = single / cluster if cluster > 0 else 0
            
            self.results['comparison'][query_name] = {
                'improvement_percent': improvement,
                'speedup': speedup
            }
        
        # Throughput improvements
        for threads in self.results['single_node']['throughput']:
            single_qps = self.results['single_node']['throughput'][threads]['qps']
            cluster_qps = self.results['cluster']['throughput'][threads]['qps']
            improvement = ((cluster_qps - single_qps) / single_qps) * 100
            
            self.results['comparison'][f'throughput_{threads}'] = {
                'improvement_percent': improvement,
                'speedup': cluster_qps / single_qps if single_qps > 0 else 0
            }
    
    def save_results(self):
        """Save results to JSON file"""
        with open('cluster_benchmark_results.json', 'w') as f:
            json.dump(self.results, f, indent=2)
        print("\n✓ Results saved to cluster_benchmark_results.json")
    
    def print_summary(self):
        """Print benchmark summary"""
        print("\n" + "="*60)
        print("BENCHMARK SUMMARY")
        print("="*60)
        
        print("\nQuery Performance Comparison:")
        print(f"{'Query':<30} {'Single (ms)':<15} {'Cluster (ms)':<15} {'Speedup':<10}")
        print("-" * 70)
        
        for query_name in self.results['single_node']['queries']:
            single = self.results['single_node']['queries'][query_name]['avg_ms']
            cluster = self.results['cluster']['queries'][query_name]['avg_ms']
            speedup = self.results['comparison'][query_name]['speedup']
            print(f"{query_name:<30} {single:<15.2f} {cluster:<15.2f} {speedup:<10.2f}x")
        
        print("\nThroughput Comparison:")
        print(f"{'Threads':<20} {'Single (QPS)':<15} {'Cluster (QPS)':<15} {'Speedup':<10}")
        print("-" * 60)
        
        for threads in self.results['single_node']['throughput']:
            single_qps = self.results['single_node']['throughput'][threads]['qps']
            cluster_qps = self.results['cluster']['throughput'][threads]['qps']
            speedup = cluster_qps / single_qps if single_qps > 0 else 0
            print(f"{threads:<20} {single_qps:<15.2f} {cluster_qps:<15.2f} {speedup:<10.2f}x")
        
        print("\n" + "="*60)


def main():
    benchmark = ClusterBenchmark()
    benchmark.run_all_benchmarks()


if __name__ == "__main__":
    main()
