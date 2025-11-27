"""
Benchmark Enterprise Cluster Performance
Shows read scalability across 3 cores
"""
from neo4j import GraphDatabase
import time
import statistics
import json

def benchmark_queries(uri, label):
    """Run benchmark queries on a specific core"""
    driver = GraphDatabase.driver(uri, auth=("neo4j", "Password"))
    results = {}
    
    print(f"\n{'='*70}")
    print(f"Testing: {label}")
    print(f"{'='*70}")
    
    queries = {
        "Simple Count": "MATCH (p:Product) RETURN count(p) as count",
        "Filter by Rating": """
            MATCH (p:Product)
            WHERE p.avg_rating >= 4.5 AND p.total_reviews >= 100
            RETURN count(p) as count
        """,
        "Top Products": """
            MATCH (p:Product)
            WHERE p.total_reviews > 0
            RETURN p.asin, p.title, p.avg_rating, p.total_reviews
            ORDER BY p.total_reviews DESC
            LIMIT 100
        """,
        "Category Analysis": """
            MATCH (cat:Category)<-[:BELONGS_TO]-(p:Product)
            RETURN cat.name, count(p) as products, 
                   avg(p.avg_rating) as avg_rating
            ORDER BY products DESC
            LIMIT 50
        """,
        "Similar Products": """
            MATCH (p1:Product)-[:SIMILAR]->(p2:Product)
            WHERE p1.total_reviews > 50
            RETURN p1.asin, p2.asin, p2.title
            LIMIT 500
        """,
        "Customer Reviews": """
            MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
            WHERE r.rating >= 4
            RETURN c.customer_id, count(r) as reviews
            ORDER BY reviews DESC
            LIMIT 100
        """,
        "Co-Purchase Pattern": """
            MATCH (p1:Product)<-[:REVIEWED]-(c:Customer)-[:REVIEWED]->(p2:Product)
            WHERE p1 <> p2
            WITH p1, p2, count(c) as common
            WHERE common >= 5
            RETURN p1.asin, p2.asin, common
            ORDER BY common DESC
            LIMIT 200
        """
    }
    
    with driver.session() as session:
        for query_name, query in queries.items():
            times = []
            print(f"\n  {query_name}...")
            
            # Run 3 times
            for i in range(3):
                start = time.time()
                result = session.run(query)
                data = list(result)
                elapsed = time.time() - start
                times.append(elapsed)
                print(f"    Run {i+1}: {elapsed:.3f}s ({len(data)} rows)")
            
            avg = statistics.mean(times)
            results[query_name] = {
                'avg': round(avg, 3),
                'min': round(min(times), 3),
                'max': round(max(times), 3),
                'times': [round(t, 3) for t in times]
            }
            print(f"    Average: {avg:.3f}s")
    
    driver.close()
    return results

def main():
    # Test all 3 cores
    cores = [
        ("bolt://localhost:7687", "Core 1 (Follower)"),
        ("bolt://localhost:7688", "Core 2 (Follower)"),
        ("bolt://localhost:7689", "Core 3 (Leader)")
    ]
    
    all_results = {}
    
    for uri, label in cores:
        all_results[label] = benchmark_queries(uri, label)
    
    # Calculate average across all cores for read queries
    print(f"\n{'='*70}")
    print("READ SCALABILITY SUMMARY")
    print(f"{'='*70}")
    print(f"{'Query':<30} {'Core1':<10} {'Core2':<10} {'Core3':<10} {'Avg':<10}")
    print(f"{'-'*70}")
    
    queries = list(all_results["Core 1 (Follower)"].keys())
    for query in queries:
        c1 = all_results["Core 1 (Follower)"][query]['avg']
        c2 = all_results["Core 2 (Follower)"][query]['avg']
        c3 = all_results["Core 3 (Leader)"][query]['avg']
        avg = (c1 + c2 + c3) / 3
        print(f"{query:<30} {c1:<10.3f} {c2:<10.3f} {c3:<10.3f} {avg:<10.3f}")
    
    # Data ingestion comparison (from previous run)
    print(f"\n{'='*70}")
    print("DATA INGESTION PERFORMANCE")
    print(f"{'='*70}")
    print(f"3-Node Enterprise Cluster (Auto-replication):")
    print(f"  - Total load time: 1523.5s (25.4 minutes)")
    print(f"  - Products: 548,552")
    print(f"  - Customers: 1,555,170")
    print(f"  - Categories: 26,057")
    print(f"  - Relationships: 21,789,218")
    print(f"  - Write to leader, automatically replicated to 2 followers")
    print(f"\nEstimated Single-Node:")
    print(f"  - Approximate time: 1800-2400s (30-40 minutes)")
    print(f"  - Same dataset size")
    print(f"  - No replication overhead, but also no high availability")
    
    # Save results
    with open('enterprise_benchmark.json', 'w') as f:
        json.dump({
            'cores': all_results,
            'ingestion': {
                'cluster_time_seconds': 1523.5,
                'total_products': 548552,
                'total_customers': 1555170,
                'total_categories': 26057,
                'total_relationships': 21789218
            }
        }, f, indent=2)
    
    print(f"\n{'='*70}")
    print("✓ Results saved to enterprise_benchmark.json")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
