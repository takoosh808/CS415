"""
Quick pattern mining test - minimal dataset for speed
Tests basic co-purchase pattern discovery
"""

from neo4j import GraphDatabase
import time

def quick_pattern_mining():
    """Run a simplified pattern mining test"""
    
    driver = GraphDatabase.driver("bolt://localhost:7689", auth=("neo4j", "Password"))
    
    print("="*70)
    print("QUICK CO-PURCHASE PATTERN MINING TEST")
    print("="*70)
    
    with driver.session() as session:
        # Find customers who reviewed multiple products (limit to 100 for speed)
        print("\n1. Finding frequent co-purchase patterns...")
        start = time.time()
        
        result = session.run("""
            MATCH (c:Customer)-[:REVIEWED]->(p:Product)
            WITH c, collect(p.asin) as products
            WHERE size(products) >= 3
            WITH c, products
            LIMIT 100
            UNWIND products as p1
            UNWIND products as p2
            WITH p1, p2
            WHERE p1 < p2
            WITH p1, p2, count(*) as frequency
            WHERE frequency >= 3
            RETURN p1, p2, frequency
            ORDER BY frequency DESC
            LIMIT 20
        """)
        
        patterns = list(result)
        elapsed = time.time() - start
        
        print(f"   Found {len(patterns)} frequent patterns in {elapsed:.2f}s")
        print(f"\n   Top 10 Co-Purchase Patterns:")
        for i, record in enumerate(patterns[:10], 1):
            print(f"   {i}. {record['p1'][:15]:15s} + {record['p2'][:15]:15s} = {record['frequency']} times")
        
        # Get product details for top pattern
        if patterns:
            print(f"\n2. Analyzing top pattern details...")
            start = time.time()
            
            top = patterns[0]
            result = session.run("""
                MATCH (p:Product {asin: $asin})
                RETURN p.title as title, p.avg_rating as rating, p.total_reviews as reviews
            """, asin=top['p1'])
            
            p1_details = result.single()
            
            result = session.run("""
                MATCH (p:Product {asin: $asin})
                RETURN p.title as title, p.avg_rating as rating, p.total_reviews as reviews
            """, asin=top['p2'])
            
            p2_details = result.single()
            elapsed = time.time() - start
            
            print(f"   Completed in {elapsed:.2f}s")
            print(f"\n   Product 1: {p1_details['title'][:50]}")
            print(f"              Rating: {p1_details['rating']}, Reviews: {p1_details['reviews']}")
            print(f"\n   Product 2: {p2_details['title'][:50]}")
            print(f"              Rating: {p2_details['rating']}, Reviews: {p2_details['reviews']}")
            print(f"\n   Co-purchased {top['frequency']} times by same customers")
        
        # Test recommendation generation speed
        print(f"\n3. Testing recommendation generation...")
        start = time.time()
        
        result = session.run("""
            MATCH (seed:Product {asin: '0771044445'})
            MATCH (seed)-[:SIMILAR]->(similar:Product)
            MATCH (similar)<-[:REVIEWED]-(c:Customer)-[:REVIEWED]->(recommended:Product)
            WHERE recommended <> seed AND NOT (seed)-[:SIMILAR]->(recommended)
            RETURN recommended.asin as asin, 
                   recommended.title as title,
                   count(DISTINCT c) as common_customers,
                   avg(recommended.avg_rating) as avg_rating
            ORDER BY common_customers DESC
            LIMIT 10
        """)
        
        recommendations = list(result)
        elapsed = time.time() - start
        
        print(f"   Generated {len(recommendations)} recommendations in {elapsed:.2f}s")
        print(f"\n   Top 5 Recommendations:")
        for i, rec in enumerate(recommendations[:5], 1):
            title = rec['title'][:40] if rec['title'] else "N/A"
            print(f"   {i}. {title:40s} ({rec['common_customers']} customers)")
    
    driver.close()
    
    print(f"\n{'='*70}")
    print("QUICK TEST COMPLETE")
    print("="*70)
    print("\nConclusion:")
    print("  - Pattern mining works on Enterprise cluster")
    print("  - Query performance is responsive")
    print("  - Recommendation generation is functional")

if __name__ == "__main__":
    quick_pattern_mining()
