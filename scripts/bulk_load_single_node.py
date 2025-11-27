"""
Bulk load data into a single Neo4j node using LOAD CSV
Optimized for single-node performance testing
"""

from neo4j import GraphDatabase
import time

def clear_database(driver):
    """Clear all data from database"""
    print("\nClearing database...")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("Database cleared")

def create_indexes(driver):
    """Create indexes for faster loading"""
    print("\nCreating indexes...")
    with driver.session() as session:
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.id)")
    print("Indexes created")

def load_products(driver):
    """Load products"""
    print("\nLoading products...")
    start = time.time()
    
    with driver.session() as session:
        # Load products in batches
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
            WITH row WHERE row.`asin:ID(Product)` IS NOT NULL AND row.`asin:ID(Product)` <> ''
            CALL {
                WITH row
                CREATE (p:Product {
                    asin: row.`asin:ID(Product)`,
                    title: row.title,
                    group: row.`group`,
                    salesrank: toInteger(row.`salesrank:int`),
                    avg_rating: toFloat(row.`avg_rating:float`),
                    total_reviews: toInteger(row.`total_reviews:int`)
                })
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        result = session.run("MATCH (p:Product) RETURN count(p) as count")
        count = result.single()['count']
    
    elapsed = time.time() - start
    print(f"Loaded {count:,} products in {elapsed:.1f}s ({count/elapsed:.0f} products/sec)")
    return elapsed

def load_categories(driver):
    """Load categories"""
    print("\nLoading categories...")
    start = time.time()
    
    with driver.session() as session:
        # Categories CSV has header "name:ID(Category)"
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
            WITH row WHERE row.`name:ID(Category)` IS NOT NULL AND row.`name:ID(Category)` <> ''
            CALL {
                WITH row
                MERGE (c:Category {name: row.`name:ID(Category)`})
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        result = session.run("MATCH (c:Category) RETURN count(c) as count")
        count = result.single()['count']
    
    elapsed = time.time() - start
    print(f"Loaded {count:,} categories in {elapsed:.1f}s")
    return elapsed

def load_customers(driver):
    """Load customers"""
    print("\nLoading customers...")
    start = time.time()
    
    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
            WITH row WHERE row.`id:ID(Customer)` IS NOT NULL AND row.`id:ID(Customer)` <> ''
            CALL {
                WITH row
                CREATE (c:Customer {id: row.`id:ID(Customer)`})
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        result = session.run("MATCH (c:Customer) RETURN count(c) as count")
        count = result.single()['count']
    
    elapsed = time.time() - start
    print(f"Loaded {count:,} customers in {elapsed:.1f}s ({count/elapsed:.0f} customers/sec)")
    return elapsed

def load_belongs_to(driver):
    """Load BELONGS_TO relationships"""
    print("\nLoading BELONGS_TO relationships...")
    start = time.time()
    
    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///belongs_to.csv' AS row
            CALL {
                WITH row
                MATCH (p:Product {asin: row.`:START_ID(Product)`})
                MATCH (c:Category {name: row.`:END_ID(Category)`})
                CREATE (p)-[:BELONGS_TO]->(c)
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        result = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count")
        count = result.single()['count']
    
    elapsed = time.time() - start
    print(f"Created {count:,} BELONGS_TO relationships in {elapsed:.1f}s ({count/elapsed:.0f} rels/sec)")
    return elapsed

def load_similar(driver):
    """Load SIMILAR relationships"""
    print("\nLoading SIMILAR relationships...")
    start = time.time()
    
    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///similar.csv' AS row
            CALL {
                WITH row
                MATCH (p1:Product {asin: row.`:START_ID(Product)`})
                MATCH (p2:Product {asin: row.`:END_ID(Product)`})
                CREATE (p1)-[:SIMILAR]->(p2)
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        result = session.run("MATCH ()-[r:SIMILAR]->() RETURN count(r) as count")
        count = result.single()['count']
    
    elapsed = time.time() - start
    print(f"Created {count:,} SIMILAR relationships in {elapsed:.1f}s ({count/elapsed:.0f} rels/sec)")
    return elapsed

def load_reviews(driver):
    """Load REVIEWED relationships"""
    print("\nLoading REVIEWED relationships...")
    start = time.time()
    
    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///reviews.csv' AS row
            CALL {
                WITH row
                MATCH (c:Customer {id: row.`:START_ID(Customer)`})
                MATCH (p:Product {asin: row.`:END_ID(Product)`})
                CREATE (c)-[:REVIEWED {
                    rating: toInteger(row.`rating:int`),
                    date: row.date,
                    helpful: toInteger(row.`helpful:int`)
                }]->(p)
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
        count = result.single()['count']
    
    elapsed = time.time() - start
    print(f"Created {count:,} REVIEWED relationships in {elapsed:.1f}s ({count/elapsed:.0f} rels/sec)")
    return elapsed

def verify_data(driver):
    """Verify final data counts"""
    print("\n" + "="*60)
    print("FINAL VERIFICATION")
    print("="*60)
    
    with driver.session() as session:
        results = {}
        
        result = session.run("MATCH (p:Product) RETURN count(p) as count")
        results['Products'] = result.single()['count']
        
        result = session.run("MATCH (c:Category) RETURN count(c) as count")
        results['Categories'] = result.single()['count']
        
        result = session.run("MATCH (c:Customer) RETURN count(c) as count")
        results['Customers'] = result.single()['count']
        
        result = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count")
        results['BELONGS_TO'] = result.single()['count']
        
        result = session.run("MATCH ()-[r:SIMILAR]->() RETURN count(r) as count")
        results['SIMILAR'] = result.single()['count']
        
        result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
        results['REVIEWED'] = result.single()['count']
        
        for name, count in results.items():
            print(f"  {name}: {count:,}")
    
    return results

def main():
    print("="*60)
    print("SINGLE NODE BULK LOAD - Performance Benchmark")
    print("="*60)
    
    # Connect to single node
    driver = GraphDatabase.driver("bolt://localhost:7690", auth=("neo4j", "Password"))
    
    try:
        total_start = time.time()
        
        # Clear and prepare
        clear_database(driver)
        create_indexes(driver)
        
        # Load data and track times
        times = {}
        times['products'] = load_products(driver)
        times['categories'] = load_categories(driver)
        times['customers'] = load_customers(driver)
        times['belongs_to'] = load_belongs_to(driver)
        times['similar'] = load_similar(driver)
        times['reviews'] = load_reviews(driver)
        
        # Verify
        counts = verify_data(driver)
        
        total_time = time.time() - total_start
        
        print("\n" + "="*60)
        print(f"SINGLE NODE LOAD COMPLETE - Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
        print("="*60)
        
        # Calculate throughput
        total_nodes = counts['Products'] + counts['Categories'] + counts['Customers']
        total_rels = counts['BELONGS_TO'] + counts['SIMILAR'] + counts['REVIEWED']
        
        print(f"\nThroughput:")
        print(f"  Nodes: {total_nodes/total_time:.0f} nodes/sec")
        print(f"  Relationships: {total_rels/total_time:.0f} rels/sec")
        print(f"  Total: {(total_nodes + total_rels)/total_time:.0f} elements/sec")
        
        return {
            'times': times,
            'counts': counts,
            'total_time': total_time
        }
        
    finally:
        driver.close()

if __name__ == "__main__":
    main()
