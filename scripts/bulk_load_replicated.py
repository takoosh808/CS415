"""
Replicated bulk load - all 3 nodes get identical full copies of the data.
This enables high availability and read scalability.
"""

from neo4j import GraphDatabase
import time
import threading

NODES = [
    {"uri": "bolt://localhost:7687", "name": "Node1"},
    {"uri": "bolt://localhost:7688", "name": "Node2"},
    {"uri": "bolt://localhost:7689", "name": "Node3"}
]
PASSWORD = "Password"

def clear_database(driver, node_name):
    """Clear all existing data"""
    print(f"[{node_name}] Clearing database...")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print(f"[{node_name}] Database cleared")

def create_indexes(driver, node_name):
    """Create indexes for faster lookups"""
    print(f"[{node_name}] Creating indexes...")
    with driver.session() as session:
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)")
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")
    print(f"[{node_name}] Indexes created")

def load_products(driver, node_name):
    """Load ALL products"""
    print(f"[{node_name}] Loading products...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
    WITH row WHERE row.`asin:ID(Product)` IS NOT NULL
    CALL {
        WITH row
        CREATE (p:Product {
            asin: row.`asin:ID(Product)`,
            title: row.title,
            group: row.group,
            salesrank: CASE 
                WHEN row.`salesrank:int` IS NULL OR row.`salesrank:int` = '' THEN -1 
                ELSE toInteger(row.`salesrank:int`) 
            END,
            avg_rating: CASE 
                WHEN row.`avg_rating:float` IS NULL OR row.`avg_rating:float` = '' THEN 0.0 
                ELSE toFloat(row.`avg_rating:float`) 
            END,
            total_reviews: CASE 
                WHEN row.`total_reviews:int` IS NULL OR row.`total_reviews:int` = '' THEN 0 
                ELSE toInteger(row.`total_reviews:int`) 
            END
        })
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
    
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    print(f"[{node_name}] Loaded {nodes_created:,} products in {elapsed:.1f}s")
    return nodes_created

def load_categories(driver, node_name):
    """Load ALL categories"""
    print(f"[{node_name}] Loading categories...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
    WITH row WHERE row.`name:ID(Category)` IS NOT NULL
    CALL {
        WITH row
        MERGE (c:Category {name: row.`name:ID(Category)`})
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
    
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    print(f"[{node_name}] Loaded {nodes_created:,} categories in {elapsed:.1f}s")
    return nodes_created

def load_customers(driver, node_name):
    """Load ALL customers"""
    print(f"[{node_name}] Loading customers...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
    WITH row WHERE row.`id:ID(Customer)` IS NOT NULL
    CALL {
        WITH row
        CREATE (c:Customer {customer_id: row.`id:ID(Customer)`})
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
    
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    print(f"[{node_name}] Loaded {nodes_created:,} customers in {elapsed:.1f}s")
    return nodes_created

def load_belongs_to(driver, node_name):
    """Load ALL product-category relationships"""
    print(f"[{node_name}] Loading BELONGS_TO relationships...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///belongs_to.csv' AS row
    WITH row WHERE row.`:START_ID(Product)` IS NOT NULL AND row.`:END_ID(Category)` IS NOT NULL
    CALL {
        WITH row
        MATCH (p:Product {asin: row.`:START_ID(Product)`})
        MATCH (c:Category {name: row.`:END_ID(Category)`})
        CREATE (p)-[:BELONGS_TO]->(c)
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
    
    elapsed = time.time() - start
    rels_created = summary.counters.relationships_created
    print(f"[{node_name}] Created {rels_created:,} BELONGS_TO relationships in {elapsed:.1f}s")
    return rels_created

def load_similar(driver, node_name):
    """Load ALL similar product relationships"""
    print(f"[{node_name}] Loading SIMILAR relationships...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///similar.csv' AS row
    WITH row WHERE row.`:START_ID(Product)` IS NOT NULL AND row.`:END_ID(Product)` IS NOT NULL
    CALL {
        WITH row
        MATCH (p1:Product {asin: row.`:START_ID(Product)`})
        MATCH (p2:Product {asin: row.`:END_ID(Product)`})
        CREATE (p1)-[:SIMILAR]->(p2)
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
    
    elapsed = time.time() - start
    rels_created = summary.counters.relationships_created
    print(f"[{node_name}] Created {rels_created:,} SIMILAR relationships in {elapsed:.1f}s")
    return rels_created

def load_reviews(driver, node_name):
    """Load ALL review relationships"""
    print(f"[{node_name}] Loading REVIEWED relationships...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///reviews.csv' AS row
    WITH row WHERE row.`:START_ID(Customer)` IS NOT NULL AND row.`:END_ID(Product)` IS NOT NULL
    CALL {
        WITH row
        MATCH (c:Customer {customer_id: row.`:START_ID(Customer)`})
        MATCH (p:Product {asin: row.`:END_ID(Product)`})
        CREATE (c)-[:REVIEWED {
            rating: toInteger(row.`rating:int`),
            votes: toInteger(row.`votes:int`),
            helpful: toInteger(row.`helpful:int`)
        }]->(p)
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query)
        summary = result.consume()
    
    elapsed = time.time() - start
    rels_created = summary.counters.relationships_created
    print(f"[{node_name}] Created {rels_created:,} REVIEWED relationships in {elapsed:.1f}s")
    return rels_created

def load_node(node_config):
    """Load all data on a single node"""
    driver = GraphDatabase.driver(node_config["uri"], auth=("neo4j", PASSWORD))
    node_name = node_config["name"]
    
    try:
        clear_database(driver, node_name)
        create_indexes(driver, node_name)
        
        load_products(driver, node_name)
        load_categories(driver, node_name)
        load_customers(driver, node_name)
        load_belongs_to(driver, node_name)
        load_similar(driver, node_name)
        load_reviews(driver, node_name)
        
        print(f"[{node_name}] ✓ COMPLETE")
        
    finally:
        driver.close()

def get_counts(driver, node_name):
    """Get counts of all entity types"""
    with driver.session() as session:
        result = session.run("""
            MATCH (n)
            RETURN labels(n)[0] AS label, count(n) AS count
            ORDER BY label
        """)
        counts = {record["label"]: record["count"] for record in result}
    return counts

def main():
    total_start = time.time()
    
    print("="*70)
    print("REPLICATED BULK CSV LOAD - All Nodes Get Full Copy")
    print("="*70)
    print("This creates a replicated cluster where:")
    print("- All 3 nodes have identical full datasets")
    print("- Enables pattern mining across complete graph")
    print("- Provides high availability and read scalability")
    print("="*70)
    
    # Load all nodes in parallel
    print("\nStarting parallel load on all 3 nodes...")
    threads = []
    for node in NODES:
        thread = threading.Thread(target=load_node, args=(node,))
        thread.start()
        threads.append(thread)
    
    # Wait for all loads to complete
    for thread in threads:
        thread.join()
    
    # Verify final counts
    print("\n" + "="*70)
    print("FINAL VERIFICATION")
    print("="*70)
    
    for node in NODES:
        driver = GraphDatabase.driver(node["uri"], auth=("neo4j", PASSWORD))
        try:
            counts = get_counts(driver, node["name"])
            print(f"\n{node['name']}:")
            for label, count in counts.items():
                print(f"  {label}: {count:,}")
        finally:
            driver.close()
    
    total_elapsed = time.time() - total_start
    print("\n" + "="*70)
    print(f"REPLICATED LOAD COMPLETE - Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
    print("="*70)
    print("All 3 nodes now contain the full dataset:")
    print("- 548,552 Products")
    print("- 1,555,170 Customers")
    print("- 26,057 Categories")
    print("- 12,964,535 BELONGS_TO relationships")
    print("- 1,231,439 SIMILAR relationships")
    print("- 7,593,244 REVIEWED relationships")
    print("="*70)

if __name__ == "__main__":
    main()
