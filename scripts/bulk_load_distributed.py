"""
Distributed bulk load - partition data across 3 nodes using hash-based distribution.
Each node loads 1/3 of the data based on row number modulo 3.
"""

from neo4j import GraphDatabase
import time

NODES = [
    {"uri": "bolt://localhost:7687", "name": "Node1", "index": 0},
    {"uri": "bolt://localhost:7688", "name": "Node2", "index": 1},
    {"uri": "bolt://localhost:7689", "name": "Node3", "index": 2}
]
PASSWORD = "Password"

def clear_database(driver, node_name):
    """Clear all existing data"""
    print(f"\n[{node_name}] Clearing database...")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print(f"[{node_name}] Database cleared")

def create_indexes(driver, node_name):
    """Create indexes for faster lookups"""
    print(f"\n[{node_name}] Creating indexes...")
    with driver.session() as session:
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)")
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")
    print(f"[{node_name}] Indexes created")

def load_products(driver, node_name, node_index):
    """Load products - distributed by line number mod 3"""
    print(f"\n[{node_name}] Loading products (every 3rd row starting at {node_index})...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
    WITH row, toInteger(linenumber()) AS linenum
    WHERE linenum % 3 = $node_index AND row.`asin:ID(Product)` IS NOT NULL
    CALL {
        WITH row
        CREATE (p:Product {
            asin: row.`asin:ID(Product)`,
            title: row.title,
            product_group: row.group,
            salesrank: CASE 
                WHEN row.`salesrank:int` IS NULL OR row.`salesrank:int` = '' THEN -1 
                ELSE toInteger(row.`salesrank:int`) 
            END
        })
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query, node_index=node_index)
        summary = result.consume()
    
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    print(f"[{node_name}] Loaded {nodes_created:,} products in {elapsed:.1f}s")
    return nodes_created

def load_categories(driver, node_name, node_index):
    """Load categories - distributed by line number mod 3"""
    print(f"\n[{node_name}] Loading categories (every 3rd row starting at {node_index})...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
    WITH row, toInteger(linenumber()) AS linenum
    WHERE linenum % 3 = $node_index AND row.`name:ID(Category)` IS NOT NULL
    CALL {
        WITH row
        MERGE (c:Category {name: row.`name:ID(Category)`})
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query, node_index=node_index)
        summary = result.consume()
    
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    print(f"[{node_name}] Loaded {nodes_created:,} categories in {elapsed:.1f}s")
    return nodes_created

def load_customers(driver, node_name, node_index):
    """Load customers - distributed by line number mod 3"""
    print(f"\n[{node_name}] Loading customers (every 3rd row starting at {node_index})...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
    WITH row, toInteger(linenumber()) AS linenum
    WHERE linenum % 3 = $node_index AND row.`id:ID(Customer)` IS NOT NULL
    CALL {
        WITH row
        CREATE (c:Customer {customer_id: row.`id:ID(Customer)`})
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query, node_index=node_index)
        summary = result.consume()
    
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    print(f"[{node_name}] Loaded {nodes_created:,} customers in {elapsed:.1f}s")
    return nodes_created

def load_belongs_to(driver, node_name, node_index):
    """Load product-category relationships - distributed by line number mod 3"""
    print(f"\n[{node_name}] Loading BELONGS_TO relationships (every 3rd row starting at {node_index})...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///belongs_to.csv' AS row
    WITH row, toInteger(linenumber()) AS linenum
    WHERE linenum % 3 = $node_index AND row.`:START_ID(Product)` IS NOT NULL AND row.`:END_ID(Category)` IS NOT NULL
    CALL {
        WITH row
        MATCH (p:Product {asin: row.`:START_ID(Product)`})
        MATCH (c:Category {name: row.`:END_ID(Category)`})
        CREATE (p)-[:BELONGS_TO]->(c)
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query, node_index=node_index)
        summary = result.consume()
    
    elapsed = time.time() - start
    rels_created = summary.counters.relationships_created
    print(f"[{node_name}] Created {rels_created:,} BELONGS_TO relationships in {elapsed:.1f}s")
    return rels_created

def load_similar(driver, node_name, node_index):
    """Load similar product relationships - distributed by line number mod 3"""
    print(f"\n[{node_name}] Loading SIMILAR relationships (every 3rd row starting at {node_index})...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///similar.csv' AS row
    WITH row, toInteger(linenumber()) AS linenum
    WHERE linenum % 3 = $node_index AND row.`:START_ID(Product)` IS NOT NULL AND row.`:END_ID(Product)` IS NOT NULL
    CALL {
        WITH row
        MATCH (p1:Product {asin: row.`:START_ID(Product)`})
        MATCH (p2:Product {asin: row.`:END_ID(Product)`})
        CREATE (p1)-[:SIMILAR]->(p2)
    } IN TRANSACTIONS OF 5000 ROWS
    """
    
    start = time.time()
    with driver.session() as session:
        result = session.run(query, node_index=node_index)
        summary = result.consume()
    
    elapsed = time.time() - start
    rels_created = summary.counters.relationships_created
    print(f"[{node_name}] Created {rels_created:,} SIMILAR relationships in {elapsed:.1f}s")
    return rels_created

def load_reviews(driver, node_name, node_index):
    """Load review relationships - distributed by line number mod 3"""
    print(f"\n[{node_name}] Loading REVIEWED relationships (every 3rd row starting at {node_index})...")
    
    query = """
    LOAD CSV WITH HEADERS FROM 'file:///reviews.csv' AS row
    WITH row, toInteger(linenumber()) AS linenum
    WHERE linenum % 3 = $node_index AND row.`:START_ID(Customer)` IS NOT NULL AND row.`:END_ID(Product)` IS NOT NULL
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
        result = session.run(query, node_index=node_index)
        summary = result.consume()
    
    elapsed = time.time() - start
    rels_created = summary.counters.relationships_created
    print(f"[{node_name}] Created {rels_created:,} REVIEWED relationships in {elapsed:.1f}s")
    return rels_created

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
    
    # Connect to all nodes
    drivers = []
    for node in NODES:
        driver = GraphDatabase.driver(node["uri"], auth=("neo4j", PASSWORD))
        drivers.append((driver, node["name"], node["index"]))
    
    print("="*70)
    print("DISTRIBUTED BULK CSV LOAD - 3 Nodes")
    print("="*70)
    
    try:
        # Step 1: Clear all databases
        print("\n" + "="*70)
        print("STEP 1: Clearing All Databases")
        print("="*70)
        for driver, node_name, _ in drivers:
            clear_database(driver, node_name)
        
        # Step 2: Create indexes
        print("\n" + "="*70)
        print("STEP 2: Creating Indexes on All Nodes")
        print("="*70)
        for driver, node_name, _ in drivers:
            create_indexes(driver, node_name)
        
        # Step 3: Load products (distributed)
        print("\n" + "="*70)
        print("STEP 3: Loading Products (Distributed)")
        print("="*70)
        total_products = 0
        for driver, node_name, node_index in drivers:
            count = load_products(driver, node_name, node_index)
            total_products += count
        print(f"\nTotal products across all nodes: {total_products:,}")
        
        # Step 4: Load categories (distributed)
        print("\n" + "="*70)
        print("STEP 4: Loading Categories (Distributed)")
        print("="*70)
        total_categories = 0
        for driver, node_name, node_index in drivers:
            count = load_categories(driver, node_name, node_index)
            total_categories += count
        print(f"\nTotal categories across all nodes: {total_categories:,}")
        
        # Step 5: Load customers (distributed)
        print("\n" + "="*70)
        print("STEP 5: Loading Customers (Distributed)")
        print("="*70)
        total_customers = 0
        for driver, node_name, node_index in drivers:
            count = load_customers(driver, node_name, node_index)
            total_customers += count
        print(f"\nTotal customers across all nodes: {total_customers:,}")
        
        # Step 6: Load BELONGS_TO relationships (distributed)
        print("\n" + "="*70)
        print("STEP 6: Loading BELONGS_TO Relationships (Distributed)")
        print("="*70)
        total_belongs_to = 0
        for driver, node_name, node_index in drivers:
            count = load_belongs_to(driver, node_name, node_index)
            total_belongs_to += count
        print(f"\nTotal BELONGS_TO relationships: {total_belongs_to:,}")
        
        # Step 7: Load SIMILAR relationships (distributed)
        print("\n" + "="*70)
        print("STEP 7: Loading SIMILAR Relationships (Distributed)")
        print("="*70)
        total_similar = 0
        for driver, node_name, node_index in drivers:
            count = load_similar(driver, node_name, node_index)
            total_similar += count
        print(f"\nTotal SIMILAR relationships: {total_similar:,}")
        
        # Step 8: Load REVIEWED relationships (distributed)
        print("\n" + "="*70)
        print("STEP 8: Loading REVIEWED Relationships (Distributed)")
        print("="*70)
        total_reviews = 0
        for driver, node_name, node_index in drivers:
            count = load_reviews(driver, node_name, node_index)
            total_reviews += count
        print(f"\nTotal REVIEWED relationships: {total_reviews:,}")
        
        # Final counts per node
        print("\n" + "="*70)
        print("FINAL VERIFICATION - Per Node Counts")
        print("="*70)
        for driver, node_name, _ in drivers:
            counts = get_counts(driver, node_name)
            print(f"\n{node_name}:")
            for label, count in counts.items():
                print(f"  {label}: {count:,}")
        
        total_elapsed = time.time() - total_start
        print("\n" + "="*70)
        print(f"DISTRIBUTED LOAD COMPLETE - Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
        print("="*70)
        print(f"Total Products: {total_products:,}")
        print(f"Total Categories: {total_categories:,}")
        print(f"Total Customers: {total_customers:,}")
        print(f"Total BELONGS_TO: {total_belongs_to:,}")
        print(f"Total SIMILAR: {total_similar:,}")
        print(f"Total REVIEWED: {total_reviews:,}")
        print("="*70)
        
    finally:
        # Close all connections
        for driver, _, _ in drivers:
            driver.close()

if __name__ == "__main__":
    main()
