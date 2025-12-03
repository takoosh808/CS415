"""
Parallel Bulk Data Loader for Neo4j Cluster
============================================

This script loads Amazon product data into multiple Neo4j instances
simultaneously using multi-threading for faster cluster setup.

PURPOSE:
--------
When running a Neo4j cluster (multiple database instances for redundancy
or read scaling), each node needs the same data. This script:
1. Connects to all 3 cluster nodes in parallel
2. Clears existing data
3. Loads CSV data into each node
4. Creates necessary indexes for performance

CLUSTER ARCHITECTURE:
---------------------
This assumes a 3-node setup (common for high availability):
- Node1 (port 7687): Primary node
- Node2 (port 7688): Replica/Read node
- Node3 (port 7689): Replica/Read node

In production, this replication would be handled automatically by
Neo4j clustering. This manual approach is for testing/development.

DATA MODEL:
-----------
The script creates these graph elements:

NODES:
- Product: Amazon products (asin, title, group, salesrank, ratings)
- Customer: Reviewers (customer_id)
- Category: Product categories (name)

RELATIONSHIPS:
- (Product)-[:BELONGS_TO]->(Category)
- (Product)-[:SIMILAR]->(Product)
- (Customer)-[:REVIEWED {rating, votes, helpful}]->(Product)

CSV FILES REQUIRED:
-------------------
- products.csv: Product nodes
- categories.csv: Category nodes
- customers.csv: Customer nodes
- belongs_to.csv: Product-Category edges
- similar.csv: Product-Product similarity edges
- reviews.csv: Customer-Product review edges

Files must be in Neo4j's import directory (configured in neo4j.conf).

THREADING:
----------
Uses Python's threading module to load all 3 nodes simultaneously.
This is effective because the bottleneck is network I/O to Neo4j,
not CPU processing.

DEPENDENCIES:
- neo4j: Official Neo4j Python driver

USAGE:
------
    python bulk_load_replicated.py
    
    # Ensure CSV files are in each node's import directory first
"""

from neo4j import GraphDatabase
import time
import threading


# ============================================================================
# CLUSTER CONFIGURATION
# ============================================================================
# Define all nodes in the Neo4j cluster
# Each node runs on a different port on localhost (for local testing)
# In production, these would be different servers

NODES = [
    {"uri": "bolt://localhost:7687", "name": "Node1"},  # Primary
    {"uri": "bolt://localhost:7688", "name": "Node2"},  # Replica 1
    {"uri": "bolt://localhost:7689", "name": "Node3"}   # Replica 2
]

# Authentication password (same for all nodes in this setup)
PASSWORD = "Password"


# ============================================================================
# DATABASE PREPARATION FUNCTIONS
# ============================================================================

def clear_database(driver, node_name):
    """
    Remove all existing data from the database.
    
    CYPHER: MATCH (n) DETACH DELETE n
    - MATCH (n): Find all nodes
    - DETACH DELETE: Delete nodes AND their relationships
    
    WARNING: This is destructive! Use only for fresh loads.
    
    Parameters:
    -----------
    driver : GraphDatabase.driver
        Active Neo4j connection
    node_name : str
        Node identifier for logging
    """
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


def create_indexes(driver, node_name):
    """
    Create indexes for faster lookups during relationship creation.
    
    WHY INDEXES MATTER:
    -------------------
    When creating relationships, Neo4j must find the source and target
    nodes. Without indexes, this requires scanning ALL nodes (slow).
    With indexes, it's a direct lookup (fast).
    
    CREATE INDEX ... IF NOT EXISTS:
    - Creates index only if it doesn't already exist
    - Prevents errors on repeated runs
    
    INDEXES CREATED:
    - Product.asin: Primary key for products
    - Customer.customer_id: Primary key for customers  
    - Category.name: Primary key for categories
    
    Parameters:
    -----------
    driver : GraphDatabase.driver
        Active Neo4j connection
    node_name : str
        Node identifier for logging
    """
    with driver.session() as session:
        # Index for finding products by ASIN
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        # Index for finding customers by ID
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)")
        # Index for finding categories by name
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")


# ============================================================================
# NODE LOADING FUNCTIONS
# ============================================================================

def load_products(driver, node_name):
    """
    Load Product nodes from CSV file.
    
    CYPHER BREAKDOWN:
    -----------------
    LOAD CSV WITH HEADERS FROM 'file:///products.csv' AS row
    - Read CSV file from Neo4j's import directory
    - WITH HEADERS: First row contains column names
    - AS row: Each subsequent row becomes a 'row' object
    
    WITH row WHERE row.`asin:ID(Product)` IS NOT NULL
    - Filter out rows with null ASIN (data quality)
    
    CALL { ... } IN TRANSACTIONS OF 5000 ROWS
    - Process in batches of 5000 to avoid memory issues
    - Each batch is a separate transaction
    
    CREATE (p:Product {...})
    - Create new Product node with properties
    
    CASE WHEN ... THEN ... ELSE ... END
    - Handle missing/empty values with defaults
    - toInteger/toFloat: Convert strings to proper types
    
    Returns:
    --------
    int
        Number of Product nodes created
    """
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
        summary = result.consume()  # Get execution statistics
    elapsed = time.time() - start
    nodes_created = summary.counters.nodes_created
    return nodes_created


def load_categories(driver, node_name):
    """
    Load Category nodes from CSV file.
    
    Uses MERGE instead of CREATE to avoid duplicate categories.
    MERGE: Create if doesn't exist, otherwise match existing.
    
    Returns:
    --------
    int
        Number of Category nodes created
    """
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
    return nodes_created


def load_customers(driver, node_name):
    """
    Load Customer nodes from CSV file.
    
    Uses CREATE (not MERGE) because customer IDs should be unique.
    
    Returns:
    --------
    int
        Number of Customer nodes created
    """
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
    return nodes_created


# ============================================================================
# RELATIONSHIP LOADING FUNCTIONS
# ============================================================================

def load_belongs_to(driver, node_name):
    """
    Create BELONGS_TO relationships (Product -> Category).
    
    CYPHER PATTERN:
    ---------------
    MATCH (p:Product {asin: ...})
    MATCH (c:Category {name: ...})
    CREATE (p)-[:BELONGS_TO]->(c)
    
    This links products to their categories. A product can belong
    to multiple categories.
    
    Returns:
    --------
    int
        Number of relationships created
    """
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
    return rels_created


def load_similar(driver, node_name):
    """
    Create SIMILAR relationships (Product -> Product).
    
    These represent Amazon's "customers who bought this also bought"
    or "similar items" links. This is a graph edge between two products.
    
    Returns:
    --------
    int
        Number of relationships created
    """
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
    return rels_created


def load_reviews(driver, node_name):
    """
    Create REVIEWED relationships (Customer -> Product).
    
    This is the most important relationship for pattern mining:
    it shows which customers reviewed which products.
    
    RELATIONSHIP PROPERTIES:
    - rating: Star rating (1-5)
    - votes: Number of people who voted on this review
    - helpful: Number who found the review helpful
    
    Returns:
    --------
    int
        Number of relationships created
    """
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
    return rels_created


# ============================================================================
# ORCHESTRATION FUNCTIONS
# ============================================================================

def load_node(node_config):
    """
    Load all data into a single Neo4j node.
    
    This is the target function for each thread - it handles
    one complete node from start to finish.
    
    LOADING ORDER MATTERS:
    ----------------------
    1. Clear database (clean slate)
    2. Create indexes (for fast lookups)
    3. Load Products (nodes first)
    4. Load Categories (nodes)
    5. Load Customers (nodes)
    6. Load BELONGS_TO (relationships - need nodes to exist)
    7. Load SIMILAR (relationships)
    8. Load REVIEWED (relationships)
    
    Parameters:
    -----------
    node_config : dict
        Dictionary with 'uri' and 'name' keys
    """
    # Connect to this specific node
    driver = GraphDatabase.driver(node_config["uri"], auth=("neo4j", PASSWORD))
    node_name = node_config["name"]
    
    try:
        # Execute loading pipeline
        clear_database(driver, node_name)
        create_indexes(driver, node_name)
        load_products(driver, node_name)
        load_categories(driver, node_name)
        load_customers(driver, node_name)
        load_belongs_to(driver, node_name)
        load_similar(driver, node_name)
        load_reviews(driver, node_name)
    finally:
        # Always close the connection
        driver.close()


def get_counts(driver, node_name):
    """
    Get count of each node type in the database.
    
    Useful for verifying all nodes were loaded correctly.
    
    CYPHER EXPLAINED:
    -----------------
    MATCH (n): Find all nodes
    labels(n)[0]: Get the first label of each node
    count(n): Count nodes with that label
    
    Returns:
    --------
    dict
        Mapping of label name to count
    """
    with driver.session() as session:
        result = session.run("""
            MATCH (n)
            RETURN labels(n)[0] AS label, count(n) AS count
            ORDER BY label
        """)
        counts = {record["label"]: record["count"] for record in result}
    return counts


def main():
    """
    Main entry point - orchestrates parallel loading of all cluster nodes.
    
    PARALLEL EXECUTION:
    -------------------
    1. Start a thread for each cluster node
    2. Each thread loads data independently
    3. Wait for all threads to complete
    4. Verify counts on each node
    
    This is much faster than loading nodes sequentially because
    each node's loading is I/O bound (waiting for Neo4j).
    """
    total_start = time.time()
    
    # Start parallel loading threads
    threads = []
    for node in NODES:
        thread = threading.Thread(target=load_node, args=(node,))
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Verify data was loaded correctly on each node
    for node in NODES:
        driver = GraphDatabase.driver(node["uri"], auth=("neo4j", PASSWORD))
        try:
            get_counts(driver, node["name"])
        finally:
            driver.close()
    
    total_elapsed = time.time() - total_start


if __name__ == "__main__":
    main()
