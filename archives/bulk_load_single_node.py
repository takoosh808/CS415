"""
Bulk Data Loader for Single-Node Neo4j Database
================================================

This script loads Amazon product data from CSV files into a Neo4j graph database.
It's designed for a single Neo4j instance (not a cluster setup).

WHAT IS NEO4J?
--------------
Neo4j is a graph database that stores data differently from traditional databases:
- Instead of tables with rows, Neo4j uses NODES (entities) and RELATIONSHIPS (connections)
- Nodes have LABELS (types like Product, Customer) and PROPERTIES (attributes like title, rating)
- Relationships connect nodes and can also have properties (like rating on a REVIEWED relationship)

EXAMPLE GRAPH STRUCTURE:
    (Customer)-[:REVIEWED {rating: 5}]->(Product)-[:BELONGS_TO]->(Category)
    
    This reads as: "A Customer REVIEWED a Product with rating 5, 
                    and that Product BELONGS_TO a Category"

DATA BEING LOADED:
------------------
1. Products: Amazon products with ASIN, title, group, sales rank, ratings
2. Categories: Product categories (Books, Electronics, etc.)
3. Customers: Customer identifiers (anonymized)
4. BELONGS_TO: Links products to their categories
5. SIMILAR: Links similar/related products
6. REVIEWED: Customer reviews with ratings and dates

CSV FILES REQUIRED (in Neo4j's import folder):
- products.csv
- categories.csv
- customers.csv
- belongs_to.csv
- similar.csv
- reviews.csv

CYPHER COMMANDS EXPLAINED:
--------------------------
- LOAD CSV: Reads data from a CSV file
- CREATE: Creates new nodes or relationships
- MATCH: Finds existing nodes or patterns
- MERGE: Creates if doesn't exist, otherwise matches
- IN TRANSACTIONS OF X ROWS: Batches operations to avoid memory issues

DEPENDENCIES:
- neo4j: Official Python driver for Neo4j
"""

from neo4j import GraphDatabase  # Official Neo4j Python driver
import time  # For measuring load times


def clear_database(driver):
    """
    Delete all nodes and relationships from the database.
    
    WARNING: This is destructive! All data will be permanently removed.
    Used to ensure a clean slate before loading new data.
    
    CYPHER: MATCH (n) DETACH DELETE n
    - MATCH (n): Match all nodes (n is a variable)
    - DETACH DELETE: Remove nodes AND all their relationships
    """
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


def create_indexes(driver):
    """
    Create database indexes on frequently-queried properties.
    
    WHY INDEXES MATTER:
    Without an index, finding a product by ASIN requires scanning ALL products.
    With an index, Neo4j can jump directly to the matching product (like
    using a book's index instead of reading every page).
    
    CYPHER: CREATE INDEX name IF NOT EXISTS FOR (label) ON (property)
    - IF NOT EXISTS: Only create if index doesn't already exist
    - FOR (p:Product) ON (p.asin): Index the 'asin' property of 'Product' nodes
    """
    with driver.session() as session:
        # Index for looking up products by their ASIN (Amazon ID)
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        # Index for looking up categories by name
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")
        # Index for looking up customers by ID
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.id)")


def load_products(driver):
    """
    Load product nodes from products.csv into Neo4j.
    
    Each row becomes a Product node with properties:
    - asin: Amazon Standard Identification Number (unique product ID)
    - title: Product name
    - group: Product category group (Book, DVD, Music, Video)
    - salesrank: Amazon sales rank (lower = selling better)
    - avg_rating: Average customer rating (1-5 stars)
    - total_reviews: Number of customer reviews
    
    CYPHER EXPLAINED:
    - LOAD CSV WITH HEADERS: Read CSV, first row contains column names
    - FROM 'file:///products.csv': File path (relative to Neo4j import folder)
    - WITH row WHERE condition: Filter out invalid rows
    - toInteger(), toFloat(): Convert string values to proper types
    - IN TRANSACTIONS OF 10000 ROWS: Commit every 10,000 rows (memory efficiency)
    """
    start = time.time()
    with driver.session() as session:
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
        # Verify load by counting products
        result = session.run("MATCH (p:Product) RETURN count(p) as count")
        count = result.single()['count']
    elapsed = time.time() - start
    return elapsed


def load_categories(driver):
    """
    Load category nodes from categories.csv.
    
    Uses MERGE instead of CREATE to avoid duplicate categories.
    MERGE = "find or create" - if category exists, use it; otherwise create it.
    """
    start = time.time()
    with driver.session() as session:
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
    return elapsed


def load_customers(driver):
    """
    Load customer nodes from customers.csv.
    
    Customers are anonymized - we only have an ID, no personal information.
    This is typical for privacy-respecting datasets.
    """
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
    return elapsed


def load_belongs_to(driver):
    """
    Load BELONGS_TO relationships linking Products to Categories.
    
    This creates directed edges: (Product)-[:BELONGS_TO]->(Category)
    
    HOW RELATIONSHIP LOADING WORKS:
    1. MATCH (p:Product {asin: ...}): Find the source product node
    2. MATCH (c:Category {name: ...}): Find the target category node
    3. CREATE (p)-[:BELONGS_TO]->(c): Create the relationship between them
    
    Note: Using smaller transaction batches (5000) because relationship
    creation with MATCH lookups is more memory-intensive.
    """
    start = time.time()
    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///belongs_to.csv' AS row
            CALL {
                WITH row
                MATCH (p:Product {asin: row.`:START_ID(Product)`})
                MATCH (c:Category {name: row.`:END_ID(Category)`})
                CREATE (p)-[:BELONGS_TO]->(c)
            } IN TRANSACTIONS OF 5000 ROWS
        """)
        result = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count")
        count = result.single()['count']
    elapsed = time.time() - start
    return elapsed


def load_similar(driver):
    """
    Load SIMILAR relationships between related products.
    
    Creates: (Product1)-[:SIMILAR]->(Product2)
    
    These relationships indicate that Amazon considers two products
    to be similar or related (often shown as "Customers also viewed...").
    """
    start = time.time()
    with driver.session() as session:
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///similar.csv' AS row
            CALL {
                WITH row
                MATCH (p1:Product {asin: row.`:START_ID(Product)`})
                MATCH (p2:Product {asin: row.`:END_ID(Product)`})
                CREATE (p1)-[:SIMILAR]->(p2)
            } IN TRANSACTIONS OF 5000 ROWS
        """)
        result = session.run("MATCH ()-[r:SIMILAR]->() RETURN count(r) as count")
        count = result.single()['count']
    elapsed = time.time() - start
    return elapsed


def load_reviews(driver):
    """
    Load REVIEWED relationships with customer ratings.
    
    Creates: (Customer)-[:REVIEWED {rating, date, helpful}]->(Product)
    
    This is special because the relationship itself has properties:
    - rating: 1-5 star rating
    - date: When the review was written
    - helpful: Number of "helpful" votes the review received
    """
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
            } IN TRANSACTIONS OF 5000 ROWS
        """)
        result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
        count = result.single()['count']
    elapsed = time.time() - start
    return elapsed


def verify_data(driver):
    """
    Count all loaded entities to verify the data load was successful.
    
    Returns a dictionary with counts of each node type and relationship type.
    This is useful for validation and debugging.
    """
    with driver.session() as session:
        results = {}
        # Count each type of node
        result = session.run("MATCH (p:Product) RETURN count(p) as count")
        results['Products'] = result.single()['count']
        result = session.run("MATCH (c:Category) RETURN count(c) as count")
        results['Categories'] = result.single()['count']
        result = session.run("MATCH (c:Customer) RETURN count(c) as count")
        results['Customers'] = result.single()['count']
        # Count each type of relationship
        result = session.run("MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count")
        results['BELONGS_TO'] = result.single()['count']
        result = session.run("MATCH ()-[r:SIMILAR]->() RETURN count(r) as count")
        results['SIMILAR'] = result.single()['count']
        result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
        results['REVIEWED'] = result.single()['count']
    return results


def main():
    """
    Main entry point - orchestrates the complete data loading process.
    
    Steps:
    1. Connect to Neo4j
    2. Clear any existing data
    3. Create indexes for fast lookups
    4. Load nodes (Products, Categories, Customers)
    5. Load relationships (BELONGS_TO, SIMILAR, REVIEWED)
    6. Verify the loaded data
    7. Report timing statistics
    """
    # Connect to Neo4j using the Bolt protocol
    # auth=(username, password) for authentication
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Password"))
    
    try:
        total_start = time.time()
        
        # Step 1: Clear existing data for a fresh start
        clear_database(driver)
        
        # Step 2: Create indexes before loading data (improves MATCH performance)
        create_indexes(driver)
        
        # Step 3: Load all data, tracking time for each type
        times = {}
        times['products'] = load_products(driver)
        times['categories'] = load_categories(driver)
        times['customers'] = load_customers(driver)
        times['belongs_to'] = load_belongs_to(driver)
        times['similar'] = load_similar(driver)
        times['reviews'] = load_reviews(driver)
        
        # Step 4: Verify loaded counts
        counts = verify_data(driver)
        
        total_time = time.time() - total_start
        
        return {
            'times': times,
            'counts': counts,
            'total_time': total_time
        }
    finally:
        # Always close the driver connection to free resources
        driver.close()


# Only run main() if this script is executed directly (not imported)
if __name__ == "__main__":
    main()
