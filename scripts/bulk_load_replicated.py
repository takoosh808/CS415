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
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def create_indexes(driver, node_name):
    with driver.session() as session:
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)")
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")

def load_products(driver, node_name):
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
    return nodes_created

def load_categories(driver, node_name):
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

def load_belongs_to(driver, node_name):
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

def load_node(node_config):
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
    finally:
        driver.close()

def get_counts(driver, node_name):
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
    threads = []
    for node in NODES:
        thread = threading.Thread(target=load_node, args=(node,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    for node in NODES:
        driver = GraphDatabase.driver(node["uri"], auth=("neo4j", PASSWORD))
        try:
            get_counts(driver, node["name"])
        finally:
            driver.close()
    total_elapsed = time.time() - total_start

if __name__ == "__main__":
    main()
