from neo4j import GraphDatabase
import time

def clear_database(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def create_indexes(driver):
    with driver.session() as session:
        session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
        session.run("CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)")
        session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.id)")

def load_products(driver):
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
        result = session.run("MATCH (p:Product) RETURN count(p) as count")
        count = result.single()['count']
    elapsed = time.time() - start
    return elapsed

def load_categories(driver):
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
    return results

def main():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Password"))
    try:
        total_start = time.time()
        clear_database(driver)
        create_indexes(driver)
        times = {}
        times['products'] = load_products(driver)
        times['categories'] = load_categories(driver)
        times['customers'] = load_customers(driver)
        times['belongs_to'] = load_belongs_to(driver)
        times['similar'] = load_similar(driver)
        times['reviews'] = load_reviews(driver)
        counts = verify_data(driver)
        total_time = time.time() - total_start
        return {
            'times': times,
            'counts': counts,
            'total_time': total_time
        }
    finally:
        driver.close()

if __name__ == "__main__":
    main()
