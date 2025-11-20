from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Password"))

with driver.session(database="neo4j") as session:
    product_count = session.run("MATCH (p:Product) RETURN count(p) as cnt").single()['cnt']
    customer_count = session.run("MATCH (c:Customer) RETURN count(c) as cnt").single()['cnt']
    reviewed_count = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as cnt").single()['cnt']
    similar_count = session.run("MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as cnt").single()['cnt']
    
    print(f"Products: {product_count:,}")
    print(f"Customers: {customer_count:,}")
    print(f"Reviews: {reviewed_count:,}")
    print(f"Similarities: {similar_count:,}")

driver.close()
