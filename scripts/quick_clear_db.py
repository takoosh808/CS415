from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Password"))

print("Clearing database...")
with driver.session(database="amazon-analysis") as session:
    while True:
        result = session.run("MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) as deleted")
        deleted = result.single()['deleted']
        if deleted == 0:
            break
        print(f"Deleted {deleted} nodes...")

print("Database cleared!")
driver.close()
