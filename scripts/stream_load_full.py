from neo4j import GraphDatabase
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_processing.parser import parse_amazon_data


def clear_database(driver, database):
    with driver.session(database=database) as session:
        count = session.run("MATCH (n) RETURN count(n) as cnt LIMIT 1").single()['cnt']
        
    if count == 0:
        return
    
    while True:
        with driver.session(database=database) as session:
            result = session.run("""
                MATCH (n)
                WITH n LIMIT 5000
                DETACH DELETE n
                RETURN count(n) as deleted
            """)
            deleted = result.single()['deleted']
            if deleted == 0:
                break


def stream_load_products(driver, file_path, database, batch_size=1000):
    products = parse_amazon_data(file_path)
    batch = []
    
    for product in products:
        batch.append(product)
        
        if len(batch) >= batch_size:
            load_batch(driver, batch, database)
            batch = []
    
    if batch:
        load_batch(driver, batch, database)


def load_batch(driver, batch, database):
    product_data = []
    review_data = []
    similar_data = []
    category_data = []
    
    for product in batch:
        product_data.append({
            'id': product.get('id', 0),
            'asin': product.get('asin', ''),
            'title': product.get('title', 'Unknown'),
            'group': product.get('group', 'Unknown'),
            'salesrank': product.get('salesrank', 0),
            'avg_rating': product.get('avg_rating', 0.0),
            'total_reviews': product.get('total_reviews', 0)
        })
        
        for similar_asin in product.get('similar', []):
            similar_data.append({
                'asin': product['asin'],
                'similar_asin': similar_asin
            })
        
        for category in product.get('categories', []):
            for cat_name in category:
                category_data.append({
                    'asin': product['asin'],
                    'category': cat_name
                })
        
        for review in product.get('reviews', []):
            review_data.append({
                'customer_id': review['customer'],
                'asin': product['asin'],
                'date': review['date'],
                'rating': review['rating'],
                'votes': review['votes'],
                'helpful': review['helpful']
            })
    
    with driver.session(database=database) as session:
        session.run("""
            UNWIND $products as product
            MERGE (p:Product {asin: product.asin})
            SET p.id = product.id,
                p.title = product.title,
                p.group = product.group,
                p.salesrank = product.salesrank,
                p.avg_rating = product.avg_rating,
                p.total_reviews = product.total_reviews
        """, products=product_data)
        
        if similar_data:
            session.run("""
                UNWIND $similar as sim
                MATCH (p1:Product {asin: sim.asin})
                MERGE (p2:Product {asin: sim.similar_asin})
                MERGE (p1)-[:SIMILAR_TO]->(p2)
            """, similar=similar_data)
        
        if category_data:
            session.run("""
                UNWIND $categories as cat
                MATCH (p:Product {asin: cat.asin})
                MERGE (c:Category {name: cat.category})
                MERGE (p)-[:BELONGS_TO]->(c)
            """, categories=category_data)
        
        if review_data:
            # Simplified review loading - just customer and REVIEWED relationship
            session.run("""
                UNWIND $reviews as review
                MATCH (p:Product {asin: review.asin})
                MERGE (c:Customer {id: review.customer_id})
                MERGE (c)-[:REVIEWED {rating: review.rating, date: review.date}]->(p)
            """, reviews=review_data)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Load Amazon data into Neo4j')
    parser.add_argument('--file', default='amazon-meta.txt', help='Input file path')
    args = parser.parse_args()
    
    uri = "bolt://localhost:7687"
    username = "neo4j"
    password = "Password"
    database = "amazon-analysis"
    
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    clear_database(driver, database)
    stream_load_products(driver, args.file, database)
    driver.close()


if __name__ == "__main__":
    main()
