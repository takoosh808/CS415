# Neo4j database manager - handles putting data into Neo4j

from neo4j import GraphDatabase
import os

class Neo4jManager:
    def __init__(self):
        # Connect to Neo4j
        self.driver = None
        self.database = "amazon-analysis"  # our database name
        
    def connect(self):
        # Connect to the database
        try:
            uri = "bolt://localhost:7687"
            user = "neo4j" 
            password = "Password"  # Updated to match .env file
            
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            print("Connected to Neo4j")
            return True
        except Exception as e:
            print(f"Failed to connect: {e}")
            return False
    
    def close_connection(self):
        if self.driver:
            self.driver.close()
            print("Connection closed")
    
    def clear_database(self):
        # Clear all data from database
        print("Clearing database...")
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared")
    
    def create_indexes(self):
        # Create some indexes for better performance
        print("Creating indexes...")
        with self.driver.session(database=self.database) as session:
            # Product indexes
            session.run("CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)")
            session.run("CREATE INDEX product_id IF NOT EXISTS FOR (p:Product) ON (p.id)")
            
            # Customer index
            session.run("CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.customer_id)")
            
            # Category index
            session.run("CREATE INDEX category_name IF NOT EXISTS FOR (cat:Category) ON (cat.name)")
        
        print("Indexes created")
    
    def insert_products(self, products):
        # Insert all products into database
        print(f"Inserting {len(products)} products...")
        
        with self.driver.session(database=self.database) as session:
            for i, product in enumerate(products):
                # Create product node
                query = """
                CREATE (p:Product {
                    id: $id,
                    asin: $asin,
                    title: $title,
                    group: $group,
                    salesrank: $salesrank,
                    avg_rating: $avg_rating,
                    total_reviews: $total_reviews
                })
                """
                
                session.run(query, 
                    id=product.get('id'),
                    asin=product.get('asin'),
                    title=product.get('title', ''),
                    group=product.get('group', ''),
                    salesrank=product.get('salesrank'),
                    avg_rating=product.get('avg_rating'),
                    total_reviews=product.get('total_reviews', 0)
                )
                
                # Progress update
                if (i + 1) % 1000 == 0:
                    print(f"Inserted {i + 1} products")
        
        print("Products inserted")
    
    def insert_customers(self, reviews):
        # Insert unique customers from reviews
        print("Inserting customers...")
        
        # Get unique customer IDs
        customer_ids = set()
        for review in reviews:
            if 'customer_id' in review:
                customer_ids.add(review['customer_id'])
        
        print(f"Found {len(customer_ids)} unique customers")
        
        with self.driver.session(database=self.database) as session:
            for customer_id in customer_ids:
                query = "CREATE (c:Customer {customer_id: $customer_id})"
                session.run(query, customer_id=customer_id)
        
        print("Customers inserted")
    
    def insert_categories(self, categories):
        # Insert all categories
        print(f"Inserting {len(categories)} categories...")
        
        with self.driver.session(database=self.database) as session:
            for category in categories:
                query = "CREATE (cat:Category {name: $name})"
                session.run(query, name=category)
        
        print("Categories inserted")
    
    def insert_reviews(self, reviews):
        # Create review relationships between customers and products
        print(f"Creating {len(reviews)} review relationships...")
        
        with self.driver.session(database=self.database) as session:
            for i, review in enumerate(reviews):
                query = """
                MATCH (c:Customer {customer_id: $customer_id})
                MATCH (p:Product {id: $product_id})
                CREATE (c)-[r:REVIEWED {
                    rating: $rating,
                    date: $date,
                    votes: $votes,
                    helpful_votes: $helpful_votes
                }]->(p)
                """
                
                session.run(query,
                    customer_id=review.get('customer_id'),
                    product_id=review.get('product_id'),
                    rating=review.get('rating'),
                    date=review.get('date', ''),
                    votes=review.get('votes', 0),
                    helpful_votes=review.get('helpful_votes', 0)
                )
                
                # Progress update
                if (i + 1) % 5000 == 0:
                    print(f"Created {i + 1} review relationships")
        
        print("Review relationships created")
    
    def create_co_purchasing(self):
        # Create co-purchasing relationships
        # This finds products bought by same customers
        print("Creating co-purchasing relationships...")
        
        with self.driver.session(database=self.database) as session:
            # Simple version - find products reviewed by same customer
            query = """
            MATCH (c:Customer)-[:REVIEWED]->(p1:Product)
            MATCH (c)-[:REVIEWED]->(p2:Product)
            WHERE p1.id < p2.id
            MERGE (p1)-[co:CO_PURCHASED_WITH]-(p2)
            ON CREATE SET co.count = 1
            ON MATCH SET co.count = co.count + 1
            """
            
            result = session.run(query)
            print("Co-purchasing relationships created")
    
    def get_stats(self):
        # Get database statistics
        with self.driver.session(database=self.database) as session:
            # Count nodes
            result = session.run("MATCH (n) RETURN labels(n)[0] as label, count(n) as count")
            stats = {}
            for record in result:
                stats[record['label']] = record['count']
            
            # Count relationships
            result = session.run("MATCH ()-[r]-() RETURN type(r) as rel_type, count(r) as count")
            for record in result:
                stats[record['rel_type'] + '_relationships'] = record['count']
            
            return stats
    
    def load_all_data(self, cleaned_data):
        # Main function to load all data
        print("Starting data load...")
        
        # Connect first
        if not self.connect():
            return False
        
        # Clear database
        self.clear_database()
        
        # Create indexes
        self.create_indexes()
        
        # Insert data
        self.insert_products(cleaned_data['products'])
        self.insert_customers(cleaned_data['reviews'])
        self.insert_categories(cleaned_data['categories'])
        self.insert_reviews(cleaned_data['reviews'])
        
        # Create co-purchasing (this might take a while)
        self.create_co_purchasing()
        
        # Get final stats
        stats = self.get_stats()
        print("Final database statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("Data load complete!")
        return True

# Test it
if __name__ == "__main__":
    manager = Neo4jManager()
    print("Neo4j manager ready")