"""
Neo4j Graph Database Schema Design for Amazon Product Data
=========================================================

This module defines the graph schema and provides ingestion functionality
for Amazon product metadata into Neo4j database.

Graph Schema Design:
-------------------
NODES:
- Product: Core product information
- Category: Product categories 
- Customer: Individual customers who wrote reviews

RELATIONSHIPS:
- (Product)-[:SIMILAR_TO]->(Product): Similar product recommendations
- (Product)-[:BELONGS_TO]->(Category): Product categorization
- (Customer)-[:REVIEWED]->(Product): Customer product reviews

This schema is appropriate because:
1. Graph structure naturally represents product similarities and recommendations
2. Category hierarchies are well-suited for graph traversal
3. Review relationships enable recommendation analysis
4. Scales well with Neo4j's native graph operations
5. Supports complex queries for product discovery and analysis
"""

from neo4j import GraphDatabase
import json
import logging
import time
from typing import Dict, List, Any
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Neo4jAmazonIngester:
    """Handles ingestion of Amazon product data into Neo4j"""
    
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "Password"):
        """Initialize Neo4j connection"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info(f"Connected to Neo4j at {uri}")
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
    
    def create_constraints_and_indexes(self):
        """
        Create database constraints and indexes for optimal performance:
        1. Unique constraints on primary keys
        2. Indexes on frequently queried properties
        3. Full-text search indexes for text properties
        """
        logger.info("Creating constraints and indexes...")
        
        constraints_and_indexes = [
            # Unique constraints
            "CREATE CONSTRAINT product_asin IF NOT EXISTS FOR (p:Product) REQUIRE p.asin IS UNIQUE",
            "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (cat:Category) REQUIRE cat.name IS UNIQUE",
            
            # Performance indexes
            "CREATE INDEX product_salesrank IF NOT EXISTS FOR (p:Product) ON (p.salesrank)",
            "CREATE INDEX product_group IF NOT EXISTS FOR (p:Product) ON (p.group)",
            "CREATE INDEX review_rating IF NOT EXISTS FOR ()-[r:REVIEWED]->() ON (r.rating)",
            "CREATE INDEX review_date IF NOT EXISTS FOR ()-[r:REVIEWED]->() ON (r.date)",
            
            # Full-text search index for product titles
            "CREATE FULLTEXT INDEX product_title_search IF NOT EXISTS FOR (p:Product) ON EACH [p.title]"
        ]
        
        with self.driver.session() as session:
            for query in constraints_and_indexes:
                try:
                    session.run(query)
                    logger.info(f"Executed: {query}")
                except Exception as e:
                    logger.warning(f"Failed to execute {query}: {e}")
    
    def clear_database(self):
        """Clear all data from database (for fresh ingestion)"""
        logger.info("Clearing existing data...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared")
    
    def ingest_products(self, products_data: List[Dict[str, Any]]):
        """
        Ingest product data using batch processing for performance:
        
        ALGORITHM:
        1. Create Product nodes in batches
        2. Create Category nodes for unique categories
        3. Create Customer nodes for unique customers
        4. Create SIMILAR_TO relationships between products
        5. Create BELONGS_TO relationships (Product->Category)
        6. Create REVIEWED relationships (Customer->Product) with review data
        """
        logger.info(f"Starting ingestion of {len(products_data)} products...")
        start_time = time.time()
        
        # Batch configuration
        BATCH_SIZE = 1000
        
        # Step 1: Create Product nodes
        self._create_product_nodes(products_data, BATCH_SIZE)
        
        # Step 2: Create Category nodes
        self._create_category_nodes(products_data, BATCH_SIZE)
        
        # Step 3: Create Customer nodes
        self._create_customer_nodes(products_data, BATCH_SIZE)
        
        # Step 4: Create relationships
        self._create_similar_relationships(products_data, BATCH_SIZE)
        self._create_category_relationships(products_data, BATCH_SIZE)
        self._create_review_relationships(products_data, BATCH_SIZE)
        
        ingestion_time = time.time() - start_time
        logger.info(f"Ingestion completed in {ingestion_time:.2f} seconds")
        
        # Validation queries
        self._validate_ingestion()
    
    def _create_product_nodes(self, products_data: List[Dict], batch_size: int):
        """Create Product nodes in batches"""
        logger.info("Creating Product nodes...")
        
        product_creation_query = """
        UNWIND $products AS product
        CREATE (p:Product {
            id: product.id,
            asin: product.asin,
            title: product.title,
            group: product.group,
            salesrank: product.salesrank,
            avg_rating: product.avg_rating,
            total_reviews: product.total_reviews
        })
        """
        
        with self.driver.session() as session:
            for i in range(0, len(products_data), batch_size):
                batch = products_data[i:i + batch_size]
                session.run(product_creation_query, products=batch)
                if (i // batch_size + 1) % 10 == 0:
                    logger.info(f"Created {min(i + batch_size, len(products_data))} product nodes...")
    
    def _create_category_nodes(self, products_data: List[Dict], batch_size: int):
        """Create unique Category nodes"""
        logger.info("Creating Category nodes...")
        
        # Collect unique categories
        unique_categories = set()
        for product in products_data:
            if product.get('categories'):
                unique_categories.update(product['categories'])
        
        logger.info(f"Found {len(unique_categories)} unique categories")
        
        category_creation_query = """
        UNWIND $categories AS category
        MERGE (c:Category {name: category})
        """
        
        categories_list = list(unique_categories)
        with self.driver.session() as session:
            for i in range(0, len(categories_list), batch_size):
                batch = categories_list[i:i + batch_size]
                session.run(category_creation_query, categories=batch)
    
    def _create_customer_nodes(self, products_data: List[Dict], batch_size: int):
        """Create unique Customer nodes"""
        logger.info("Creating Customer nodes...")
        
        # Collect unique customers
        unique_customers = set()
        for product in products_data:
            if product.get('reviews'):
                for review in product['reviews']:
                    if review.get('customer_id'):
                        unique_customers.add(review['customer_id'])
        
        logger.info(f"Found {len(unique_customers)} unique customers")
        
        customer_creation_query = """
        UNWIND $customers AS customer
        MERGE (c:Customer {customer_id: customer})
        """
        
        customers_list = list(unique_customers)
        with self.driver.session() as session:
            for i in range(0, len(customers_list), batch_size):
                batch = customers_list[i:i + batch_size]
                session.run(customer_creation_query, customers=batch)
    
    def _create_similar_relationships(self, products_data: List[Dict], batch_size: int):
        """Create SIMILAR_TO relationships between products"""
        logger.info("Creating SIMILAR_TO relationships...")
        
        similarity_query = """
        UNWIND $similarities AS sim
        MATCH (p1:Product {asin: sim.from_asin})
        MATCH (p2:Product {asin: sim.to_asin})
        MERGE (p1)-[:SIMILAR_TO]->(p2)
        """
        
        similarities = []
        for product in products_data:
            if product.get('similar_products'):
                for similar_asin in product['similar_products']:
                    similarities.append({
                        'from_asin': product['asin'],
                        'to_asin': similar_asin
                    })
        
        logger.info(f"Creating {len(similarities)} similarity relationships...")
        
        with self.driver.session() as session:
            for i in range(0, len(similarities), batch_size):
                batch = similarities[i:i + batch_size]
                session.run(similarity_query, similarities=batch)
    
    def _create_category_relationships(self, products_data: List[Dict], batch_size: int):
        """Create BELONGS_TO relationships between products and categories"""
        logger.info("Creating BELONGS_TO relationships...")
        
        category_relationship_query = """
        UNWIND $relationships AS rel
        MATCH (p:Product {asin: rel.product_asin})
        MATCH (c:Category {name: rel.category_name})
        MERGE (p)-[:BELONGS_TO]->(c)
        """
        
        relationships = []
        for product in products_data:
            if product.get('categories'):
                for category in product['categories']:
                    relationships.append({
                        'product_asin': product['asin'],
                        'category_name': category
                    })
        
        logger.info(f"Creating {len(relationships)} category relationships...")
        
        with self.driver.session() as session:
            for i in range(0, len(relationships), batch_size):
                batch = relationships[i:i + batch_size]
                session.run(category_relationship_query, relationships=batch)
    
    def _create_review_relationships(self, products_data: List[Dict], batch_size: int):
        """Create REVIEWED relationships with review data"""
        logger.info("Creating REVIEWED relationships...")
        
        review_relationship_query = """
        UNWIND $reviews AS review
        MATCH (c:Customer {customer_id: review.customer_id})
        MATCH (p:Product {asin: review.product_asin})
        MERGE (c)-[r:REVIEWED]->(p)
        SET r.rating = review.rating,
            r.date = review.date,
            r.votes = review.votes,
            r.helpful = review.helpful
        """
        
        reviews = []
        for product in products_data:
            if product.get('reviews'):
                for review in product['reviews']:
                    if review.get('customer_id') and review.get('rating'):
                        reviews.append({
                            'customer_id': review['customer_id'],
                            'product_asin': product['asin'],
                            'rating': review['rating'],
                            'date': review.get('date'),
                            'votes': review.get('votes'),
                            'helpful': review.get('helpful')
                        })
        
        logger.info(f"Creating {len(reviews)} review relationships...")
        
        with self.driver.session() as session:
            for i in range(0, len(reviews), batch_size):
                batch = reviews[i:i + batch_size]
                session.run(review_relationship_query, reviews=batch)
    
    def _validate_ingestion(self):
        """Run validation queries to confirm successful ingestion"""
        logger.info("Validating data ingestion...")
        
        validation_queries = [
            ("Total Products", "MATCH (p:Product) RETURN count(p) as count"),
            ("Total Categories", "MATCH (c:Category) RETURN count(c) as count"),
            ("Total Customers", "MATCH (c:Customer) RETURN count(c) as count"),
            ("SIMILAR_TO relationships", "MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as count"),
            ("BELONGS_TO relationships", "MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count"),
            ("REVIEWED relationships", "MATCH ()-[r:REVIEWED]->() RETURN count(r) as count"),
            ("Products with reviews", "MATCH (p:Product)<-[:REVIEWED]-() RETURN count(DISTINCT p) as count"),
            ("Average product rating", "MATCH (p:Product) WHERE p.avg_rating IS NOT NULL RETURN avg(p.avg_rating) as avg_rating"),
        ]
        
        with self.driver.session() as session:
            for description, query in validation_queries:
                result = session.run(query).single()
                logger.info(f"{description}: {result['count'] if 'count' in result else result}")
    
    def get_performance_metrics(self):
        """Get performance metrics for ingestion validation"""
        logger.info("Collecting performance metrics...")
        
        performance_queries = [
            # Query performance tests
            ("Find product by ASIN", "MATCH (p:Product {asin: $asin}) RETURN p", {"asin": "0827229534"}),
            ("Find products in category", "MATCH (p:Product)-[:BELONGS_TO]->(c:Category {name: $category}) RETURN count(p) as count", {"category": "Book"}),
            ("Find similar products", "MATCH (p:Product {asin: $asin})-[:SIMILAR_TO]->(similar) RETURN count(similar) as count", {"asin": "0827229534"}),
            ("Average rating by category", "MATCH (p:Product)-[:BELONGS_TO]->(c:Category {name: $category}) WHERE p.avg_rating IS NOT NULL RETURN avg(p.avg_rating) as avg_rating", {"category": "Book"}),
            ("Customer review count", "MATCH (c:Customer)-[r:REVIEWED]->() RETURN c.customer_id, count(r) as reviews ORDER BY reviews DESC LIMIT 5"),
        ]
        
        with self.driver.session() as session:
            for description, query, *params in performance_queries:
                start_time = time.time()
                if params:
                    result = session.run(query, params[0])
                else:
                    result = session.run(query)
                
                # Consume results
                records = list(result)
                query_time = time.time() - start_time
                
                logger.info(f"{description}: {query_time:.4f}s, {len(records)} results")
                if len(records) <= 5:  # Show results for small result sets
                    for record in records:
                        logger.info(f"  {dict(record)}")

def main():
    """Main ingestion function"""
    # Load processed data
    data_file = Path("processed_amazon_data.json")
    if not data_file.exists():
        logger.error(f"Data file {data_file} not found. Run data_preparation.py first.")
        return
    
    with open(data_file, 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    
    logger.info(f"Loaded {len(products_data)} products from {data_file}")
    
    # Initialize Neo4j ingester
    ingester = Neo4jAmazonIngester()
    
    try:
        # Setup database
        ingester.clear_database()  # Comment out to preserve existing data
        ingester.create_constraints_and_indexes()
        
        # Ingest data
        ingester.ingest_products(products_data)
        
        # Performance testing
        ingester.get_performance_metrics()
        
    finally:
        ingester.close()

if __name__ == "__main__":
    main()