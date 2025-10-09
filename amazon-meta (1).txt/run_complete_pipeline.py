"""
Complete Amazon Metadata Processing and Neo4j Setup
===================================================

This script orchestrates the complete data processing pipeline:
1. Data preparation and transformation
2. Neo4j database setup
3. Data ingestion
4. Validation and performance testing

Usage:
    python run_complete_pipeline.py

Requirements:
    pip install neo4j pandas numpy
"""

import subprocess
import sys
import logging
import time
from pathlib import Path
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def install_requirements():
    """Install required Python packages"""
    logger.info("Installing required packages...")
    packages = ['neo4j', 'pandas', 'numpy']
    
    for package in packages:
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            logger.info(f"Successfully installed {package}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install {package}: {e}")

def check_neo4j_connection():
    """Check if Neo4j is running and accessible"""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Password"))
        with driver.session() as session:
            session.run("RETURN 1")
        driver.close()
        logger.info("Neo4j connection successful")
        return True
    except Exception as e:
        logger.error(f"Neo4j connection failed: {e}")
        logger.error("Please ensure Neo4j is running on localhost:7687 with username 'neo4j' and password 'Password'")
        return False

def run_data_preparation():
    """Execute data preparation script"""
    logger.info("Running data preparation...")
    
    try:
        from data_preparation import AmazonDataProcessor
        
        input_file = "amazon-meta.txt"
        output_file = "processed_amazon_data.json"
        
        if not Path(input_file).exists():
            logger.error(f"Input file {input_file} not found!")
            return False
        
        # Process 50,000 products for milestone requirements (10-500MB)
        processor = AmazonDataProcessor(input_file, output_file, max_products=50000)
        processor.process_data()
        
        return True
        
    except Exception as e:
        logger.error(f"Data preparation failed: {e}")
        return False

def run_neo4j_ingestion():
    """Execute Neo4j data ingestion"""
    logger.info("Running Neo4j ingestion...")
    
    try:
        from neo4j_ingestion import Neo4jAmazonIngester
        
        data_file = Path("processed_amazon_data.json")
        if not data_file.exists():
            logger.error(f"Processed data file {data_file} not found!")
            return False
        
        with open(data_file, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        
        ingester = Neo4jAmazonIngester()
        
        try:
            # Setup database
            ingester.create_constraints_and_indexes()
            
            # Ingest data
            ingester.ingest_products(products_data)
            
            # Performance testing
            ingester.get_performance_metrics()
            
            return True
            
        finally:
            ingester.close()
            
    except Exception as e:
        logger.error(f"Neo4j ingestion failed: {e}")
        return False

def generate_validation_queries():
    """Generate additional validation queries for the report"""
    logger.info("Generating validation queries...")
    
    validation_queries = {
        "Data Volume Validation": [
            "MATCH (p:Product) RETURN count(p) as total_products",
            "MATCH (c:Category) RETURN count(c) as total_categories", 
            "MATCH (cust:Customer) RETURN count(cust) as total_customers",
            "MATCH ()-[r]->() RETURN type(r) as relationship_type, count(r) as count ORDER BY count DESC"
        ],
        
        "Data Quality Validation": [
            "MATCH (p:Product) WHERE p.title IS NULL OR p.title = '' RETURN count(p) as products_without_title",
            "MATCH (p:Product) WHERE p.group IS NULL OR p.group = '' RETURN count(p) as products_without_group",
            "MATCH (p:Product) WHERE p.avg_rating < 1 OR p.avg_rating > 5 RETURN count(p) as invalid_ratings",
            "MATCH (p:Product) RETURN p.group, count(p) as count ORDER BY count DESC LIMIT 10"
        ],
        
        "Business Intelligence Queries": [
            "MATCH (p:Product)-[:BELONGS_TO]->(c:Category {name: 'Book'}) WHERE p.avg_rating IS NOT NULL RETURN avg(p.avg_rating) as avg_book_rating",
            "MATCH (p:Product) WHERE p.salesrank IS NOT NULL RETURN min(p.salesrank) as best_rank, max(p.salesrank) as worst_rank",
            "MATCH (p:Product)<-[r:REVIEWED]-(c:Customer) RETURN c.customer_id, count(r) as review_count ORDER BY review_count DESC LIMIT 10",
            "MATCH (p1:Product)-[:SIMILAR_TO]->(p2:Product) RETURN p1.asin, count(p2) as similar_count ORDER BY similar_count DESC LIMIT 10"
        ],
        
        "Performance Test Queries": [
            "MATCH (p:Product {asin: '0827229534'}) RETURN p",
            "MATCH (p:Product)-[:BELONGS_TO]->(c:Category) WHERE c.name CONTAINS 'Religion' RETURN count(p)",
            "MATCH path = (p1:Product)-[:SIMILAR_TO*1..2]->(p2:Product) WHERE p1.asin = '0827229534' RETURN length(path), count(*)",
            "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) WHERE r.rating = 5 RETURN c.customer_id, count(r) as five_star_reviews ORDER BY five_star_reviews DESC LIMIT 5"
        ]
    }
    
    # Save queries to file for report
    with open("validation_queries.json", "w") as f:
        json.dump(validation_queries, f, indent=2)
    
    logger.info("Validation queries saved to validation_queries.json")

def main():
    """Main pipeline execution"""
    logger.info("Starting complete Amazon metadata processing pipeline...")
    start_time = time.time()
    
    # Step 1: Install requirements
    install_requirements()
    
    # Step 2: Check Neo4j connection
    if not check_neo4j_connection():
        logger.error("Pipeline aborted due to Neo4j connection issues")
        return
    
    # Step 3: Data preparation
    if not run_data_preparation():
        logger.error("Pipeline aborted due to data preparation issues")
        return
    
    # Step 4: Neo4j ingestion
    if not run_neo4j_ingestion():
        logger.error("Pipeline aborted due to ingestion issues")
        return
    
    # Step 5: Generate validation queries
    generate_validation_queries()
    
    total_time = time.time() - start_time
    logger.info(f"Complete pipeline finished successfully in {total_time:.2f} seconds")
    
    # Summary
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY:")
    logger.info("- Data processed and reduced to milestone requirements (10-500MB)")
    logger.info("- Neo4j database configured with appropriate schema")
    logger.info("- Data successfully ingested with validation")
    logger.info("- Performance metrics collected")
    logger.info("- Validation queries generated for report")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()