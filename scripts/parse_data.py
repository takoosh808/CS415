"""
Main data parsing and preprocessing script
Processes the amazon-meta.txt file and prepares data for Neo4j import
"""

import sys
import os
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from data_processing.parser import AmazonDataParser
from data_processing.preprocessor import DataPreprocessor

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/data_processing.log'),
            logging.StreamHandler()
        ]
    )

def main():
    """Main data processing pipeline"""
    print("=== Amazon Data Processing Pipeline ===")
    print("Parsing amazon-meta.txt and preparing data for Neo4j import...")
    
    # Setup logging
    os.makedirs('logs', exist_ok=True)
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Step 1: Parse raw data
        logger.info("Step 1: Parsing amazon-meta.txt")
        parser = AmazonDataParser()
        
        # Check if file exists
        data_file = "amazon-meta.txt"
        if not os.path.exists(data_file):
            raise FileNotFoundError(f"Data file not found: {data_file}")
        
        # Parse the data
        parsed_data = parser.parse_file(data_file)
        
        # Create output directories
        os.makedirs('data/parsed', exist_ok=True)
        os.makedirs('data/cleaned', exist_ok=True)
        
        # Save parsed data
        parser.save_parsed_data('data/parsed/')
        
        print(f" Parsing complete:")
        print(f"  - Products: {len(parsed_data['products']):,}")
        print(f"  - Reviews: {len(parsed_data['reviews']):,}")
        print(f"  - Categories: {len(parsed_data['categories']):,}")
        print(f"  - Similar relationships: {len(parsed_data['similar_products']):,}")
        
        # Step 2: Preprocess data
        logger.info("Step 2: Preprocessing and cleaning data")
        preprocessor = DataPreprocessor()
        
        cleaned_data = preprocessor.preprocess_data(parsed_data)
        
        # Save cleaned data
        preprocessor.save_cleaned_data('data/cleaned/')
        
        # Get and display statistics
        stats = preprocessor.get_statistics()
        
        print(f"\\n Preprocessing complete:")
        print(f"  - Cleaned products: {stats['total_products']:,}")
        print(f"  - Cleaned reviews: {stats['total_reviews']:,}")
        print(f"  - Unique categories: {stats['total_categories']:,}")
        print(f"  - Clean relationships: {stats['total_relationships']:,}")
        
        print(f"\\nProduct distribution by group:")
        for group, count in stats['products_by_group'].items():
            print(f"  - {group}: {count:,}")
        
        print(f"\\nRating distribution:")
        for rating, count in sorted(stats['rating_distribution'].items()):
            print(f"  - {rating} stars: {count:,}")
        
        print(f"\\nReview count statistics:")
        for stat, value in stats['review_count_stats'].items():
            print(f"  - {stat}: {value:.1f}")
        
        print("\\n=== Data Processing Complete ===")
        print("Data is ready for Neo4j import. Run 'python scripts/import_to_neo4j.py' next.")
        
    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        print(f"\\n Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()