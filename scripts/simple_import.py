"""
Imports Amazon data into Neo4j database
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from data_processing.parser import AmazonDataParser
from data_processing.preprocessor import DataPreprocessor
from data_processing.neo4j_manager import Neo4jManager

def main():
    
    try:
        print("=== Neo4j Data Import ===")
        print()
        
        # Parse the test data
        print("1. Parsing Amazon data...")
        parser = AmazonDataParser()
        products = parser.parse_file('amazon-meta-test.txt')
        print(f"   Found {len(products)} products")
        
        # Preprocess the data
        print("2. Preprocessing data...")
        preprocessor = DataPreprocessor()
        cleaned_data = preprocessor.process_all_data(products)
        print(f"   Cleaned {len(cleaned_data['products'])} products")
        print(f"   Found {len(cleaned_data['reviews'])} reviews")
        
        # Connect to Neo4j and load data
        print("3. Connecting to Neo4j...")
        manager = Neo4jManager()
        
        if manager.driver is None:
            print("    Could not connect to Neo4j")
            print("   Make sure Neo4j is running and check .env settings")
            return
            
        print("    Connected successfully")
        
        # Load all data
        print("4. Loading data into Neo4j...")
        manager.load_all_data(cleaned_data)
        
        # Get final statistics
        print("5. Getting final statistics...")
        try:
            stats = manager.get_stats()
            print()
            print("=== Final Results ===")
            for key, value in stats.items():
                print(f"   {key}: {value}")
        except Exception as e:
            print(f"   Could not get stats: {e}")
        
        manager.close_connection()
        print()
        print(" Import completed successfully!")
        
    except Exception as e:
        print(f" Import failed: {e}")
        print()
        print("Troubleshooting tips:")
        print("1. Make sure Neo4j is running (neo4j start)")
        print("2. Check .env file settings")
        print("3. Verify amazon-meta-test.txt exists")

if __name__ == "__main__":
    main()