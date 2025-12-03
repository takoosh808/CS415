"""
Amazon Data to CSV Converter
============================

This script converts the raw Amazon metadata file into Neo4j-compatible
CSV files for bulk import.

PURPOSE:
--------
Neo4j's fastest bulk import method is via CSV files. This script:
1. Parses the Amazon metadata text format
2. Extracts entities (Products, Customers, Categories)
3. Extracts relationships (Reviews, Similar, Belongs_To)
4. Writes Neo4j bulk import format CSVs

INPUT FORMAT:
-------------
The Amazon metadata file has a custom text format:

    Id:   1
    ASIN: 0827229534
    title: Patterns of Preaching: A Sermon Sampler
    group: Book
    salesrank: 396585
    similar: 5  0804215715  156101074X  ...
    categories: 2
       |Books[283155]|Subjects[1000]|Religion...
       |Books[283155]|Subjects[1000]|Religion...
    reviews: total: 2  downloaded: 2  avg rating: 5
       2000-7-28  customer: A2JW67OY8U6HHK  rating: 5  votes: 10  helpful: 9
       ...

OUTPUT FORMAT:
--------------
Neo4j bulk import CSV format with typed column headers:

NODE FILES:
- products.csv: asin:ID(Product), title, group, salesrank:int, ...
- customers.csv: id:ID(Customer)
- categories.csv: name:ID(Category)

RELATIONSHIP FILES:
- reviews.csv: :START_ID(Customer), :END_ID(Product), rating:int, ...
- similar.csv: :START_ID(Product), :END_ID(Product)
- belongs_to.csv: :START_ID(Product), :END_ID(Category)

COLUMN NAMING CONVENTIONS:
--------------------------
Neo4j bulk import uses special suffixes:
- :ID(Label): Unique identifier for node type
- :START_ID(Label): Source node ID for relationship
- :END_ID(Label): Target node ID for relationship
- :int, :float: Type hints for Neo4j

WORKFLOW:
---------
1. Create output directory
2. Open all CSV files simultaneously
3. Stream through input file (memory efficient)
4. Write each entity/relationship to appropriate CSV
5. Write collected customers and categories at end

DEPENDENCIES:
- csv: Standard library CSV handling
- parser: Custom module for Amazon text format (parser.py)

USAGE:
------
    python convert_to_csv.py --file amazon-meta.txt --output csv_data
"""

import csv
import sys
import os
from parser import parse_amazon_data  # Import our custom parser


def convert_to_csv(input_file, output_dir):
    """
    Convert Amazon metadata to Neo4j bulk import CSV files.
    
    This function orchestrates the entire conversion process:
    1. Parse raw Amazon metadata
    2. Write node files (Products, Customers, Categories)
    3. Write relationship files (Reviews, Similar, Belongs_To)
    
    Parameters:
    -----------
    input_file : str
        Path to Amazon metadata file (amazon-meta.txt)
    output_dir : str
        Directory to write CSV output files
        
    Side Effects:
    -------------
    Creates output_dir if it doesn't exist.
    Creates 6 CSV files in output_dir.
    """
    
    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    
    # Define output file paths
    products_file = os.path.join(output_dir, 'products.csv')
    customers_file = os.path.join(output_dir, 'customers.csv')
    categories_file = os.path.join(output_dir, 'categories.csv')
    reviews_file = os.path.join(output_dir, 'reviews.csv')
    similar_file = os.path.join(output_dir, 'similar.csv')
    belongs_to_file = os.path.join(output_dir, 'belongs_to.csv')
    
    # Track unique customers and categories (collected during product processing)
    # These are written at the end since we don't know all values upfront
    customers = set()
    categories = set()
    
    # Open all files simultaneously for efficient streaming writes
    with open(products_file, 'w', newline='', encoding='utf-8') as pf, \
         open(reviews_file, 'w', newline='', encoding='utf-8') as rf, \
         open(similar_file, 'w', newline='', encoding='utf-8') as sf, \
         open(belongs_to_file, 'w', newline='', encoding='utf-8') as bf:
        
        # Create CSV writers
        products_writer = csv.writer(pf)
        reviews_writer = csv.writer(rf)
        similar_writer = csv.writer(sf)
        belongs_writer = csv.writer(bf)
        
        # Write header rows with Neo4j type annotations
        # :ID(Label) marks the unique identifier for that node type
        products_writer.writerow(['asin:ID(Product)', 'title', 'group', 'salesrank:int', 'avg_rating:float', 'total_reviews:int'])
        reviews_writer.writerow([':START_ID(Customer)', ':END_ID(Product)', 'rating:int', 'date', 'votes:int', 'helpful:int'])
        similar_writer.writerow([':START_ID(Product)', ':END_ID(Product)'])
        belongs_writer.writerow([':START_ID(Product)', ':END_ID(Category)'])
        
        # Parse Amazon data using generator (memory efficient for large files)
        products = parse_amazon_data(input_file)
        count = 0
        
        # Process each product from the parser
        for product in products:
            # Escape quotes in title for CSV compatibility
            title = product.get('title', '')
            
            # Write product node
            products_writer.writerow([
                product.get('asin', ''),
                title.replace('"', '""') if title else '',  # Escape double quotes
                product.get('group', ''),
                product.get('salesrank', ''),
                product.get('avg_rating', ''),
                product.get('total_reviews', '')
            ])
            
            # Write SIMILAR relationships (Product -> Product)
            # Each similar ASIN creates a new relationship
            for similar in product.get('similar', []):
                similar_writer.writerow([product.get('asin', ''), similar])
            
            # Write BELONGS_TO relationships (Product -> Category)
            # Categories can be nested lists (multiple category hierarchies)
            for category_list in product.get('categories', []):
                if isinstance(category_list, list):
                    # Handle list of categories
                    for category in category_list:
                        categories.add(category)  # Track for later
                        belongs_writer.writerow([product.get('asin', ''), category])
                else:
                    # Handle single category
                    categories.add(category_list)
                    belongs_writer.writerow([product.get('asin', ''), category_list])
            
            # Write REVIEWED relationships (Customer -> Product)
            # Each review creates a relationship with properties
            for review in product.get('reviews', []):
                customer_id = review['customer']
                customers.add(customer_id)  # Track for later
                
                reviews_writer.writerow([
                    customer_id,              # Source: Customer
                    product.get('asin', ''),  # Target: Product
                    review['rating'],         # Rating (1-5)
                    review['date'],           # Review date
                    review['votes'],          # Number of votes
                    review['helpful']         # Helpful votes
                ])
            
            count += 1
    
    # Write Customer nodes (collected during product processing)
    # These are unique customer IDs from all reviews
    with open(customers_file, 'w', newline='', encoding='utf-8') as cf:
        customers_writer = csv.writer(cf)
        customers_writer.writerow(['id:ID(Customer)'])  # Header
        
        # Write sorted for consistency
        for customer in sorted(customers):
            customers_writer.writerow([customer])
    
    # Write Category nodes (collected during product processing)
    # These are unique category names from all products
    with open(categories_file, 'w', newline='', encoding='utf-8') as cf:
        categories_writer = csv.writer(cf)
        categories_writer.writerow(['name:ID(Category)'])  # Header
        
        # Write sorted, escaping quotes
        for category in sorted(categories):
            categories_writer.writerow([category.replace('"', '""')])


if __name__ == "__main__":
    import argparse
    
    # Command-line interface
    parser = argparse.ArgumentParser(description='Convert Amazon data to CSV for bulk import')
    parser.add_argument('--file', required=True, help='Input file (amazon-meta.txt or amazon-meta-10mb.txt)')
    parser.add_argument('--output', default='csv_data', help='Output directory for CSV files')
    args = parser.parse_args()
    
    convert_to_csv(args.file, args.output)
