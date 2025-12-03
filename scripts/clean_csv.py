"""
CSV Data Cleaning Utility
=========================

This script cleans and validates CSV files before loading into Neo4j.
It ensures data integrity by removing duplicates and invalid references.

PURPOSE:
--------
When loading data into a graph database, relationships (edges) must
reference existing nodes. This script:

1. DEDUPLICATES nodes (Products, Customers, Categories)
2. VALIDATES relationships (ensures both endpoints exist)
3. REMOVES orphaned references

WHY THIS MATTERS:
-----------------
Without cleaning, you'll encounter these problems:
- Duplicate nodes: Same product appears twice with different data
- Dangling relationships: Edge points to non-existent node
- Import failures: Neo4j MATCH fails for invalid references

DATA FLOW:
----------
1. Clean node files first (products, customers, categories)
2. Build sets of valid IDs
3. Filter relationship files to only include valid references

FILES PROCESSED:
----------------
NODE FILES (deduplicated):
- products.csv: Remove duplicate ASINs
- customers.csv: Remove duplicate customer IDs
- categories.csv: Remove duplicate category names

RELATIONSHIP FILES (validated):
- similar.csv: Keep only if BOTH products exist
- belongs_to.csv: Keep only if product AND category exist
- reviews.csv: Keep only if customer AND product exist

ALGORITHM:
----------
For node files:
1. Read each row
2. Check if ID already seen
3. If not seen: write row, mark ID as seen
4. If seen: skip (duplicate)

For relationship files:
1. Read each row
2. Check if START_ID exists in valid node set
3. Check if END_ID exists in valid node set
4. If both exist: write row
5. If either missing: skip (invalid reference)

DEPENDENCIES:
- csv: Standard library for CSV handling
- os: File operations

USAGE:
------
    python clean_csv.py --dir csv_data
    
    # Where csv_data contains the raw CSV files
"""

import csv
import os


def clean_csv_files(csv_dir):
    """
    Clean all CSV files in the specified directory.
    
    This function processes files in a specific order:
    1. Node files first (to build valid ID sets)
    2. Relationship files second (using those ID sets)
    
    Parameters:
    -----------
    csv_dir : str
        Path to directory containing CSV files
        
    Side Effects:
    -------------
    Overwrites original CSV files with cleaned versions.
    Creates temporary *_clean.csv files during processing.
    """
    
    # ========================================================================
    # STAGE 1: CLEAN PRODUCTS (Node File)
    # ========================================================================
    # Remove duplicate ASINs, keeping first occurrence
    
    products_file = os.path.join(csv_dir, 'products.csv')
    products_clean = os.path.join(csv_dir, 'products_clean.csv')
    
    # Set to track valid ASINs for relationship validation later
    valid_asins = set()
    # Set to detect duplicates during processing
    seen_asins = set()
    
    with open(products_file, 'r', encoding='utf-8') as fin, \
         open(products_clean, 'w', newline='', encoding='utf-8') as fout:
        
        # DictReader: access columns by name (e.g., row['asin:ID(Product)'])
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()  # Write column headers
        
        for row in reader:
            asin = row['asin:ID(Product)']
            
            # Only keep non-empty, non-duplicate ASINs
            if asin and asin not in seen_asins:
                writer.writerow(row)
                seen_asins.add(asin)
                valid_asins.add(asin)  # Track for relationship validation
    
    # Replace original with cleaned version
    os.replace(products_clean, products_file)
    
    
    # ========================================================================
    # STAGE 2: CLEAN CUSTOMERS (Node File)
    # ========================================================================
    # Remove duplicate customer IDs
    
    customers_file = os.path.join(csv_dir, 'customers.csv')
    customers_clean = os.path.join(csv_dir, 'customers_clean.csv')
    
    valid_customers = set()  # For review validation
    seen_customers = set()
    
    with open(customers_file, 'r', encoding='utf-8') as fin, \
         open(customers_clean, 'w', newline='', encoding='utf-8') as fout:
        
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            customer_id = row['id:ID(Customer)']
            
            if customer_id and customer_id not in seen_customers:
                writer.writerow(row)
                seen_customers.add(customer_id)
                valid_customers.add(customer_id)
    
    os.replace(customers_clean, customers_file)
    
    
    # ========================================================================
    # STAGE 3: CLEAN CATEGORIES (Node File)
    # ========================================================================
    # Remove duplicate category names
    
    categories_file = os.path.join(csv_dir, 'categories.csv')
    categories_clean = os.path.join(csv_dir, 'categories_clean.csv')
    
    valid_categories = set()  # For belongs_to validation
    seen_categories = set()
    
    with open(categories_file, 'r', encoding='utf-8') as fin, \
         open(categories_clean, 'w', newline='', encoding='utf-8') as fout:
        
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            category = row['name:ID(Category)']
            
            if category and category not in seen_categories:
                writer.writerow(row)
                seen_categories.add(category)
                valid_categories.add(category)
    
    os.replace(categories_clean, categories_file)
    
    
    # ========================================================================
    # STAGE 4: CLEAN SIMILAR RELATIONSHIPS
    # ========================================================================
    # Keep only if BOTH products exist in valid_asins
    
    similar_file = os.path.join(csv_dir, 'similar.csv')
    similar_clean = os.path.join(csv_dir, 'similar_clean.csv')
    
    with open(similar_file, 'r', encoding='utf-8') as fin, \
         open(similar_clean, 'w', newline='', encoding='utf-8') as fout:
        
        # Use basic csv.reader for relationship files
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        
        # Copy header row
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            start_asin, end_asin = row[0], row[1]
            
            # Both products must exist
            if start_asin in valid_asins and end_asin in valid_asins:
                writer.writerow(row)
    
    os.replace(similar_clean, similar_file)
    
    
    # ========================================================================
    # STAGE 5: CLEAN BELONGS_TO RELATIONSHIPS
    # ========================================================================
    # Keep only if product AND category exist
    
    belongs_file = os.path.join(csv_dir, 'belongs_to.csv')
    belongs_clean = os.path.join(csv_dir, 'belongs_to_clean.csv')
    
    with open(belongs_file, 'r', encoding='utf-8') as fin, \
         open(belongs_clean, 'w', newline='', encoding='utf-8') as fout:
        
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            asin, category = row[0], row[1]
            
            # Product must exist AND category must exist
            if asin in valid_asins and category in valid_categories:
                writer.writerow(row)
    
    os.replace(belongs_clean, belongs_file)
    
    
    # ========================================================================
    # STAGE 6: CLEAN REVIEWS RELATIONSHIPS
    # ========================================================================
    # Keep only if customer AND product exist
    
    reviews_file = os.path.join(csv_dir, 'reviews.csv')
    reviews_clean = os.path.join(csv_dir, 'reviews_clean.csv')
    
    with open(reviews_file, 'r', encoding='utf-8') as fin, \
         open(reviews_clean, 'w', newline='', encoding='utf-8') as fout:
        
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            customer, asin = row[0], row[1]
            
            # Customer must exist AND product must exist
            if customer in valid_customers and asin in valid_asins:
                writer.writerow(row)
    
    os.replace(reviews_clean, reviews_file)


if __name__ == "__main__":
    import argparse
    
    # Command-line interface
    parser = argparse.ArgumentParser(description='Clean CSV files to remove invalid relationships')
    parser.add_argument('--dir', default='csv_data', help='CSV directory to clean')
    args = parser.parse_args()
    
    clean_csv_files(args.dir)
