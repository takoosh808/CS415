import csv
import os

def clean_csv_files(csv_dir):
    print("Cleaning CSV files...")
    
    # Step 1: Remove duplicate products and read all valid ASINs
    products_file = os.path.join(csv_dir, 'products.csv')
    products_clean = os.path.join(csv_dir, 'products_clean.csv')
    valid_asins = set()
    seen_asins = set()
    duplicates_removed = 0
    
    print("Cleaning products.csv (removing duplicates)...")
    with open(products_file, 'r', encoding='utf-8') as fin, \
         open(products_clean, 'w', newline='', encoding='utf-8') as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            asin = row['asin:ID(Product)']
            if asin and asin not in seen_asins:
                writer.writerow(row)
                seen_asins.add(asin)
                valid_asins.add(asin)
            elif asin in seen_asins:
                duplicates_removed += 1
    
    os.replace(products_clean, products_file)
    print(f"Found {len(valid_asins):,} valid product ASINs")
    print(f"Removed {duplicates_removed:,} duplicate products")
    
    # Step 2: Remove duplicate customers and read all valid customer IDs
    customers_file = os.path.join(csv_dir, 'customers.csv')
    customers_clean = os.path.join(csv_dir, 'customers_clean.csv')
    valid_customers = set()
    seen_customers = set()
    duplicates_removed = 0
    
    print("\nCleaning customers.csv (removing duplicates)...")
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
            elif customer_id in seen_customers:
                duplicates_removed += 1
    
    os.replace(customers_clean, customers_file)
    print(f"Found {len(valid_customers):,} valid customer IDs")
    print(f"Removed {duplicates_removed:,} duplicate customers")
    
    # Step 3: Remove duplicate categories and read all valid categories
    categories_file = os.path.join(csv_dir, 'categories.csv')
    categories_clean = os.path.join(csv_dir, 'categories_clean.csv')
    valid_categories = set()
    seen_categories = set()
    duplicates_removed = 0
    
    print("\nCleaning categories.csv (removing duplicates)...")
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
            elif category in seen_categories:
                duplicates_removed += 1
    
    os.replace(categories_clean, categories_file)
    print(f"Found {len(valid_categories):,} valid categories")
    print(f"Removed {duplicates_removed:,} duplicate categories")
    
    # Step 4: Clean similar.csv (only keep relationships where both products exist)
    similar_file = os.path.join(csv_dir, 'similar.csv')
    similar_clean = os.path.join(csv_dir, 'similar_clean.csv')
    
    print("\nCleaning similar.csv...")
    kept = 0
    removed = 0
    
    with open(similar_file, 'r', encoding='utf-8') as fin, \
         open(similar_clean, 'w', newline='', encoding='utf-8') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            start_asin, end_asin = row[0], row[1]
            if start_asin in valid_asins and end_asin in valid_asins:
                writer.writerow(row)
                kept += 1
            else:
                removed += 1
    
    print(f"  Kept: {kept:,} relationships")
    print(f"  Removed: {removed:,} bad relationships")
    os.replace(similar_clean, similar_file)
    
    # Step 5: Clean belongs_to.csv (only keep relationships where product and category exist)
    belongs_file = os.path.join(csv_dir, 'belongs_to.csv')
    belongs_clean = os.path.join(csv_dir, 'belongs_to_clean.csv')
    
    print("\nCleaning belongs_to.csv...")
    kept = 0
    removed = 0
    
    with open(belongs_file, 'r', encoding='utf-8') as fin, \
         open(belongs_clean, 'w', newline='', encoding='utf-8') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            asin, category = row[0], row[1]
            if asin in valid_asins and category in valid_categories:
                writer.writerow(row)
                kept += 1
            else:
                removed += 1
    
    print(f"  Kept: {kept:,} relationships")
    print(f"  Removed: {removed:,} bad relationships")
    os.replace(belongs_clean, belongs_file)
    
    # Step 6: Clean reviews.csv (only keep relationships where customer and product exist)
    reviews_file = os.path.join(csv_dir, 'reviews.csv')
    reviews_clean = os.path.join(csv_dir, 'reviews_clean.csv')
    
    print("\nCleaning reviews.csv...")
    kept = 0
    removed = 0
    
    with open(reviews_file, 'r', encoding='utf-8') as fin, \
         open(reviews_clean, 'w', newline='', encoding='utf-8') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            customer, asin = row[0], row[1]
            if customer in valid_customers and asin in valid_asins:
                writer.writerow(row)
                kept += 1
            else:
                removed += 1
    
    print(f"  Kept: {kept:,} relationships")
    print(f"  Removed: {removed:,} bad relationships")
    os.replace(reviews_clean, reviews_file)
    
    print("\n✓ CSV files cleaned successfully!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Clean CSV files to remove invalid relationships')
    parser.add_argument('--dir', default='csv_data', help='CSV directory to clean')
    args = parser.parse_args()
    
    clean_csv_files(args.dir)
