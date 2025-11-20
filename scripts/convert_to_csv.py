import csv
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data_processing.parser import parse_amazon_data

def convert_to_csv(input_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    products_file = os.path.join(output_dir, 'products.csv')
    customers_file = os.path.join(output_dir, 'customers.csv')
    categories_file = os.path.join(output_dir, 'categories.csv')
    reviews_file = os.path.join(output_dir, 'reviews.csv')
    similar_file = os.path.join(output_dir, 'similar.csv')
    belongs_to_file = os.path.join(output_dir, 'belongs_to.csv')
    
    print(f"Converting {input_file} to CSV files...")
    print(f"Output directory: {output_dir}")
    
    customers = set()
    categories = set()
    
    with open(products_file, 'w', newline='', encoding='utf-8') as pf, \
         open(reviews_file, 'w', newline='', encoding='utf-8') as rf, \
         open(similar_file, 'w', newline='', encoding='utf-8') as sf, \
         open(belongs_to_file, 'w', newline='', encoding='utf-8') as bf:
        
        products_writer = csv.writer(pf)
        reviews_writer = csv.writer(rf)
        similar_writer = csv.writer(sf)
        belongs_writer = csv.writer(bf)
        
        products_writer.writerow(['asin:ID(Product)', 'title', 'group', 'salesrank:int', 'avg_rating:float', 'total_reviews:int'])
        reviews_writer.writerow([':START_ID(Customer)', ':END_ID(Product)', 'rating:int', 'date', 'votes:int', 'helpful:int'])
        similar_writer.writerow([':START_ID(Product)', ':END_ID(Product)'])
        belongs_writer.writerow([':START_ID(Product)', ':END_ID(Category)'])
        
        products = parse_amazon_data(input_file)
        count = 0
        
        for product in products:
            title = product.get('title', '')
            products_writer.writerow([
                product.get('asin', ''),
                title.replace('"', '""') if title else '',
                product.get('group', ''),
                product.get('salesrank', ''),
                product.get('avg_rating', ''),
                product.get('total_reviews', '')
            ])
            
            for similar in product.get('similar', []):
                similar_writer.writerow([product.get('asin', ''), similar])
            
            for category_list in product.get('categories', []):
                if isinstance(category_list, list):
                    for category in category_list:
                        categories.add(category)
                        belongs_writer.writerow([product.get('asin', ''), category])
                else:
                    categories.add(category_list)
                    belongs_writer.writerow([product.get('asin', ''), category_list])
            
            for review in product.get('reviews', []):
                customer_id = review['customer']
                customers.add(customer_id)
                reviews_writer.writerow([
                    customer_id,
                    product.get('asin', ''),
                    review['rating'],
                    review['date'],
                    review['votes'],
                    review['helpful']
                ])
            
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count:,} products...")
    
    with open(customers_file, 'w', newline='', encoding='utf-8') as cf:
        customers_writer = csv.writer(cf)
        customers_writer.writerow(['id:ID(Customer)'])
        for customer in sorted(customers):
            customers_writer.writerow([customer])
    
    with open(categories_file, 'w', newline='', encoding='utf-8') as cf:
        categories_writer = csv.writer(cf)
        categories_writer.writerow(['name:ID(Category)'])
        for category in sorted(categories):
            categories_writer.writerow([category.replace('"', '""')])
    
    print(f"\nConversion complete!")
    print(f"Products: {count:,}")
    print(f"Customers: {len(customers):,}")
    print(f"Categories: {len(categories):,}")
    print(f"\nCSV files created in: {output_dir}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Convert Amazon data to CSV for bulk import')
    parser.add_argument('--file', required=True, help='Input file (amazon-meta.txt or amazon-meta-10mb.txt)')
    parser.add_argument('--output', default='csv_data', help='Output directory for CSV files')
    args = parser.parse_args()
    
    convert_to_csv(args.file, args.output)
