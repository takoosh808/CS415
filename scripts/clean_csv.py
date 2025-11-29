import csv
import os

def clean_csv_files(csv_dir):
    products_file = os.path.join(csv_dir, 'products.csv')
    products_clean = os.path.join(csv_dir, 'products_clean.csv')
    valid_asins = set()
    seen_asins = set()
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
    os.replace(products_clean, products_file)
    customers_file = os.path.join(csv_dir, 'customers.csv')
    customers_clean = os.path.join(csv_dir, 'customers_clean.csv')
    valid_customers = set()
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
    categories_file = os.path.join(csv_dir, 'categories.csv')
    categories_clean = os.path.join(csv_dir, 'categories_clean.csv')
    valid_categories = set()
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
    similar_file = os.path.join(csv_dir, 'similar.csv')
    similar_clean = os.path.join(csv_dir, 'similar_clean.csv')
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
    os.replace(similar_clean, similar_file)
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
            if asin in valid_asins and category in valid_categories:
                writer.writerow(row)
    os.replace(belongs_clean, belongs_file)
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
            if customer in valid_customers and asin in valid_asins:
                writer.writerow(row)
    os.replace(reviews_clean, reviews_file)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Clean CSV files to remove invalid relationships')
    parser.add_argument('--dir', default='csv_data', help='CSV directory to clean')
    args = parser.parse_args()
    clean_csv_files(args.dir)
