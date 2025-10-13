# Data preprocessor - cleans up the parsed data
# Makes sure everything is ready for the database

class DataPreprocessor:
    def __init__(self):
        pass
        
    def clean_products(self, products):
        # Clean up product data
        print("Cleaning product data...")
        cleaned_products = []
        
        for product in products:
            # Make sure required fields exist
            if 'id' not in product or 'asin' not in product:
                continue
                
            # Clean the title
            if 'title' in product:
                product['title'] = product['title'].replace('\n', ' ').strip()
                if len(product['title']) > 200:  # Truncate long titles
                    product['title'] = product['title'][:200] + '...'
            
            # Fix rating if it's weird
            if 'avg_rating' in product:
                try:
                    rating = float(product['avg_rating'])
                    if rating < 0 or rating > 5:
                        product['avg_rating'] = None
                    else:
                        product['avg_rating'] = rating
                except:
                    product['avg_rating'] = None
            
            # Make sure salesrank is a number
            if 'salesrank' in product:
                try:
                    product['salesrank'] = int(product['salesrank'])
                except:
                    product['salesrank'] = None
            
            cleaned_products.append(product)
            
        print(f"Cleaned {len(cleaned_products)} products")
        return cleaned_products
    
    def clean_reviews(self, products):
        # Get all reviews from products and clean them
        print("Cleaning review data...")
        all_reviews = []
        
        for product in products:
            if 'reviews' in product:
                for review in product['reviews']:
                    # Make sure review has required fields
                    if 'customer_id' in review and 'rating' in review:
                        # Fix rating
                        try:
                            rating = int(review['rating'])
                            if rating >= 1 and rating <= 5:
                                review['rating'] = rating
                                review['product_id'] = product['id']
                                all_reviews.append(review)
                        except:
                            pass  # Skip bad reviews
        
        print(f"Cleaned {len(all_reviews)} reviews")
        return all_reviews
    
    def extract_categories(self, products):
        # Get all unique categories
        print("Extracting categories...")
        all_categories = set()
        
        for product in products:
            if 'categories' in product:
                for category_path in product['categories']:
                    for category in category_path:
                        if category.strip():
                            all_categories.add(category.strip())
        
        category_list = list(all_categories)
        print(f"Found {len(category_list)} unique categories")
        return category_list
    
    def create_relationships(self, products):
        # Create relationships for similar products
        print("Creating product relationships...")
        relationships = []
        
        for product in products:
            if 'similar' in product:
                for similar_asin in product['similar']:
                    relationship = {
                        'from_product': product['asin'],
                        'to_product': similar_asin,
                        'type': 'SIMILAR_TO'
                    }
                    relationships.append(relationship)
        
        print(f"Created {len(relationships)} relationships")
        return relationships
    
    def process_all_data(self, products):
        # Main function to clean everything
        print("Starting data preprocessing...")
        
        # Clean products first
        cleaned_products = self.clean_products(products)
        
        # Clean reviews
        cleaned_reviews = self.clean_reviews(cleaned_products)
        
        # Extract categories
        categories = self.extract_categories(cleaned_products)
        
        # Create relationships
        relationships = self.create_relationships(cleaned_products)
        
        result = {
            'products': cleaned_products,
            'reviews': cleaned_reviews,
            'categories': categories,
            'relationships': relationships
        }
        
        print("Data preprocessing complete!")
        return result

# Test it
if __name__ == "__main__":
    preprocessor = DataPreprocessor()
    print("Data preprocessor ready")