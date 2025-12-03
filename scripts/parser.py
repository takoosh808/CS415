def parse_amazon_data(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        current_product = {}
        current_reviews = []
        in_categories = False
        categories_remaining = 0
        current_categories = []
        
        for line in file:
            line_stripped = line.strip()
            
            if not line_stripped:
                if current_product:
                    current_product['reviews'] = current_reviews
                    yield current_product
                    current_product = {}
                    current_reviews = []
                    in_categories = False
                    categories_remaining = 0
                    current_categories = []
                continue
            
            if in_categories:
                if categories_remaining > 0:
                    if '|' in line_stripped:
                        cat_parts = line_stripped.split('|')[1:]
                        category_path = []
                        for cat in cat_parts:
                            if '[' in cat and ']' in cat:
                                cat_name = cat.split('[')[0].strip()
                                category_path.append(cat_name)
                        if category_path:
                            current_categories.append(category_path)
                    categories_remaining -= 1
                    if categories_remaining == 0:
                        in_categories = False
                        current_product['categories'] = current_categories
                        current_categories = []
                continue
            
            if line_stripped.startswith('Id:'):
                current_product['id'] = int(line_stripped.split(':')[1].strip())
                
            elif line_stripped.startswith('ASIN:'):
                current_product['asin'] = line_stripped.split(':')[1].strip()
                
            elif line_stripped.startswith('title:'):
                current_product['title'] = line_stripped.split(':', 1)[1].strip()
                
            elif line_stripped.startswith('group:'):
                current_product['group'] = line_stripped.split(':')[1].strip()
                
            elif line_stripped.startswith('salesrank:'):
                try:
                    current_product['salesrank'] = int(line_stripped.split(':')[1].strip())
                except:
                    current_product['salesrank'] = 0
                    
            elif line_stripped.startswith('similar:'):
                parts = line_stripped.split()
                if len(parts) > 2:
                    current_product['similar'] = parts[2:]
                else:
                    current_product['similar'] = []
                    
            elif line_stripped.startswith('categories:'):
                try:
                    count = int(line_stripped.split(':')[1].strip())
                    if count > 0:
                        in_categories = True
                        categories_remaining = count
                    else:
                        current_product['categories'] = []
                except:
                    current_product['categories'] = []
                
            elif line_stripped.startswith('reviews:'):
                parts = line_stripped.split()
                if len(parts) >= 8:
                    current_product['total_reviews'] = int(parts[2])
                    current_product['downloaded_reviews'] = int(parts[4])
                    current_product['avg_rating'] = float(parts[7])
                else:
                    current_product['total_reviews'] = 0
                    current_product['downloaded_reviews'] = 0
                    current_product['avg_rating'] = 0.0
                    
            elif 'cutomer:' in line_stripped and 'rating:' in line_stripped:
                try:
                    parts = line_stripped.split()
                    date = parts[0]
                    customer_idx = parts.index('cutomer:')
                    customer_id = parts[customer_idx + 1]
                    rating_idx = parts.index('rating:')
                    rating = int(parts[rating_idx + 1])
                    votes_idx = parts.index('votes:')
                    votes = int(parts[votes_idx + 1])
                    helpful_idx = parts.index('helpful:')
                    helpful = int(parts[helpful_idx + 1])
                    
                    current_reviews.append({
                        'date': date,
                        'customer': customer_id,
                        'rating': rating,
                        'votes': votes,
                        'helpful': helpful
                    })
                except:
                    pass
        
        # Yield last product if exists
        if current_product:
            current_product['reviews'] = current_reviews
            yield current_product


class AmazonDataParser:
    def __init__(self):
        self.products = []
        
    def parse_file(self, file_path):
        # Open file and read all products
        print(f"Reading file: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            current_product = {}
            current_reviews = []
            
            for line in file:
                line = line.strip()
                
                # Empty line means end of product
                if not line:
                    if current_product:
                        current_product['reviews'] = current_reviews
                        self.products.append(current_product)
                        current_product = {}
                        current_reviews = []
                    continue
                
                # Parse different fields
                if line.startswith('Id:'):
                    current_product['id'] = int(line.split(':')[1].strip())
                    
                elif line.startswith('ASIN:'):
                    current_product['asin'] = line.split(':')[1].strip()
                    
                elif line.strip().startswith('title:'):
                    current_product['title'] = line.split(':', 1)[1].strip()
                    
                elif line.strip().startswith('group:'):
                    current_product['group'] = line.split(':')[1].strip()
                    
                elif line.strip().startswith('salesrank:'):
                    try:
                        current_product['salesrank'] = int(line.split(':')[1].strip())
                    except:
                        current_product['salesrank'] = None
                        
                elif line.strip().startswith('similar:'):
                    # Parse similar products
                    parts = line.split()
                    if len(parts) > 2:
                        similar_list = []
                        for i in range(2, len(parts)):
                            similar_list.append(parts[i])
                        current_product['similar'] = similar_list
                        
                elif line.strip().startswith('categories:'):
                    # Parse categories - next line has count
                    current_product['categories'] = self.parse_categories(file)
                    
                elif line.strip().startswith('reviews:'):
                    # Parse review summary and individual reviews
                    parts = line.split()
                    if len(parts) >= 8:
                        current_product['total_reviews'] = int(parts[2])
                        current_product['downloaded_reviews'] = int(parts[4])
                        current_product['avg_rating'] = float(parts[7])
                    
                    # Read individual reviews
                    current_reviews = self.parse_reviews(file)
        
        # Add last product if exists
        if current_product:
            current_product['reviews'] = current_reviews
            self.products.append(current_product)
            
        print(f"Parsed {len(self.products)} products")
        return self.products
    
    def parse_categories(self, file):
        # Read category count
        count_line = file.readline().strip()
        try:
            count = int(count_line)
        except:
            return []
            
        categories = []
        for i in range(count):
            cat_line = file.readline().strip()
            if '|' in cat_line:
                # Extract category path
                cat_part = cat_line.split('|')[1:]
                category_path = []
                for cat in cat_part:
                    if '[' in cat and ']' in cat:
                        cat_name = cat.split('[')[0].strip()
                        category_path.append(cat_name)
                categories.append(category_path)
        
        return categories
    
    def parse_reviews(self, file):
        reviews = []
        
        # Read lines until we hit empty line or non-review
        while True:
            line = file.readline()
            
            if not line or line.strip() == '':
                break
                
            line = line.strip()
            
            # Check if this looks like a review (contains "cutomer:" and "rating:")
            if 'cutomer:' in line and 'rating:' in line:
                try:
                    # Parse the review format: date cutomer: ID rating: X votes: Y helpful: Z
                    parts = line.split()
                    date = parts[0]
                    
                    # Find customer ID (after "cutomer:")
                    cutomer_idx = parts.index('cutomer:')
                    customer_id = parts[cutomer_idx + 1]
                    
                    # Find rating (after "rating:")
                    rating_idx = parts.index('rating:')
                    rating = int(parts[rating_idx + 1])
                    
                    # Find votes (after "votes:")
                    votes_idx = parts.index('votes:')
                    votes = int(parts[votes_idx + 1])
                    
                    # Find helpful votes (after "helpful:")
                    helpful_idx = parts.index('helpful:')
                    helpful_votes = int(parts[helpful_idx + 1])
                    
                    review = {
                        'date': date,
                        'customer_id': customer_id,
                        'rating': rating,
                        'votes': votes,
                        'helpful_votes': helpful_votes
                    }
                    reviews.append(review)
                except:
                    # Not a valid review, stop parsing reviews
                    break
            else:
                # Not a review format, stop
                break
                
        return reviews
    
    def get_products(self):
        return self.products

# Test the parser
if __name__ == "__main__":
    parser = AmazonDataParser()
    products = parser.parse_file("amazon-meta-test.txt")
    print(f"Found {len(products)} products")