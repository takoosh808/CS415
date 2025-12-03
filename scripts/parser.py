"""
Amazon Product Metadata Parser
==============================

This module parses the Amazon product co-purchasing network metadata file
into Python data structures suitable for database import.

PURPOSE:
--------
The Amazon metadata file has a custom text format that's difficult to work
with directly. This parser:
1. Reads the raw text format
2. Extracts structured product data
3. Provides generator (memory-efficient) and class-based approaches

INPUT FILE FORMAT:
------------------
The Amazon metadata file looks like this:

    Id:   1
    ASIN: 0827229534
    title: Patterns of Preaching: A Sermon Sampler
    group: Book
    salesrank: 396585
    similar: 5  0804215715  156101074X  0687023955  ...
    categories: 2
       |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|...
       |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|...
    reviews: total: 2  downloaded: 2  avg rating: 5
       2000-7-28  cutomer: A2JW67OY8U6HHK  rating: 5  votes: 10  helpful: 9
       2003-12-14  cutomer: A2VE83MZF98ITY  rating: 5  votes: 6  helpful: 5

    Id:   2
    ASIN: 0738700797
    ...

FORMAT NOTES:
- Each product is separated by blank lines
- "cutomer" is intentionally misspelled in the original data
- Categories use pipe-delimited hierarchy: |Level1[id]|Level2[id]|...
- Similar products are space-separated ASINs

OUTPUT DATA STRUCTURE:
----------------------
Each product becomes a dictionary:
{
    'id': 1,                          # Internal product ID
    'asin': '0827229534',             # Amazon Standard ID Number
    'title': 'Patterns of Preaching', # Product title
    'group': 'Book',                  # Product category (Book, DVD, etc.)
    'salesrank': 396585,              # Amazon sales rank
    'similar': ['0804215715', ...],   # List of similar product ASINs
    'categories': [['Books', 'Subjects', 'Religion']],  # Category hierarchies
    'total_reviews': 2,               # Total number of reviews
    'downloaded_reviews': 2,          # Reviews in this file
    'avg_rating': 5.0,                # Average star rating
    'reviews': [                      # List of review details
        {
            'date': '2000-7-28',
            'customer': 'A2JW67OY8U6HHK',
            'rating': 5,
            'votes': 10,
            'helpful': 9
        },
        ...
    ]
}

TWO PARSING APPROACHES:
-----------------------
1. parse_amazon_data() - Generator function (RECOMMENDED)
   - Memory efficient: yields one product at a time
   - Good for large files (gigabytes)
   - Cannot random access products

2. AmazonDataParser class - Loads all into memory
   - Stores all products in a list
   - Easy random access
   - Uses more memory

USAGE:
------
Generator approach (memory efficient):
    for product in parse_amazon_data('amazon-meta.txt'):
        process(product)

Class approach (full memory load):
    parser = AmazonDataParser()
    products = parser.parse_file('amazon-meta.txt')
    print(len(products))
"""


def parse_amazon_data(file_path):
    """
    Generator function that yields one product at a time.
    
    This is the RECOMMENDED approach for large files because it:
    - Uses constant memory regardless of file size
    - Starts returning products immediately
    - Integrates well with streaming pipelines
    
    Parameters:
    -----------
    file_path : str
        Path to Amazon metadata file
        
    Yields:
    -------
    dict
        Product dictionary with all extracted fields
        
    Example:
    --------
        for product in parse_amazon_data('amazon-meta.txt'):
            print(product['title'])
    """
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        # State for current product being parsed
        current_product = {}
        current_reviews = []
        
        # State for multi-line category parsing
        in_categories = False      # Are we reading category lines?
        categories_remaining = 0   # How many category lines left?
        current_categories = []    # Collected category paths
        
        for line in file:
            line_stripped = line.strip()
            
            # =================================================================
            # PRODUCT BOUNDARY DETECTION
            # =================================================================
            # Empty line = end of current product, start of new product
            if not line_stripped:
                if current_product:
                    # Attach reviews and yield completed product
                    current_product['reviews'] = current_reviews
                    yield current_product
                    
                    # Reset state for next product
                    current_product = {}
                    current_reviews = []
                    in_categories = False
                    categories_remaining = 0
                    current_categories = []
                continue
            
            # =================================================================
            # CATEGORY LINE PROCESSING (Special multi-line handling)
            # =================================================================
            # Categories span multiple lines after "categories:" header
            if in_categories:
                if categories_remaining > 0:
                    # Parse category hierarchy: |Books[283155]|Subjects[1000]|...
                    if '|' in line_stripped:
                        # Split by pipe, skip empty first element
                        cat_parts = line_stripped.split('|')[1:]
                        category_path = []
                        
                        for cat in cat_parts:
                            # Extract name from "CategoryName[id]" format
                            if '[' in cat and ']' in cat:
                                cat_name = cat.split('[')[0].strip()
                                category_path.append(cat_name)
                        
                        if category_path:
                            current_categories.append(category_path)
                    
                    categories_remaining -= 1
                    
                    # Done with all category lines?
                    if categories_remaining == 0:
                        in_categories = False
                        current_product['categories'] = current_categories
                        current_categories = []
                continue
            
            # =================================================================
            # FIELD PARSING
            # =================================================================
            
            # Internal product ID
            if line_stripped.startswith('Id:'):
                current_product['id'] = int(line_stripped.split(':')[1].strip())
            
            # Amazon Standard Identification Number (unique product ID)
            elif line_stripped.startswith('ASIN:'):
                current_product['asin'] = line_stripped.split(':')[1].strip()
            
            # Product title (may contain colons, so split at first colon only)
            elif line_stripped.startswith('title:'):
                current_product['title'] = line_stripped.split(':', 1)[1].strip()
            
            # Product group (Book, DVD, Music, etc.)
            elif line_stripped.startswith('group:'):
                current_product['group'] = line_stripped.split(':')[1].strip()
            
            # Amazon sales rank (lower = more popular)
            elif line_stripped.startswith('salesrank:'):
                try:
                    current_product['salesrank'] = int(line_stripped.split(':')[1].strip())
                except:
                    current_product['salesrank'] = 0  # Default for invalid values
            
            # Similar products: "similar: 5  ASIN1  ASIN2  ASIN3  ASIN4  ASIN5"
            # First number is count, rest are ASINs
            elif line_stripped.startswith('similar:'):
                parts = line_stripped.split()
                if len(parts) > 2:
                    # Skip "similar:" and count, take remaining ASINs
                    current_product['similar'] = parts[2:]
                else:
                    current_product['similar'] = []
            
            # Categories header: "categories: 2" (number = lines to follow)
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
            
            # Review summary: "reviews: total: 2  downloaded: 2  avg rating: 5"
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
            
            # Individual review line
            # NOTE: "cutomer" is misspelled in the original Amazon data!
            elif 'cutomer:' in line_stripped and 'rating:' in line_stripped:
                try:
                    # Parse: "2000-7-28  cutomer: A2JW67  rating: 5  votes: 10  helpful: 9"
                    parts = line_stripped.split()
                    
                    date = parts[0]
                    
                    # Find each field by keyword index
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
                    pass  # Skip malformed review lines
        
        # Don't forget the last product (no trailing blank line)
        if current_product:
            current_product['reviews'] = current_reviews
            yield current_product


class AmazonDataParser:
    """
    Class-based parser that loads all products into memory.
    
    Use this when you need:
    - Random access to products
    - Multiple passes over the data
    - Simple product count
    
    NOT recommended for very large files (will use lots of RAM).
    
    Attributes:
    -----------
    products : list
        List of all parsed product dictionaries
        
    Example:
    --------
        parser = AmazonDataParser()
        products = parser.parse_file('amazon-meta.txt')
        print(f"Found {len(products)} products")
        print(products[0]['title'])
    """
    
    def __init__(self):
        """Initialize empty product list."""
        self.products = []
        
    def parse_file(self, file_path):
        """
        Parse entire file and store all products in memory.
        
        Parameters:
        -----------
        file_path : str
            Path to Amazon metadata file
            
        Returns:
        --------
        list
            List of all product dictionaries
        """
        print(f"Reading file: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            current_product = {}
            current_reviews = []
            
            for line in file:
                line = line.strip()
                
                # Empty line = end of product
                if not line:
                    if current_product:
                        current_product['reviews'] = current_reviews
                        self.products.append(current_product)
                        current_product = {}
                        current_reviews = []
                    continue
                
                # Parse different fields (same logic as generator function)
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
                    parts = line.split()
                    if len(parts) > 2:
                        similar_list = []
                        for i in range(2, len(parts)):
                            similar_list.append(parts[i])
                        current_product['similar'] = similar_list
                        
                elif line.strip().startswith('categories:'):
                    # Delegate to category parsing helper
                    current_product['categories'] = self.parse_categories(file)
                    
                elif line.strip().startswith('reviews:'):
                    # Parse review summary line
                    parts = line.split()
                    if len(parts) >= 8:
                        current_product['total_reviews'] = int(parts[2])
                        current_product['downloaded_reviews'] = int(parts[4])
                        current_product['avg_rating'] = float(parts[7])
                    
                    # Delegate to review parsing helper
                    current_reviews = self.parse_reviews(file)
        
        # Don't forget last product
        if current_product:
            current_product['reviews'] = current_reviews
            self.products.append(current_product)
            
        print(f"Parsed {len(self.products)} products")
        return self.products
    
    def parse_categories(self, file):
        """
        Parse category lines from current file position.
        
        The categories section looks like:
            categories: 2
               |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|...
               |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|...
        
        Parameters:
        -----------
        file : file object
            Open file positioned after "categories:" line
            
        Returns:
        --------
        list
            List of category path lists
        """
        # First line after header is the count
        count_line = file.readline().strip()
        try:
            count = int(count_line)
        except:
            return []
            
        categories = []
        
        # Read 'count' category lines
        for i in range(count):
            cat_line = file.readline().strip()
            
            if '|' in cat_line:
                # Extract category path from pipe-delimited format
                # "|Books[283155]|Subjects[1000]|Religion..." becomes ["Books", "Subjects", "Religion"]
                cat_part = cat_line.split('|')[1:]  # Skip empty first element
                category_path = []
                
                for cat in cat_part:
                    # Extract name from "CategoryName[id]" format
                    if '[' in cat and ']' in cat:
                        cat_name = cat.split('[')[0].strip()
                        category_path.append(cat_name)
                
                categories.append(category_path)
        
        return categories
    
    def parse_reviews(self, file):
        """
        Parse individual review lines from current file position.
        
        Reviews look like:
           2000-7-28  cutomer: A2JW67  rating: 5  votes: 10  helpful: 9
        
        NOTE: "cutomer" is misspelled in the original Amazon data!
        
        Parameters:
        -----------
        file : file object
            Open file positioned after "reviews:" summary line
            
        Returns:
        --------
        list
            List of review dictionaries
        """
        reviews = []
        
        # Read lines until we hit empty line or non-review
        while True:
            line = file.readline()
            
            # End of file or empty line = done with reviews
            if not line or line.strip() == '':
                break
                
            line = line.strip()
            
            # Check if this looks like a review line
            # Must contain both "cutomer:" and "rating:"
            if 'cutomer:' in line and 'rating:' in line:
                try:
                    # Parse format: "date cutomer: ID rating: X votes: Y helpful: Z"
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
                    # Malformed review line, stop parsing reviews
                    break
            else:
                # Not a review format, we've moved past reviews section
                break
                
        return reviews
    
    def get_products(self):
        """
        Get list of all parsed products.
        
        Returns:
        --------
        list
            List of product dictionaries
        """
        return self.products


# =============================================================================
# STANDALONE TESTING
# =============================================================================
if __name__ == "__main__":
    # Test with a small sample file
    parser = AmazonDataParser()
    products = parser.parse_file("amazon-meta-test.txt")
    print(f"Found {len(products)} products")