"""
Amazon Metadata Data Preparation Pipeline for Neo4j
====================================================

This module handles data reduction, cleansing, and transformation
of Amazon product metadata for ingestion into Neo4j graph database.

Data Structure Analysis:
- Original file: ~977MB, 14.4M lines, 548,552 products
- Each product has: ID, ASIN, title, group, salesrank, similar products, categories, reviews
- Target: Reduce to 10-500MB for milestone processing
"""

import re
import json
import logging
from typing import Dict, List, Optional, Generator
from dataclasses import dataclass
from pathlib import Path
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Product:
    """Data class representing a cleaned Amazon product"""
    id: int
    asin: str
    title: Optional[str] = None
    group: Optional[str] = None
    salesrank: Optional[int] = None
    similar_products: List[str] = None
    categories: List[str] = None
    reviews: List[Dict] = None
    avg_rating: Optional[float] = None
    total_reviews: Optional[int] = None

    def __post_init__(self):
        if self.similar_products is None:
            self.similar_products = []
        if self.categories is None:
            self.categories = []
        if self.reviews is None:
            self.reviews = []

class AmazonDataProcessor:
    """Main class for processing Amazon metadata"""
    
    def __init__(self, input_file: str, output_file: str, max_products: int = 50000):
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.max_products = max_products
        self.processed_count = 0
        self.valid_products = 0
        
    def data_reduction(self) -> Generator[str, None, None]:
        """
        Data Reduction Pseudo-code:
        1. Read input file line by line
        2. Skip discontinued products (no title/group)
        3. Filter products by group (Book, Music, Video, etc.)
        4. Limit to max_products for size control
        5. Yield valid product blocks
        """
        logger.info(f"Starting data reduction, target: {self.max_products} products")
        
        with open(self.input_file, 'r', encoding='utf-8', errors='ignore') as file:
            current_product = []
            in_product = False
            
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                
                # Skip header lines
                if line_num <= 3:
                    continue
                
                # Start of new product
                if line.startswith('Id:'):
                    if current_product and self._is_valid_product_block(current_product):
                        if self.valid_products < self.max_products:
                            yield '\n'.join(current_product)
                            self.valid_products += 1
                        else:
                            break
                    current_product = [line]
                    in_product = True
                    self.processed_count += 1
                    
                    if self.processed_count % 10000 == 0:
                        logger.info(f"Processed {self.processed_count} products, valid: {self.valid_products}")
                
                elif in_product:
                    current_product.append(line)
            
            # Process last product
            if current_product and self._is_valid_product_block(current_product) and self.valid_products < self.max_products:
                yield '\n'.join(current_product)
                self.valid_products += 1
                
        logger.info(f"Data reduction complete: {self.valid_products} valid products from {self.processed_count} total")
    
    def _is_valid_product_block(self, lines: List[str]) -> bool:
        """
        Data Cleansing Validation:
        1. Must have title (not discontinued)
        2. Must have group/category
        3. Should have meaningful content
        """
        text = '\n'.join(lines)
        
        # Skip discontinued products
        if 'discontinued product' in text:
            return False
            
        # Must have title
        if 'title:' not in text:
            return False
            
        # Must have group
        if 'group:' not in text:
            return False
            
        # Skip products with very short titles (likely data quality issues)
        title_match = re.search(r'title:\s*(.+)', text)
        if title_match and len(title_match.group(1).strip()) < 5:
            return False
            
        return True
    
    def parse_product(self, product_text: str) -> Optional[Product]:
        """
        Data Transformation Parser Algorithm:
        
        INPUT: Raw product text block
        OUTPUT: Structured Product object
        
        ALGORITHM:
        1. Extract basic fields (ID, ASIN, title, group, salesrank)
        2. Parse similar products list
        3. Extract and clean category hierarchy
        4. Parse review data with ratings and dates
        5. Calculate aggregate review metrics
        6. Validate and clean all extracted data
        7. Return structured Product object
        """
        try:
            lines = product_text.strip().split('\n')
            
            # Initialize product with required fields
            product_id = self._extract_id(lines[0])
            asin = self._extract_asin(lines[1])
            
            if product_id is None or not asin:
                return None
                
            product = Product(id=product_id, asin=asin)
            
            # Extract other fields
            for line in lines[2:]:
                line = line.strip()
                
                if line.startswith('title:'):
                    product.title = self._clean_title(line[6:].strip())
                    
                elif line.startswith('group:'):
                    product.group = line[6:].strip()
                    
                elif line.startswith('salesrank:'):
                    try:
                        product.salesrank = int(line[10:].strip())
                    except ValueError:
                        product.salesrank = None
                        
                elif line.startswith('similar:'):
                    product.similar_products = self._extract_similar_products(line)
                    
                elif line.startswith('|') and '[' in line:
                    category = self._extract_category(line)
                    if category and category not in product.categories:
                        product.categories.append(category)
                        
                elif 'cutomer:' in line or 'customer:' in line:  # Handle typo in original data
                    review = self._parse_review(line)
                    if review:
                        product.reviews.append(review)
                        
                elif line.startswith('reviews: total:'):
                    product.total_reviews, product.avg_rating = self._extract_review_summary(line)
            
            # Data quality validation
            if not product.title or not product.group:
                return None
                
            return product
            
        except Exception as e:
            logger.warning(f"Failed to parse product: {e}")
            return None
    
    def _extract_id(self, line: str) -> Optional[int]:
        """Extract product ID from line"""
        match = re.search(r'Id:\s*(\d+)', line)
        return int(match.group(1)) if match else None
    
    def _extract_asin(self, line: str) -> Optional[str]:
        """Extract ASIN from line"""
        match = re.search(r'ASIN:\s*(\w+)', line)
        return match.group(1) if match else None
    
    def _clean_title(self, title: str) -> str:
        """
        Title cleaning:
        1. Remove extra whitespace
        2. Fix encoding issues
        3. Truncate very long titles
        4. Remove special characters that may cause issues
        """
        title = re.sub(r'\s+', ' ', title)  # Normalize whitespace
        title = title.replace('\x00', '')   # Remove null characters
        title = title[:200]                 # Truncate long titles
        return title.strip()
    
    def _extract_similar_products(self, line: str) -> List[str]:
        """Extract similar product ASINs"""
        parts = line.split()
        if len(parts) < 2:
            return []
        
        try:
            count = int(parts[1])
            asins = parts[2:2+count]
            return [asin for asin in asins if len(asin) == 10]  # Valid ASINs are 10 chars
        except (ValueError, IndexError):
            return []
    
    def _extract_category(self, line: str) -> Optional[str]:
        """Extract main category from category hierarchy"""
        # Extract category path like: |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|
        categories = re.findall(r'\|([^[]+)\[', line)
        return categories[-1] if categories else None  # Return most specific category
    
    def _parse_review(self, line: str) -> Optional[Dict]:
        """Parse individual review data"""
        try:
            # Pattern: 2000-7-28  cutomer: A2JW67OY8U6HHK  rating: 5  votes:  10  helpful:   9
            parts = line.strip().split()
            if len(parts) < 8:
                return None
                
            date = parts[0]
            customer_id = None
            rating = None
            votes = None
            helpful = None
            
            for i, part in enumerate(parts):
                if part.endswith(':'):
                    if i + 1 < len(parts):
                        if part == 'cutomer:' or part == 'customer:':
                            customer_id = parts[i + 1]
                        elif part == 'rating:':
                            try:
                                rating = int(parts[i + 1])
                            except ValueError:
                                pass
                        elif part == 'votes:':
                            try:
                                votes = int(parts[i + 1])
                            except ValueError:
                                pass
                        elif part == 'helpful:':
                            try:
                                helpful = int(parts[i + 1])
                            except ValueError:
                                pass
            
            if rating is not None:
                return {
                    'date': date,
                    'customer_id': customer_id,
                    'rating': rating,
                    'votes': votes,
                    'helpful': helpful
                }
        except Exception:
            pass
        return None
    
    def _extract_review_summary(self, line: str) -> tuple:
        """Extract total reviews and average rating"""
        try:
            # Pattern: reviews: total: 2  downloaded: 2  avg rating: 5
            total_match = re.search(r'total:\s*(\d+)', line)
            avg_match = re.search(r'avg rating:\s*([\d.]+)', line)
            
            total = int(total_match.group(1)) if total_match else None
            avg_rating = float(avg_match.group(1)) if avg_match else None
            
            return total, avg_rating
        except (ValueError, AttributeError):
            return None, None
    
    def process_data(self) -> None:
        """
        Main processing pipeline:
        1. Apply data reduction
        2. Parse and transform each product
        3. Apply data cleansing
        4. Save to JSON format for Neo4j ingestion
        """
        logger.info("Starting data processing pipeline")
        start_time = time.time()
        
        processed_products = []
        
        for product_text in self.data_reduction():
            product = self.parse_product(product_text)
            if product:
                processed_products.append(product.__dict__)
        
        # Save processed data
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_products, f, indent=2, ensure_ascii=False)
        
        processing_time = time.time() - start_time
        file_size_mb = self.output_file.stat().st_size / (1024 * 1024)
        
        logger.info(f"Processing complete:")
        logger.info(f"- Products processed: {len(processed_products)}")
        logger.info(f"- Output file size: {file_size_mb:.2f} MB")
        logger.info(f"- Processing time: {processing_time:.2f} seconds")
        logger.info(f"- Output file: {self.output_file}")

def main():
    """Main execution function"""
    input_file = "amazon-meta.txt"
    output_file = "processed_amazon_data.json"
    
    # Target 50,000 products for ~100-200MB output (within 10-500MB requirement)
    processor = AmazonDataProcessor(input_file, output_file, max_products=50000)
    processor.process_data()

if __name__ == "__main__":
    main()