# Create test data script
# Takes the big amazon file and makes a smaller one for testing

import os

def create_test_data():
    # Settings
    input_file = "amazon-meta.txt"
    output_file = "amazon-meta-test.txt" 
    target_size_mb = 10
    
    print(f"Creating {target_size_mb}MB test dataset from {input_file}")
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found!")
        return False
    
    # Calculate target size in bytes
    target_bytes = target_size_mb * 1024 * 1024
    current_bytes = 0
    products_copied = 0
    
    try:
        with open(input_file, 'r') as infile:
            with open(output_file, 'w') as outfile:
                current_product_lines = []
                
                for line in infile:
                    current_product_lines.append(line)
                    
                    # Check if we finished a product (empty line)
                    if line.strip() == '':
                        # Calculate size of this product
                        product_text = ''.join(current_product_lines)
                        product_size = len(product_text.encode('utf-8'))
                        
                        # Check if adding this product would exceed target size
                        if current_bytes + product_size > target_bytes:
                            print(f"Reached target size with {products_copied} products")
                            break
                        
                        # Write the product
                        outfile.write(product_text)
                        current_bytes += product_size
                        products_copied += 1
                        
                        # Clear for next product
                        current_product_lines = []
                        
                        # Progress update
                        if products_copied % 500 == 0:
                            print(f"Copied {products_copied} products, {current_bytes / (1024*1024):.1f}MB")
        
        # Final stats
        final_size_mb = current_bytes / (1024 * 1024)
        print(f"Test dataset created: {output_file}")
        print(f"Size: {final_size_mb:.1f}MB")
        print(f"Products: {products_copied}")
        
        return True
        
    except Exception as e:
        print(f"Error creating test data: {e}")
        return False

def main():
    print("Amazon Test Data Creator")
    print("-" * 30)
    
    success = create_test_data()
    
    if success:
        print("Done!")
    else:
        print("Failed!")

if __name__ == "__main__":
    main()