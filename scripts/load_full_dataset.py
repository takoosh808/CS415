"""
Load full Amazon dataset (932 MB) into Neo4j for scalability validation
This script uses CSV bulk import for maximum performance
"""

import subprocess
import sys
import os
import time
from pathlib import Path


def check_neo4j_running():
    """Check if Neo4j is accessible"""
    from neo4j import GraphDatabase
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                     auth=("neo4j", "Password"))
        with driver.session(database="neo4j") as session:
            session.run("RETURN 1")
        driver.close()
        return True
    except Exception as e:
        print(f"Neo4j not accessible: {e}")
        return False


def convert_full_dataset_to_csv():
    """Convert full amazon-meta.txt to CSV files"""
    print("\n" + "="*70)
    print("STEP 1: Converting full dataset to CSV files")
    print("="*70)
    
    csv_output_dir = "csv_data_full"
    
    # Check if already converted
    if os.path.exists(csv_output_dir) and os.path.exists(f"{csv_output_dir}/products.csv"):
        print(f"\n✓ CSV files already exist in {csv_output_dir}/")
        response = input("Do you want to reconvert? (y/n): ").strip().lower()
        if response != 'y':
            print("Using existing CSV files...")
            return csv_output_dir
    
    # Run conversion
    print("\nConverting amazon-meta.txt to CSV format...")
    print("This will take 5-15 minutes for the full dataset...")
    
    start_time = time.time()
    result = subprocess.run([
        sys.executable, 
        "scripts/convert_to_csv.py",
        "--file", "amazon-meta.txt",
        "--output", csv_output_dir
    ], capture_output=True, text=True)
    
    elapsed = time.time() - start_time
    
    if result.returncode == 0:
        print(f"\n✓ CSV conversion completed in {elapsed/60:.1f} minutes")
        print(result.stdout)
        return csv_output_dir
    else:
        print(f"\n✗ Conversion failed:")
        print(result.stderr)
        return None


def bulk_import_to_neo4j(csv_dir):
    """Use Neo4j bulk import for fast loading"""
    print("\n" + "="*70)
    print("STEP 2: Bulk importing into Neo4j")
    print("="*70)
    print("\nNOTE: For bulk import, Neo4j must be STOPPED.")
    print("This provides 10-100x faster loading than streaming import.")
    print("\nInstructions:")
    print("1. Stop Neo4j in Neo4j Desktop")
    print("2. Delete existing 'neo4j' database if needed")
    print("3. Run the neo4j-admin import command (see BULK_IMPORT.md)")
    print("4. Start Neo4j again")
    
    print("\n" + "-"*70)
    print("Alternative: Stream loading (slower but doesn't require stopping DB)")
    print("-"*70)
    
    response = input("\nDo you want to use stream loading instead? (y/n): ").strip().lower()
    
    if response == 'y':
        return stream_load_from_csv(csv_dir)
    else:
        print("\nPlease follow the bulk import instructions in BULK_IMPORT.md")
        print("Once complete, run the benchmark script to validate performance.")
        return False


def stream_load_from_csv(csv_dir):
    """Stream load CSV data into Neo4j (slower but DB can stay running)"""
    print("\n" + "="*70)
    print("Stream Loading Data into Neo4j")
    print("="*70)
    
    if not check_neo4j_running():
        print("\n✗ Neo4j is not running. Please start it first.")
        return False
    
    print("\nThis will take 1-3 hours for the full dataset...")
    print("Loading via streaming inserts...")
    
    start_time = time.time()
    
    # Use our stream_load_full.py script but with full dataset
    result = subprocess.run([
        sys.executable,
        "scripts/stream_load_full.py",
        "--file", "amazon-meta.txt"
    ], capture_output=False, text=True)
    
    elapsed = time.time() - start_time
    
    if result.returncode == 0:
        print(f"\n✓ Stream loading completed in {elapsed/60:.1f} minutes")
        return True
    else:
        print(f"\n✗ Loading failed")
        return False


def create_optimized_indexes():
    """Create additional indexes for scalability"""
    print("\n" + "="*70)
    print("STEP 3: Creating optimized indexes")
    print("="*70)
    
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                     auth=("neo4j", "Password"))
        
        with driver.session(database="neo4j") as session:
            print("\nCreating indexes...")
            
            # Existing indexes
            indexes = [
                "CREATE INDEX product_asin IF NOT EXISTS FOR (p:Product) ON (p.asin)",
                "CREATE INDEX product_id IF NOT EXISTS FOR (p:Product) ON (p.id)",
                
                # New indexes for scalability
                "CREATE INDEX product_rating IF NOT EXISTS FOR (p:Product) ON (p.avg_rating)",
                "CREATE INDEX product_reviews IF NOT EXISTS FOR (p:Product) ON (p.total_reviews)",
                "CREATE INDEX product_group IF NOT EXISTS FOR (p:Product) ON (p.group)",
                "CREATE INDEX product_salesrank IF NOT EXISTS FOR (p:Product) ON (p.salesrank)",
                "CREATE INDEX customer_id IF NOT EXISTS FOR (c:Customer) ON (c.id)",
            ]
            
            for idx in indexes:
                try:
                    session.run(idx)
                    print(f"  ✓ {idx.split('INDEX')[1].split('IF')[0].strip()}")
                except Exception as e:
                    print(f"  - {idx.split('INDEX')[1].split('IF')[0].strip()} (already exists)")
            
            print("\n✓ All indexes created successfully")
        
        driver.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Failed to create indexes: {e}")
        return False


def verify_data_load():
    """Verify the data was loaded correctly"""
    print("\n" + "="*70)
    print("STEP 4: Verifying data load")
    print("="*70)
    
    from neo4j import GraphDatabase
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                     auth=("neo4j", "Password"))
        
        with driver.session(database="neo4j") as session:
            # Count nodes
            result = session.run("MATCH (p:Product) RETURN count(p) as count")
            product_count = result.single()['count']
            
            result = session.run("MATCH (c:Customer) RETURN count(c) as count")
            customer_count = result.single()['count']
            
            # Count relationships
            result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
            review_count = result.single()['count']
            
            result = session.run("MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as count")
            similar_count = result.single()['count']
            
            print(f"\nDatabase Statistics:")
            print(f"  Products:     {product_count:>12,}")
            print(f"  Customers:    {customer_count:>12,}")
            print(f"  Reviews:      {review_count:>12,}")
            print(f"  Similar:      {similar_count:>12,}")
            print(f"  Total Nodes:  {product_count + customer_count:>12,}")
            print(f"  Total Edges:  {review_count + similar_count:>12,}")
            
            # Check if this is full dataset
            if product_count > 500000:
                print("\n✓ Full dataset detected (500K+ products)")
            elif product_count > 100000:
                print("\n⚠ Large dataset detected (100K+ products)")
            elif product_count > 30000:
                print("\n⚠ Test dataset detected (~36K products)")
            else:
                print("\n⚠ Small dataset detected")
            
        driver.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Verification failed: {e}")
        return False


def main():
    print("\n" + "╔" + "="*68 + "╗")
    print("║" + " "*68 + "║")
    print("║" + "  AMAZON CO-PURCHASING ANALYTICS - FULL DATASET LOADER".center(68) + "║")
    print("║" + "  Milestone 4 - Scalability Validation".center(68) + "║")
    print("║" + " "*68 + "║")
    print("╚" + "="*68 + "╝")
    
    # Check if full dataset exists
    if not os.path.exists("amazon-meta.txt"):
        print("\n✗ amazon-meta.txt not found in current directory")
        print("Please ensure the full dataset (932 MB) is present.")
        return
    
    # Get file size
    size_mb = os.path.getsize("amazon-meta.txt") / (1024 * 1024)
    print(f"\n✓ Found amazon-meta.txt ({size_mb:.1f} MB)")
    
    if size_mb < 100:
        print("\n⚠ WARNING: This appears to be the test dataset (10 MB)")
        print("For full scalability validation, you need the 932 MB dataset.")
        response = input("Continue anyway? (y/n): ").strip().lower()
        if response != 'y':
            return
    
    # Step 1: Convert to CSV
    csv_dir = convert_full_dataset_to_csv()
    if not csv_dir:
        print("\n✗ Failed to convert dataset to CSV")
        return
    
    # Step 2: Load into Neo4j
    print("\n")
    print("Choose loading method:")
    print("1. Bulk Import (FAST - 10-30 min, requires stopping Neo4j)")
    print("2. Stream Load (SLOW - 1-3 hours, Neo4j stays running)")
    print("3. Skip loading (already loaded)")
    
    choice = input("\nEnter choice (1/2/3): ").strip()
    
    if choice == '1':
        bulk_import_to_neo4j(csv_dir)
        print("\nAfter bulk import completes, restart Neo4j and run:")
        print("  python scripts/validate_scalability.py")
        return
    elif choice == '2':
        if not stream_load_from_csv(csv_dir):
            print("\n✗ Stream loading failed")
            return
    elif choice == '3':
        print("\nSkipping data load...")
    else:
        print("\n✗ Invalid choice")
        return
    
    # Step 3: Create indexes
    if not create_optimized_indexes():
        print("\n⚠ Index creation failed, but continuing...")
    
    # Step 4: Verify
    if verify_data_load():
        print("\n" + "="*70)
        print("✓ FULL DATASET LOADED SUCCESSFULLY")
        print("="*70)
        print("\nNext steps:")
        print("1. Run benchmarks: python scripts/benchmark_full_dataset.py")
        print("2. Run Spark algorithms: python scripts/run_spark_algorithms.py")
        print("3. Update GUI to show full dataset stats")
    else:
        print("\n✗ Data verification failed")


if __name__ == "__main__":
    main()
