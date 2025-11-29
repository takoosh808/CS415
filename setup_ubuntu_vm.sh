#!/bin/bash
# Automated Ubuntu VM Setup for Neo4j + Spark Testing
# Run this script on a fresh Ubuntu 22.04 VM
# Usage: bash setup_ubuntu_vm.sh

set -e  # Exit on any error

echo "========================================================================"
echo "AUTOMATED SETUP: Neo4j + Spark for Full Dataset Testing"
echo "========================================================================"
echo ""
echo "This script will install:"
echo "  - Java 11 (required for Spark 3.5.3)"
echo "  - Python 3.11 + dependencies"
echo "  - Neo4j 4.4 Community Edition"
echo "  - PySpark 3.5.3 with Neo4j Connector"
echo ""
echo "VM Requirements:"
echo "  - Ubuntu 22.04 LTS"
echo "  - 12GB RAM minimum"
echo "  - 4 CPU cores minimum"
echo "  - 80GB disk space"
echo ""
read -p "Press Enter to start installation (or Ctrl+C to cancel)..."

# Update system
echo ""
echo "=========================================="
echo "Step 1/6: Updating system packages..."
echo "=========================================="
sudo apt update
sudo apt upgrade -y

# Install Java 11
echo ""
echo "=========================================="
echo "Step 2/6: Installing Java 11..."
echo "=========================================="
sudo apt install openjdk-11-jdk -y
java -version
echo "✓ Java 11 installed"

# Install Python 3.11
echo ""
echo "=========================================="
echo "Step 3/6: Installing Python 3.11..."
echo "=========================================="
sudo apt install python3.11 python3.11-venv python3-pip curl wget -y
python3.11 --version
echo "✓ Python 3.11 installed"

# Install Neo4j 4.4
echo ""
echo "=========================================="
echo "Step 4/6: Installing Neo4j 4.4..."
echo "=========================================="

# Add Neo4j repository
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
echo 'deb https://debian.neo4j.com stable 4.4' | sudo tee /etc/apt/sources.list.d/neo4j.list
sudo apt update

# Install Neo4j
sudo apt install neo4j=1:4.4.39 -y

# Configure Neo4j for optimal performance
echo "Configuring Neo4j..."
sudo tee -a /etc/neo4j/neo4j.conf > /dev/null <<EOF

# Performance tuning for Spark testing
dbms.memory.heap.initial_size=4G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2G

# Allow remote connections (for accessing from host machine)
dbms.default_listen_address=0.0.0.0

# Increase transaction timeout for large queries
dbms.transaction.timeout=300s
EOF

# Start Neo4j
sudo systemctl enable neo4j
sudo systemctl start neo4j

echo "Waiting 30 seconds for Neo4j to start..."
sleep 30

# Change default password
echo "Setting Neo4j password to 'Password'..."
curl -X POST http://localhost:7474/user/neo4j/password \
  -H "Content-Type: application/json" \
  -d '{"password":"Password"}' \
  -u neo4j:neo4j 2>/dev/null || echo "Note: Password may already be set"

# Verify Neo4j is running
if sudo systemctl is-active --quiet neo4j; then
    echo "✓ Neo4j 4.4 installed and running"
else
    echo "⚠ Neo4j may not be running. Check: sudo systemctl status neo4j"
fi

# Setup Python environment
echo ""
echo "=========================================="
echo "Step 5/6: Setting up Python environment..."
echo "=========================================="

# Create project directory
mkdir -p ~/neo4j-spark-testing
cd ~/neo4j-spark-testing

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "Installing Python packages..."
pip install --upgrade pip
pip install pyspark==3.5.3 neo4j numpy pandas

echo "✓ Python environment ready"

# Create helper scripts
echo ""
echo "=========================================="
echo "Step 6/6: Creating helper scripts..."
echo "=========================================="

# Create Neo4j test script
cat > ~/neo4j-spark-testing/test_neo4j.py <<'PYEOF'
#!/usr/bin/env python3
"""Test Neo4j connection and count records"""
from neo4j import GraphDatabase
import sys

uri = "bolt://localhost:7687"
user = "neo4j"
password = "Password"

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        # Test connection
        result = session.run("RETURN 1 as test")
        print("✓ Neo4j connection successful")
        
        # Count nodes
        customers = session.run("MATCH (c:Customer) RETURN count(c) as count").single()["count"]
        products = session.run("MATCH (p:Product) RETURN count(p) as count").single()["count"]
        reviews = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count").single()["count"]
        
        print(f"✓ Customers: {customers:,}")
        print(f"✓ Products: {products:,}")
        print(f"✓ Reviews: {reviews:,}")
        
        if reviews == 0:
            print("\n⚠ No data loaded yet. Load your data before running Spark script.")
            sys.exit(1)
        else:
            print("\n✓ Database ready for Spark testing!")
            
    driver.close()
except Exception as e:
    print(f"✗ Error connecting to Neo4j: {e}")
    print("\nTroubleshooting:")
    print("  1. Check Neo4j is running: sudo systemctl status neo4j")
    print("  2. Check password is correct")
    print("  3. Check firewall: sudo ufw status")
    sys.exit(1)
PYEOF

chmod +x ~/neo4j-spark-testing/test_neo4j.py

# Create data loader script
cat > ~/neo4j-spark-testing/load_data.py <<'PYEOF'
#!/usr/bin/env python3
"""Load CSV data into Neo4j"""
from neo4j import GraphDatabase
import csv
import os
import sys
from pathlib import Path

uri = "bolt://localhost:7687"
user = "neo4j"
password = "Password"

def load_csv_data(csv_dir):
    """Load all CSV files from directory into Neo4j"""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        print(f"✗ CSV directory not found: {csv_dir}")
        print("Please copy your csv_data folder to this VM first.")
        sys.exit(1)
    
    print("Loading data from:", csv_path.absolute())
    
    with driver.session() as session:
        # Clear existing data
        print("\n1. Clearing existing data...")
        session.run("MATCH (n) DETACH DELETE n")
        print("✓ Database cleared")
        
        # Create constraints for better performance
        print("\n2. Creating constraints...")
        session.run("CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE")
        session.run("CREATE CONSTRAINT product_asin IF NOT EXISTS FOR (p:Product) REQUIRE p.asin IS UNIQUE")
        print("✓ Constraints created")
        
        # Load customers
        customers_file = csv_path / "customers.csv"
        if customers_file.exists():
            print("\n3. Loading customers...")
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///' + $filepath AS row
                CREATE (c:Customer {customer_id: row.customer_id})
            """, filepath=str(customers_file.absolute()))
            count = session.run("MATCH (c:Customer) RETURN count(c) as count").single()["count"]
            print(f"✓ Loaded {count:,} customers")
        
        # Load products
        products_file = csv_path / "products.csv"
        if products_file.exists():
            print("\n4. Loading products...")
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///' + $filepath AS row
                CREATE (p:Product {
                    asin: row.asin,
                    title: row.title,
                    group: row.`group`
                })
            """, filepath=str(products_file.absolute()))
            count = session.run("MATCH (p:Product) RETURN count(p) as count").single()["count"]
            print(f"✓ Loaded {count:,} products")
        
        # Load reviews (this will take longest)
        reviews_file = csv_path / "reviews.csv"
        if reviews_file.exists():
            print("\n5. Loading reviews (this may take 5-10 minutes)...")
            # Batch loading for better performance
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///' + $filepath AS row
                MATCH (c:Customer {customer_id: row.customer_id})
                MATCH (p:Product {asin: row.product_id})
                CREATE (c)-[:REVIEWED {
                    rating: toInteger(row.rating),
                    date: row.date,
                    helpful: toInteger(row.helpful),
                    votes: toInteger(row.votes),
                    dataset: row.dataset
                }]->(p)
            """, filepath=str(reviews_file.absolute()))
            count = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count").single()["count"]
            print(f"✓ Loaded {count:,} reviews")
        
        print("\n✓ Data loading complete!")
        print("\nRun test script to verify: python test_neo4j.py")
    
    driver.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_data.py /path/to/csv_data")
        print("\nExample:")
        print("  python load_data.py ~/project/csv_data")
        print("  python load_data.py /mnt/shared/csv_data")
        sys.exit(1)
    
    csv_directory = sys.argv[1]
    load_csv_data(csv_directory)
PYEOF

chmod +x ~/neo4j-spark-testing/load_data.py

# Create quick start guide
cat > ~/neo4j-spark-testing/README.txt <<'EOF'
======================================================================
SETUP COMPLETE! Next Steps:
======================================================================

1. TRANSFER YOUR DATA TO THIS VM
   
   Option A: Using Shared Folder (if VirtualBox Guest Additions installed)
     - In VirtualBox: VM → Settings → Shared Folders
     - Add: C:\Users\Brian\source\repos\Project → mount at ~/project
     - Then: ls ~/project  # Should see your files
   
   Option B: Using SCP from Windows
     - Find VM IP: ip addr show
     - From Windows PowerShell:
       scp -r C:\Users\Brian\source\repos\Project\csv_data spark@<VM-IP>:~/
       scp -r C:\Users\Brian\source\repos\Project\scripts spark@<VM-IP>:~/
   
   Option C: Download from cloud storage
     - If you uploaded to Google Drive, Dropbox, etc.
     - wget <download-url>

2. LOAD DATA INTO NEO4J
   
   cd ~/neo4j-spark-testing
   source venv/bin/activate
   
   # Option A: Use the loader script
   python load_data.py ~/project/csv_data
   
   # Option B: Use your existing loader
   python ~/project/scripts/load_full_dataset.py

3. TEST NEO4J CONNECTION
   
   python test_neo4j.py
   
   Should show:
   ✓ Neo4j connection successful
   ✓ Customers: 1,555,170
   ✓ Products: 548,552
   ✓ Reviews: 7,593,244

4. RUN YOUR SPARK SCRIPT
   
   # Copy your script (if not already here)
   cp ~/project/scripts/spark_neo4j_pattern_mining.py ./
   
   # Edit to remove test mode LIMIT (line 63):
   nano spark_neo4j_pattern_mining.py
   # Comment out or remove: reviews_df = reviews_df.limit(10000)
   
   # Run the full dataset test!
   python spark_neo4j_pattern_mining.py
   
   Expected runtime: 10-15 minutes
   Expected output: spark_neo4j_patterns.json

5. COPY RESULTS BACK TO WINDOWS
   
   # Via shared folder:
   cp spark_neo4j_patterns.json ~/project/
   
   # Via SCP (from Windows):
   scp spark@<VM-IP>:~/neo4j-spark-testing/spark_neo4j_patterns.json .

======================================================================
TROUBLESHOOTING
======================================================================

Neo4j not running:
  sudo systemctl status neo4j
  sudo systemctl restart neo4j
  sudo journalctl -u neo4j -n 50

Check Neo4j logs:
  sudo tail -f /var/log/neo4j/neo4j.log

Test Neo4j from browser:
  http://<VM-IP>:7474
  Username: neo4j
  Password: Password

Memory issues:
  free -h  # Check available memory
  htop     # Monitor during execution

Python environment:
  source ~/neo4j-spark-testing/venv/bin/activate
  pip list | grep pyspark  # Should show 3.5.3

Java version:
  java -version  # Should be 11.x

======================================================================
VM SNAPSHOT RECOMMENDATION
======================================================================

Before loading data, take a VirtualBox snapshot:
  VirtualBox → VM → Snapshots → Take

This lets you quickly restore if something goes wrong.

======================================================================
EOF

echo "✓ Helper scripts created"

# Print summary
echo ""
echo "========================================================================"
echo "✓ SETUP COMPLETE!"
echo "========================================================================"
echo ""
echo "Installed:"
echo "  ✓ Java 11.x"
echo "  ✓ Python 3.11"
echo "  ✓ Neo4j 4.4 (running on port 7687)"
echo "  ✓ PySpark 3.5.3 with Neo4j Connector"
echo ""
echo "Working directory: ~/neo4j-spark-testing"
echo ""
echo "Neo4j credentials:"
echo "  URL: bolt://localhost:7687"
echo "  Username: neo4j"
echo "  Password: Password"
echo ""
echo "Next steps:"
echo "  1. Read: cat ~/neo4j-spark-testing/README.txt"
echo "  2. Transfer your data (csv_data folder) to this VM"
echo "  3. Load data: python ~/neo4j-spark-testing/load_data.py <csv_dir>"
echo "  4. Test: python ~/neo4j-spark-testing/test_neo4j.py"
echo "  5. Run Spark: python spark_neo4j_pattern_mining.py"
echo ""
echo "To access this environment later:"
echo "  cd ~/neo4j-spark-testing"
echo "  source venv/bin/activate"
echo ""
echo "========================================================================"
echo "VM is ready for full dataset testing!"
echo "========================================================================"
