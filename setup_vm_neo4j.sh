#!/bin/bash
# Setup script for running Spark with Neo4j in a VirtualBox VM
# Run this inside the Ubuntu VM: ./setup_vm_spark_neo4j.sh

set -e

echo "=========================================="
echo "Setting up Python, Spark, and Neo4j"
echo "=========================================="

# Update package list
echo "→ Updating package list..."
sudo apt update

# Install Python 3.11 and dependencies
echo "→ Installing Python 3.11..."
sudo apt install -y python3.11 python3.11-venv python3-pip default-jdk wget

# Check Java installation
echo "→ Checking Java installation..."
java -version

# Navigate to project directory
PROJECT_DIR="/home/ubuntu/Project"
echo "→ Navigating to $PROJECT_DIR"
cd "$PROJECT_DIR"

# Create virtual environment
echo "→ Creating Python virtual environment..."
python3.11 -m venv venv_vm

# Activate and install dependencies
echo "→ Installing Python packages..."
source venv_vm/bin/activate
pip install --upgrade pip
pip install pyspark neo4j

# Install Neo4j
echo "→ Installing Neo4j..."
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo gpg --dearmor -o /usr/share/keyrings/neo4j.gpg
echo "deb [signed-by=/usr/share/keyrings/neo4j.gpg] https://debian.neo4j.com stable 4.4" | sudo tee /etc/apt/sources.list.d/neo4j.list
sudo apt update
sudo apt install -y neo4j

# Start Neo4j service
echo "→ Starting Neo4j service..."
sudo systemctl enable neo4j
sudo systemctl start neo4j

# Verify Neo4j is running
echo "→ Verifying Neo4j service..."
sudo systemctl status neo4j | grep "active (running)"

# Print Neo4j default credentials
echo ""
echo "=========================================="
echo "Neo4j installed and running!"
echo "=========================================="
echo "Default credentials:"
echo "  URL: http://localhost:7474"
echo "  Bolt: bolt://localhost:7687"
echo "  Username: neo4j"
echo "  Password: neo4j (you will be prompted to change this on first login)"
echo ""

echo "=========================================="
echo "✓ Setup complete!"
echo "=========================================="
echo ""
echo "To run the Spark script:"
echo "  1. cd /home/ubuntu/Project"
echo "  2. source venv_vm/bin/activate"
echo "  3. python scripts/spark_neo4j_pattern_mining.py"
echo ""
echo "Python version:"
python --version
echo ""
echo "Spark version:"
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
echo ""