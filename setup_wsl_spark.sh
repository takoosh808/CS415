#!/bin/bash
# Setup script for running Spark with Neo4j in WSL2
# Run this inside WSL2: wsl -d Ubuntu ./setup_wsl_spark.sh

set -e

echo "=========================================="
echo "Setting up Python environment in WSL2"
echo "=========================================="

# Update package list
echo "→ Updating package list..."
sudo apt update

# Install Python 3.11 and dependencies
echo "→ Installing Python 3.11..."
sudo apt install -y python3.11 python3.11-venv python3-pip default-jdk

# Check Java installation
echo "→ Checking Java installation..."
java -version

# Navigate to project directory
PROJECT_DIR="/mnt/c/Users/Brian/source/repos/Project"
echo "→ Navigating to $PROJECT_DIR"
cd "$PROJECT_DIR"

# Create virtual environment
echo "→ Creating Python virtual environment..."
python3.11 -m venv venv_wsl

# Activate and install dependencies
echo "→ Installing Python packages..."
source venv_wsl/bin/activate
pip install --upgrade pip
pip install pyspark neo4j

echo ""
echo "=========================================="
echo "✓ Setup complete!"
echo "=========================================="
echo ""
echo "To run the script:"
echo "  1. wsl -d Ubuntu"
echo "  2. cd /mnt/c/Users/Brian/source/repos/Project"
echo "  3. source venv_wsl/bin/activate"
echo "  4. python scripts/spark_neo4j_pattern_mining.py"
echo ""
echo "Python version:"
python --version
echo ""
echo "Spark version:"
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
echo ""
