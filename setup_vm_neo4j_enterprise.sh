#!/bin/bash
# Setup script for running a Neo4j Enterprise multi-node cluster in a VirtualBox VM
# Run this inside the Ubuntu VM: ./setup_vm_neo4j_enterprise.sh

set -e

echo "=========================================="
echo "Setting up Python, Spark, and Neo4j Enterprise Cluster"
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

# Install Neo4j Enterprise Edition
echo "→ Installing Neo4j Enterprise Edition..."
wget https://dist.neo4j.com/neo4j-enterprise-4.4.21-unix.tar.gz -O neo4j-enterprise.tar.gz
tar -xzf neo4j-enterprise.tar.gz
mv neo4j-enterprise-4.4.21 neo4j-enterprise
rm neo4j-enterprise.tar.gz

# Set up directories for 3-core cluster
echo "→ Setting up directories for 3-core cluster..."
BASE_DIR="/home/ubuntu/neo4j_cluster"
mkdir -p $BASE_DIR/core1 $BASE_DIR/core2 $BASE_DIR/core3

# Copy Neo4j Enterprise files to each core
echo "→ Copying Neo4j files to Core 1..."
cp -r neo4j-enterprise/* $BASE_DIR/core1/
echo "→ Copying Neo4j files to Core 2..."
cp -r neo4j-enterprise/* $BASE_DIR/core2/
echo "→ Copying Neo4j files to Core 3..."
cp -r neo4j-enterprise/* $BASE_DIR/core3/

# Configure Core 1
echo "→ Configuring Core 1..."
cat <<EOF > $BASE_DIR/core1/conf/neo4j.conf
dbms.default_listen_address=0.0.0.0
dbms.default_advertised_address=localhost
dbms.mode=CORE
dbms.cluster.discovery.endpoints=localhost:5000,localhost:5001,localhost:5002
dbms.cluster.raft.endpoints=localhost:6000,localhost:6001,localhost:6002
dbms.cluster.transaction.endpoints=localhost:7000,localhost:7001,localhost:7002
dbms.cluster.routing.enabled=true
dbms.cluster.routing.default_router=localhost:7687
dbms.default_database=neo4j
dbms.memory.heap.initial_size=512m
dbms.memory.heap.max_size=1g
dbms.memory.pagecache.size=512m
EOF

# Configure Core 2
echo "→ Configuring Core 2..."
cat <<EOF > $BASE_DIR/core2/conf/neo4j.conf
dbms.default_listen_address=0.0.0.0
dbms.default_advertised_address=localhost
dbms.mode=CORE
dbms.cluster.discovery.endpoints=localhost:5000,localhost:5001,localhost:5002
dbms.cluster.raft.endpoints=localhost:6000,localhost:6001,localhost:6002
dbms.cluster.transaction.endpoints=localhost:7000,localhost:7001,localhost:7002
dbms.cluster.routing.enabled=true
dbms.cluster.routing.default_router=localhost:7688
dbms.default_database=neo4j
dbms.memory.heap.initial_size=512m
dbms.memory.heap.max_size=1g
dbms.memory.pagecache.size=512m
EOF

# Configure Core 3
echo "→ Configuring Core 3..."
cat <<EOF > $BASE_DIR/core3/conf/neo4j.conf
dbms.default_listen_address=0.0.0.0
dbms.default_advertised_address=localhost
dbms.mode=CORE
dbms.cluster.discovery.endpoints=localhost:5000,localhost:5001,localhost:5002
dbms.cluster.raft.endpoints=localhost:6000,localhost:6001,localhost:6002
dbms.cluster.transaction.endpoints=localhost:7000,localhost:7001,localhost:7002
dbms.cluster.routing.enabled=true
dbms.cluster.routing.default_router=localhost:7689
dbms.default_database=neo4j
dbms.memory.heap.initial_size=512m
dbms.memory.heap.max_size=1g
dbms.memory.pagecache.size=512m
EOF

# Start each core
echo "→ Starting Core 1..."
$BASE_DIR/core1/bin/neo4j start
echo "→ Starting Core 2..."
$BASE_DIR/core2/bin/neo4j start
echo "→ Starting Core 3..."
$BASE_DIR/core3/bin/neo4j start

echo "=========================================="
echo "✓ Neo4j Enterprise Multi-Node Cluster Setup Complete!"
echo "=========================================="
echo ""
echo "Access the cluster:"
echo "  Core 1: http://localhost:7474 (Bolt: 7687)"
echo "  Core 2: http://localhost:7475 (Bolt: 7688)"
echo "  Core 3: http://localhost:7476 (Bolt: 7689)"
echo ""