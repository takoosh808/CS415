# VirtualBox Ubuntu Setup for Full Dataset Testing

## Overview

This guide will help you set up a Linux VM to test your full 900MB dataset (7.6M reviews) with the Neo4j Connector for Spark.

**Estimated Setup Time**: 2-3 hours  
**Cost**: $0 (uses existing hardware)  
**Expected Success Rate**: 85-90%  
**Full Dataset Runtime**: 8-12 minutes once configured

---

## Part 1: Create Ubuntu VM (30 minutes)

### Step 1: Download Ubuntu Server ISO

1. Download Ubuntu 22.04 LTS Server: https://ubuntu.com/download/server
   - File: `ubuntu-22.04.5-live-server-amd64.iso` (~2GB)
   - Or use Desktop version if you prefer GUI: https://ubuntu.com/download/desktop

### Step 2: Create VM in VirtualBox

1. **Open VirtualBox** → Click "New"

2. **VM Settings**:

   - Name: `Neo4j-Spark-Testing`
   - Type: `Linux`
   - Version: `Ubuntu (64-bit)`
   - Click "Next"

3. **Memory Size**:

   - Allocate: **12GB (12288 MB)** minimum
   - More is better if available (16GB ideal)
   - Click "Next"

4. **Hard Disk**:

   - Select "Create a virtual hard disk now"
   - Click "Create"
   - Type: `VDI (VirtualBox Disk Image)`
   - Storage: `Dynamically allocated`
   - Size: **80GB** (your data + Neo4j + Spark)
   - Click "Create"

5. **VM Configuration** (right-click VM → Settings):

   **System Tab**:

   - Processor: Allocate **4 CPU cores** minimum
   - Enable PAE/NX: ✓

   **Display Tab**:

   - Video Memory: 128 MB
   - Graphics Controller: VMSVGA

   **Storage Tab**:

   - Click empty CD icon under "Controller: IDE"
   - Click CD icon on right → "Choose a disk file"
   - Select the Ubuntu ISO you downloaded

   **Network Tab**:

   - Adapter 1: Enable Network Adapter ✓
   - Attached to: `Bridged Adapter` (allows Neo4j access from host)
   - Alternative: `NAT` with port forwarding (7474, 7687, 8080)

6. **Start VM** → Follow Ubuntu installation wizard:

   - Language: English
   - Update installer: Skip (do it later)
   - Keyboard: Your layout
   - Network: Accept defaults (DHCP)
   - Proxy: Leave blank
   - Mirror: Accept default
   - Storage: Use entire disk (guided)
   - Profile setup:
     - Your name: `spark`
     - Server name: `spark-vm`
     - Username: `spark`
     - Password: (your choice)
   - SSH: Install OpenSSH server ✓ (optional but helpful)
   - Featured snaps: Skip all
   - Installation will take 10-15 minutes

7. **After installation**:
   - Reboot when prompted
   - Remove ISO: Devices → Optical Drives → Remove disk
   - Login with your username/password

---

## Part 2: Install Prerequisites (30 minutes)

### Step 1: Update System

```bash
sudo apt update
sudo apt upgrade -y
```

### Step 2: Install Java 11 (required for Spark 3.5.3)

```bash
sudo apt install openjdk-11-jdk -y
java -version  # Verify: should show 11.x
```

### Step 3: Install Python 3.11

```bash
sudo apt install python3.11 python3.11-venv python3-pip -y
python3.11 --version
```

### Step 4: Install Neo4j 4.4

```bash
# Add Neo4j repository
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
echo 'deb https://debian.neo4j.com stable 4.4' | sudo tee /etc/apt/sources.list.d/neo4j.list
sudo apt update

# Install Neo4j Community Edition
sudo apt install neo4j=1:4.4.39 -y

# Configure Neo4j
sudo nano /etc/neo4j/neo4j.conf
```

**Neo4j Configuration** (edit these lines):

```properties
# Uncomment and set these:
dbms.memory.heap.initial_size=4G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2G

# Allow remote connections (if accessing from Windows host)
dbms.default_listen_address=0.0.0.0

# Default password will be 'neo4j', must change on first login
```

Save (Ctrl+O, Enter, Ctrl+X)

```bash
# Start Neo4j
sudo systemctl enable neo4j
sudo systemctl start neo4j
sudo systemctl status neo4j  # Should show "active (running)"

# Change default password
curl -X POST http://localhost:7474/user/neo4j/password \
  -H "Content-Type: application/json" \
  -d '{"password":"Password"}' \
  -u neo4j:neo4j
```

---

## Part 3: Transfer Your Data (30-45 minutes)

### Option A: Shared Folder (Recommended)

1. **On VM**: Install VirtualBox Guest Additions

```bash
sudo apt install virtualbox-guest-utils virtualbox-guest-dkms -y
sudo reboot
```

2. **In VirtualBox**:

   - VM → Settings → Shared Folders
   - Click folder+ icon
   - Folder Path: `C:\Users\Brian\source\repos\Project`
   - Folder Name: `project`
   - Auto-mount: ✓
   - Make Permanent: ✓
   - Mount Point: `/home/spark/project`

3. **On VM**: Mount shared folder

```bash
sudo usermod -aG vboxsf spark
sudo mkdir -p /home/spark/project
sudo mount -t vboxsf project /home/spark/project
# Or reboot and it will auto-mount

# Verify
ls /home/spark/project  # Should see your files
```

### Option B: SCP Transfer (Alternative)

1. **Find VM IP address**:

```bash
ip addr show  # Look for inet address (e.g., 192.168.1.xxx)
```

2. **From Windows PowerShell**:

```powershell
# Transfer CSV files
scp -r C:\Users\Brian\source\repos\Project\csv_data spark@<VM-IP>:~/
scp -r C:\Users\Brian\source\repos\Project\scripts spark@<VM-IP>:~/

# Or use WinSCP GUI tool
```

### Option C: Copy via USB/Network Share

Export your data, copy to VM via any method you prefer.

---

## Part 4: Load Data into Neo4j (15-20 minutes)

```bash
cd ~/project  # or wherever your data is

# Create Python environment
python3.11 -m venv venv
source venv/bin/activate
pip install neo4j pandas

# Run your bulk import script (or adapt one below)
```

**Quick bulk import script** (if needed):

```bash
# Stop Neo4j
sudo systemctl stop neo4j

# Use neo4j-admin import (fastest method)
sudo neo4j-admin import \
  --database=neo4j \
  --nodes=Customer=csv_data/customers.csv \
  --nodes=Product=csv_data/products.csv \
  --relationships=REVIEWED=csv_data/reviews.csv \
  --relationships=SIMILAR=csv_data/similar.csv \
  --relationships=BELONGS_TO=csv_data/belongs_to.csv

# Start Neo4j
sudo systemctl start neo4j

# Wait 30 seconds for startup
sleep 30

# Verify data loaded
cypher-shell -u neo4j -p Password "MATCH (n) RETURN count(n);"
```

**Or use your existing Python loader**:

```bash
python scripts/load_full_dataset.py
# or
python scripts/stream_load_full.py
```

---

## Part 5: Install Spark and Run Your Script (15 minutes)

### Step 1: Setup Python Environment

```bash
cd ~/project
python3.11 -m venv venv_spark
source venv_spark/bin/activate

# Install dependencies
pip install pyspark==3.5.3 neo4j numpy pandas
```

### Step 2: Verify Neo4j Connection

```bash
cypher-shell -u neo4j -p Password "MATCH (c:Customer) RETURN count(c);"
# Should show: 1,555,170

cypher-shell -u neo4j -p Password "MATCH ()-[r:REVIEWED]->() RETURN count(r);"
# Should show: 7,593,244
```

### Step 3: Update Script Configuration

Edit `scripts/spark_neo4j_pattern_mining.py`:

```python
# Change Neo4j URI to localhost (since Neo4j is on same machine)
self.neo4j_uri = "bolt://localhost:7687"

# Comment out the LIMIT 10000 line (line 63):
# reviews_df = reviews_df.limit(10000)  # Remove this line
```

Or restore full dataset mode:

```bash
# Remove test mode limit
sed -i 's/reviews_df = reviews_df.limit(10000)/# FULL DATASET MODE/' scripts/spark_neo4j_pattern_mining.py
```

### Step 4: Run the Full Dataset Test! 🚀

```bash
source venv_spark/bin/activate
python scripts/spark_neo4j_pattern_mining.py
```

**Expected Output**:

```
======================================================================
CO-PURCHASE PATTERN MINING WITH NEO4J CONNECTOR FOR SPARK
======================================================================
Initializing Spark with Neo4j connector...
Spark version: 3.5.3
Connected to Neo4j at bolt://localhost:7687

Loading reviews from Neo4j using Spark connector...
✓ Loaded 7,593,244 reviews in 45-90s
  Throughput: 85k-169k reviews/sec

Checking train/test split...
✓ Using existing split:
  Training: 5,315,271 reviews (70.0%)
  Testing: 2,277,973 reviews (30.0%)

======================================================================
MINING PATTERNS WITH FP-GROWTH (Dataset: TRAIN)
======================================================================
Creating customer transactions...
✓ Created ~1,088,619 customer transactions

Running FP-Growth algorithm...
  Min Support: 0.001 (1,089 transactions minimum)
  Min Confidence: 0.1
[Progress bars showing Spark jobs...]

✓ Pattern mining complete in 5-8 minutes
✓ Found XX,XXX frequent patterns
✓ Results saved to spark_neo4j_patterns.json

TOTAL RUNTIME: 8-12 minutes
```

---

## Part 6: Retrieve Results (5 minutes)

### View results in VM:

```bash
cat spark_neo4j_patterns.json | jq '.summary'
cat spark_neo4j_patterns.json | jq '.patterns | length'
```

### Copy results back to Windows:

**Via shared folder**:

```bash
cp spark_neo4j_patterns.json /home/spark/project/
# File is now in C:\Users\Brian\source\repos\Project on Windows
```

**Via SCP** (from Windows):

```powershell
scp spark@<VM-IP>:~/project/spark_neo4j_patterns.json C:\Users\Brian\source\repos\Project\
```

---

## Troubleshooting

### Neo4j won't start:

```bash
# Check logs
sudo journalctl -u neo4j -n 50

# Check if port is already in use
sudo netstat -tlnp | grep 7687

# Restart
sudo systemctl restart neo4j
```

### Out of memory during Spark execution:

```bash
# Edit the script to reduce memory:
# Change driver/executor memory from 6g to 4g
nano scripts/spark_neo4j_pattern_mining.py
```

### Slow performance:

```bash
# Check VM has enough resources
free -h  # Should have 10GB+ available
nproc    # Should show 4 cores

# Monitor during execution
htop
```

### Can't access Neo4j from Windows:

```bash
# Check firewall
sudo ufw allow 7474
sudo ufw allow 7687

# Verify Neo4j listening on all interfaces
sudo netstat -tlnp | grep 7687  # Should show 0.0.0.0:7687
```

---

## Expected Performance on VirtualBox

Based on VirtualBox overhead and your hardware:

| Phase            | Time (VM)     | vs Windows | vs Cloud Linux |
| ---------------- | ------------- | ---------- | -------------- |
| Data loading     | 60-90s        | ∞ (failed) | ~45s           |
| Transaction prep | 30-45s        | N/A        | ~30s           |
| FP-Growth mining | 6-10 min      | N/A        | 5-8 min        |
| Validation       | 2-3 min       | N/A        | 2-3 min        |
| **TOTAL**        | **10-15 min** | **∞**      | **8-12 min**   |

**Key Difference**: It will actually **complete successfully** vs failing on Windows.

---

## Advantages of This Setup

✅ **Complete control** - full Linux environment  
✅ **Free** - no cloud costs  
✅ **Reproducible** - can snapshot VM state  
✅ **Portable** - can share VM with others  
✅ **No network issues** - everything local  
✅ **Learn Linux** - valuable skill for production deployments

## Disadvantages

⚠️ **Slower** than native Linux (10-20% overhead)  
⚠️ **Uses your RAM** - need 12GB free  
⚠️ **Initial setup time** - 2-3 hours first time  
⚠️ **Shared resources** - competes with Windows

---

## Quick Start Checklist

- [ ] Download Ubuntu ISO (2GB)
- [ ] Create VM: 12GB RAM, 4 CPU, 80GB disk
- [ ] Install Ubuntu Server/Desktop
- [ ] Install Java 11, Python 3.11, Neo4j 4.4
- [ ] Transfer your data (csv_data folder)
- [ ] Load data into Neo4j
- [ ] Install PySpark 3.5.3
- [ ] Remove test mode limit from script
- [ ] Run: `python scripts/spark_neo4j_pattern_mining.py`
- [ ] Wait 10-15 minutes for results!
- [ ] Copy `spark_neo4j_patterns.json` back to Windows

---

## Alternative: Save Time with Pre-built VM

I can create a setup script that automates steps 2-5. Would you like me to create:

1. **Automated setup script** (bash) - runs most steps automatically
2. **Docker Compose** alternative - faster setup, similar isolation
3. **Cloud VM guide** (Azure/AWS) - if you change your mind

Let me know if you want any of these or if you want to proceed with manual VirtualBox setup!
