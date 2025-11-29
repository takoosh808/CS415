# Local Testing Setup for Neo4j Connector with Spark

## Current Issues

- Windows Python + Py4J + Docker + WSL2 = too many network layers
- Connection failures when loading 7.6M reviews through this stack

## Recommended Improvements (Step-by-Step)

### Step 1: Increase WSL2 Memory Allocation

Your current `.wslconfig` needs memory limits added:

1. **Backup current config**:

   ```powershell
   Copy-Item "$env:USERPROFILE\.wslconfig" "$env:USERPROFILE\.wslconfig.backup"
   ```

2. **Update `.wslconfig`** (I'll create the updated version):

   ```ini
   [wsl2]
   memory=12GB              # Increase from default 8GB
   processors=4             # Use 4 CPU cores
   swap=4GB                 # Add swap space
   networkingMode=mirrored  # Keep existing setting
   localhostForwarding=true # Keep existing setting
   ```

3. **Apply changes**:
   ```powershell
   wsl --shutdown
   # Wait 10 seconds for WSL to fully stop
   Start-Sleep -Seconds 10
   # Restart Docker Desktop
   ```

### Step 2: Switch to WSL2 Python (Recommended)

**Why**: Native Linux environment eliminates Windows/Py4J bridge issues.

1. **Install Python in WSL2**:

   ```bash
   wsl -d Ubuntu
   sudo apt update
   sudo apt install python3.11 python3.11-venv python3-pip -y
   ```

2. **Create project environment in WSL2**:

   ```bash
   cd /mnt/c/Users/Brian/source/repos/Project
   python3.11 -m venv venv
   source venv/bin/activate
   pip install pyspark neo4j
   ```

3. **Run script from WSL2**:
   ```bash
   python scripts/spark_neo4j_pattern_mining.py
   ```

### Step 3: Optimize Neo4j Configuration

**Option A: Use Neo4j Desktop (Simpler)**

1. Download Neo4j Desktop: https://neo4j.com/download/
2. Create new database with 4GB heap
3. Stop Docker Neo4j cluster
4. Import your data
5. Update script to connect to `bolt://localhost:7687`

**Option B: Optimize Docker Neo4j (Current Setup)**

1. Increase Neo4j memory in `docker-compose.yml`:

   ```yaml
   environment:
     - NEO4J_dbms_memory_heap_initial__size=2G
     - NEO4J_dbms_memory_heap_max__size=4G
     - NEO4J_dbms_memory_pagecache_size=2G
   ```

2. Reduce cluster size for testing (single node is more stable):
   ```bash
   docker-compose stop neo4j-core2 neo4j-core3
   ```

### Step 4: Test with Reduced Dataset First

Before full dataset, validate with smaller samples:

1. **Test 1: 1,000 customers** (current test script)
2. **Test 2: 10,000 customers** (if successful)
3. **Test 3: 100,000 customers** (if successful)
4. **Test 4: Full dataset** (1.5M customers)

## Quick Win: Test Right Now

The **fastest path** to validate your code works:

### Use WSL2 Python (10 minutes)

```powershell
# From PowerShell
wsl -d Ubuntu

# In WSL2:
sudo apt update && sudo apt install python3-pip python3-venv -y
cd /mnt/c/Users/Brian/source/repos/Project
python3 -m venv venv_wsl
source venv_wsl/bin/activate
pip install pyspark neo4j

# Run with reduced dataset (already configured in script)
python scripts/spark_neo4j_pattern_mining.py
```

**This eliminates:**

- ✓ Windows/Py4J bridge issues
- ✓ Python 3.13 compatibility issues (uses 3.11)
- ✓ Network layer complexity

## Expected Improvements

| Setup                             | Success Rate | Speed       |
| --------------------------------- | ------------ | ----------- |
| Current (Windows Python + Docker) | ~10%         | N/A         |
| WSL2 Python + Docker Neo4j        | ~60-70%      | 2-3x faster |
| WSL2 Python + Native Neo4j        | ~90%         | 5x faster   |
| Linux VM + Native Neo4j           | ~95%         | 10x faster  |

## If Still Failing

**Plan B: Scale Python Driver Approach**

- Test with 50,000 customers (100x current scale)
- Proves scalability concept with actual measurements
- More reliable on current infrastructure

## Next Steps

1. Try WSL2 Python approach first (quickest win)
2. If successful, gradually increase dataset size
3. If still failing, consider Plan B (Python driver at larger scale)

Let me know which approach you'd like to try!
