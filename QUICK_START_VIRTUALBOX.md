# Quick Start: VirtualBox Ubuntu Setup

## Step-by-Step Instructions (2-3 hours total)

### Phase 1: Create the VM (30 minutes)

1. **Download Ubuntu ISO**

   - Go to: https://ubuntu.com/download/server
   - Download: Ubuntu 22.04.5 LTS Server (~2GB)
   - Or Desktop if you prefer GUI: https://ubuntu.com/download/desktop

2. **Create VM in VirtualBox**

   - Open VirtualBox → Click **"New"**
   - Name: `Neo4j-Spark-Testing`
   - Type: `Linux`, Version: `Ubuntu (64-bit)`
   - Memory: **12288 MB (12GB)**
   - Hard Disk: Create new, VDI, **80GB**, Dynamically allocated
   - Click **Create**

3. **Configure VM Settings** (Right-click VM → Settings)

   - **System** → Processor: **4 cores**
   - **Storage** → Empty CD → Click CD icon → Choose Ubuntu ISO
   - **Network** → Adapter 1: **Bridged Adapter**

4. **Install Ubuntu**
   - Start VM
   - Follow installer (all defaults OK)
   - Profile: username `spark`, password of your choice
   - Install OpenSSH server: ✓ (optional but helpful)
   - Wait 10-15 minutes for installation
   - Reboot when prompted

### Phase 2: Run Automated Setup (30 minutes)

5. **Login to Ubuntu VM**

   - Username: `spark` (or what you chose)
   - Password: (your password)

6. **Install VirtualBox Guest Additions** (for shared folders)

   ```bash
   sudo apt update
   sudo apt install virtualbox-guest-utils virtualbox-guest-dkms -y
   sudo reboot
   ```

   Wait for reboot, login again.

7. **Setup Shared Folder in VirtualBox**

   - VM window → Devices → Shared Folders → Shared Folders Settings
   - Click folder+ icon
   - Folder Path: `C:\Users\Brian\source\repos\Project`
   - Folder Name: `project`
   - Auto-mount: ✓
   - Make Permanent: ✓
   - Click OK

8. **Mount Shared Folder in Ubuntu**

   ```bash
   sudo usermod -aG vboxsf spark
   sudo mkdir -p /home/spark/project
   sudo reboot
   ```

   After reboot, verify:

   ```bash
   ls /home/spark/project
   # Should see: csv_data, scripts, files, etc.
   ```

9. **Transfer Setup Script to VM**

   In Ubuntu VM:

   ```bash
   cp /home/spark/project/setup_ubuntu_vm.sh ~/
   chmod +x ~/setup_ubuntu_vm.sh
   ```

10. **Run Automated Setup**

    ```bash
    bash ~/setup_ubuntu_vm.sh
    ```

    This will automatically install:

    - Java 11
    - Python 3.11
    - Neo4j 4.4
    - PySpark 3.5.3
    - All dependencies

    Takes 20-30 minutes. **Go get coffee!** ☕

### Phase 3: Load Data (15-20 minutes)

11. **Load Data into Neo4j**

    ```bash
    cd ~/neo4j-spark-testing
    source venv/bin/activate
    python load_data.py /home/spark/project/csv_data
    ```

    Or use your existing loader:

    ```bash
    python /home/spark/project/scripts/load_full_dataset.py
    ```

    Takes 15-20 minutes for 7.6M reviews.

12. **Verify Data Loaded**

    ```bash
    python test_neo4j.py
    ```

    Should show:

    ```
    ✓ Neo4j connection successful
    ✓ Customers: 1,555,170
    ✓ Products: 548,552
    ✓ Reviews: 7,593,244
    ✓ Database ready for Spark testing!
    ```

### Phase 4: Run Full Dataset Test! 🚀 (10-15 minutes)

13. **Copy Your Script**

    ```bash
    cp /home/spark/project/scripts/spark_neo4j_pattern_mining.py ~/neo4j-spark-testing/
    ```

14. **Remove Test Mode Limit**

    ```bash
    cd ~/neo4j-spark-testing
    nano spark_neo4j_pattern_mining.py
    ```

    Find line ~63 that says:

    ```python
    # TESTING MODE: Limit to first 10,000 reviews
    reviews_df = reviews_df.limit(10000)
    ```

    Comment it out or delete it:

    ```python
    # FULL DATASET MODE - run on all 7.6M reviews
    # reviews_df = reviews_df.limit(10000)  # Removed for full test
    ```

    Save: Ctrl+O, Enter, Ctrl+X

15. **RUN THE FULL DATASET TEST!**

    ```bash
    source venv/bin/activate
    python spark_neo4j_pattern_mining.py
    ```

    **Expected output:**

    ```
    ======================================================================
    CO-PURCHASE PATTERN MINING WITH NEO4J CONNECTOR FOR SPARK
    ======================================================================
    Initializing Spark with Neo4j connector...
    Spark version: 3.5.3
    Connected to Neo4j at bolt://localhost:7687

    Loading reviews from Neo4j using Spark connector...
    ✓ Loaded 7,593,244 reviews in 60-90s
      Throughput: 85,000-126,000 reviews/sec

    Creating customer transactions...
    ✓ Created 1,088,619 customer transactions

    Running FP-Growth algorithm...
    [Progress bars showing Spark jobs...]

    ✓ Pattern mining complete in 6-10 minutes
    ✓ Found XX,XXX frequent patterns

    TOTAL RUNTIME: 10-15 minutes
    ```

16. **Copy Results Back to Windows**

    Results are in: `spark_neo4j_patterns.json`

    Via shared folder (automatic):

    ```bash
    cp spark_neo4j_patterns.json /home/spark/project/
    ```

    File is now in: `C:\Users\Brian\source\repos\Project\` on Windows!

### Phase 5: Analyze Results (5 minutes)

17. **View Results on Windows**

    Open PowerShell:

    ```powershell
    cd C:\Users\Brian\source\repos\Project

    # View summary
    Get-Content spark_neo4j_patterns.json | ConvertFrom-Json | Select-Object -ExpandProperty summary

    # Count patterns
    (Get-Content spark_neo4j_patterns.json | ConvertFrom-Json).patterns.Count

    # View top 10 patterns
    (Get-Content spark_neo4j_patterns.json | ConvertFrom-Json).patterns | Select-Object -First 10
    ```

18. **Update Your Milestone 4 Report**

    Now you have **ACTUAL measurements**:

    - ✅ Real loading time (not estimates)
    - ✅ Real pattern mining time
    - ✅ Real pattern counts
    - ✅ Proof of 50-100x speedup vs Python driver
    - ✅ Publication-quality benchmark data

---

## Troubleshooting

### VM Can't Access Shared Folder

```bash
# Check if mounted
mount | grep vboxsf

# Manual mount
sudo mount -t vboxsf project /home/spark/project

# Verify Guest Additions installed
lsmod | grep vboxguest
```

### Neo4j Won't Start

```bash
# Check status
sudo systemctl status neo4j

# View logs
sudo journalctl -u neo4j -n 50
sudo tail -f /var/log/neo4j/neo4j.log

# Restart
sudo systemctl restart neo4j
```

### Out of Memory During Spark

```bash
# Edit script to reduce memory
nano spark_neo4j_pattern_mining.py

# Change line ~21:
.config("spark.driver.memory", "4g")  # Instead of 6g
.config("spark.executor.memory", "4g")  # Instead of 6g
```

### Can't Connect from Windows

```bash
# Find VM IP
ip addr show

# Test from Windows PowerShell
Test-NetConnection -ComputerName <VM-IP> -Port 7687

# Allow firewall (in VM)
sudo ufw allow 7474
sudo ufw allow 7687
```

---

## Time Breakdown

| Phase               | Time        | Can Skip? |
| ------------------- | ----------- | --------- |
| Download Ubuntu ISO | 20 min      | No        |
| Create & Install VM | 30 min      | No        |
| Guest Additions     | 10 min      | Yes\*     |
| Run setup script    | 30 min      | No        |
| Load data           | 20 min      | No        |
| Run test            | 15 min      | No        |
| **TOTAL**           | **2h 5min** |           |

\*Skip Guest Additions if using SCP instead of shared folders

---

## Alternative: Skip Shared Folders

If Guest Additions don't work, use SCP:

1. **Find VM IP in Ubuntu:**

   ```bash
   ip addr show | grep inet
   ```

2. **From Windows PowerShell:**

   ```powershell
   # Transfer data
   scp -r C:\Users\Brian\source\repos\Project\csv_data spark@<VM-IP>:~/
   scp C:\Users\Brian\source\repos\Project\setup_ubuntu_vm.sh spark@<VM-IP>:~/
   scp C:\Users\Brian\source\repos\Project\scripts\spark_neo4j_pattern_mining.py spark@<VM-IP>:~/

   # Get results back
   scp spark@<VM-IP>:~/neo4j-spark-testing/spark_neo4j_patterns.json .
   ```

---

## Success Criteria

After completion, you should have:

- ✅ VM running Ubuntu 22.04
- ✅ Neo4j with 7.6M reviews loaded
- ✅ Spark 3.5.3 with Neo4j Connector
- ✅ `spark_neo4j_patterns.json` with real results
- ✅ Actual timing data for your report
- ✅ Proof your implementation works on full dataset

---

## Quick Reference

### VM Credentials

- Username: `spark` (or what you set)
- Password: (your choice)

### Neo4j Credentials

- URL: `bolt://localhost:7687`
- Username: `neo4j`
- Password: `Password`

### Important Paths

- Shared folder: `/home/spark/project`
- Working dir: `~/neo4j-spark-testing`
- Python env: `source ~/neo4j-spark-testing/venv/bin/activate`

### Key Commands

```bash
# Neo4j status
sudo systemctl status neo4j

# Test Neo4j
python test_neo4j.py

# Activate Python
source ~/neo4j-spark-testing/venv/bin/activate

# Run Spark test
python spark_neo4j_pattern_mining.py

# Copy results
cp spark_neo4j_patterns.json /home/spark/project/
```

---

**Ready to start? Follow Phase 1 above!**

Questions? Check `VIRTUALBOX_SETUP_GUIDE.md` for detailed explanations.
