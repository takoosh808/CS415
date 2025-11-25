# Milestone 4 - Complete Setup and Launch Script
# This PowerShell script sets up and runs the entire application

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "MILESTONE 4 - Amazon Co-Purchasing Analytics GUI" -ForegroundColor Cyan
Write-Host "Team Baddie" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check Python
Write-Host "[1/5] Checking Python installation..." -ForegroundColor Yellow
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Python not found!" -ForegroundColor Red
    Write-Host "Please install Python 3.9+ from python.org" -ForegroundColor Red
    exit 1
}
Write-Host "  Found: $pythonVersion" -ForegroundColor Green
Write-Host ""

# Step 2: Check/Install Dependencies
Write-Host "[2/5] Checking dependencies..." -ForegroundColor Yellow
$packages = @('neo4j', 'matplotlib', 'numpy', 'pandas')
$missing = @()

foreach ($package in $packages) {
    $result = python -c "import $package" 2>&1
    if ($LASTEXITCODE -ne 0) {
        $missing += $package
        Write-Host "  Missing: $package" -ForegroundColor Red
    } else {
        Write-Host "  Found: $package" -ForegroundColor Green
    }
}

if ($missing.Count -gt 0) {
    Write-Host ""
    Write-Host "Installing missing packages..." -ForegroundColor Yellow
    pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to install dependencies" -ForegroundColor Red
        exit 1
    }
}
Write-Host ""

# Step 3: Check Neo4j
Write-Host "[3/5] Checking Neo4j database..." -ForegroundColor Yellow
python scripts\check_db_status.py
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: Neo4j not accessible" -ForegroundColor Red
    Write-Host "Please ensure:" -ForegroundColor Yellow
    Write-Host "  1. Neo4j is running on localhost:7687" -ForegroundColor Yellow
    Write-Host "  2. Credentials are neo4j/Password" -ForegroundColor Yellow
    Write-Host "  3. Data is loaded (run: python scripts\stream_load_full.py)" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 4: Generate Results
Write-Host "[4/5] Checking pre-generated results..." -ForegroundColor Yellow
if (Test-Path "gui_results.json") {
    $fileSize = (Get-Item "gui_results.json").Length / 1KB
    Write-Host "  Results file exists ($([math]::Round($fileSize, 1)) KB)" -ForegroundColor Green
    Write-Host ""
    
    $response = Read-Host "Re-generate results? (y/N)"
    if ($response -eq 'y' -or $response -eq 'Y') {
        Write-Host ""
        Write-Host "Generating results (1-2 minutes)..." -ForegroundColor Yellow
        python scripts\pregenerate_results.py
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERROR: Failed to generate results" -ForegroundColor Red
            exit 1
        }
    }
} else {
    Write-Host "  Results file not found. Generating..." -ForegroundColor Yellow
    python scripts\pregenerate_results.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to generate results" -ForegroundColor Red
        exit 1
    }
}
Write-Host ""

# Step 5: Launch GUI
Write-Host "[5/5] Launching GUI application..." -ForegroundColor Yellow
Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "Starting Amazon Co-Purchasing Analytics GUI" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "The application window should open shortly..." -ForegroundColor Green
Write-Host "Press Ctrl+C to exit" -ForegroundColor Gray
Write-Host ""

python gui_app.py

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "Application closed" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
