"""
Quick Start Script for Milestone 4 GUI
This script checks prerequisites and launches the application
"""
import sys
import os
import subprocess

def check_python_version():
    """Check if Python version is adequate"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 9):
        print("❌ Python 3.9+ required")
        print(f"   Current version: {version.major}.{version.minor}.{version.micro}")
        return False
    print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
    return True

def check_dependencies():
    """Check if required packages are installed"""
    required = ['neo4j', 'matplotlib', 'tkinter']
    missing = []
    
    for package in required:
        try:
            if package == 'tkinter':
                import tkinter
            else:
                __import__(package)
            print(f"✓ {package} installed")
        except ImportError:
            missing.append(package)
            print(f"❌ {package} NOT installed")
    
    return len(missing) == 0, missing

def check_neo4j():
    """Check if Neo4j is accessible"""
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                     auth=("neo4j", "Password"))
        with driver.session(database="neo4j") as session:
            result = session.run("MATCH (p:Product) RETURN count(p) as count")
            count = result.single()['count']
            print(f"✓ Neo4j connected ({count:,} products)")
            driver.close()
            return True, count
    except Exception as e:
        print(f"❌ Neo4j connection failed: {str(e)}")
        return False, 0

def check_results_file():
    """Check if pre-generated results exist"""
    if os.path.exists('gui_results.json'):
        size = os.path.getsize('gui_results.json') / 1024
        print(f"✓ Results file exists ({size:.1f} KB)")
        return True
    else:
        print("❌ Results file not found (gui_results.json)")
        return False

def main():
    """Main entry point"""
    print("=" * 70)
    print("MILESTONE 4 - Quick Start Checker")
    print("Team Baddie - Amazon Co-Purchasing Analytics")
    print("=" * 70)
    print()
    
    # Check Python
    print("[1/4] Checking Python version...")
    if not check_python_version():
        sys.exit(1)
    print()
    
    # Check dependencies
    print("[2/4] Checking dependencies...")
    deps_ok, missing = check_dependencies()
    if not deps_ok:
        print()
        print("Missing packages detected. Install with:")
        print("  pip install -r requirements.txt")
        sys.exit(1)
    print()
    
    # Check Neo4j
    print("[3/4] Checking Neo4j database...")
    neo4j_ok, product_count = check_neo4j()
    if not neo4j_ok:
        print()
        print("Neo4j is not running or not accessible.")
        print("Solutions:")
        print("  1. Start Neo4j server")
        print("  2. Check connection: localhost:7687")
        print("  3. Verify credentials: neo4j/Password")
        print("  4. Load data: python scripts\\stream_load_full.py")
        sys.exit(1)
    
    if product_count == 0:
        print()
        print("⚠️  Database is empty. Load data first:")
        print("  python scripts\\stream_load_full.py")
        sys.exit(1)
    print()
    
    # Check results file
    print("[4/4] Checking pre-generated results...")
    results_exist = check_results_file()
    print()
    
    if not results_exist:
        print("=" * 70)
        print("RESULTS FILE NEEDED")
        print("=" * 70)
        print()
        print("The GUI requires pre-computed pattern mining results.")
        print()
        response = input("Generate results now? (y/n): ")
        
        if response.lower() == 'y':
            print()
            print("Generating results (this takes 1-2 minutes)...")
            print()
            try:
                subprocess.run([sys.executable, 'scripts/pregenerate_results.py'], 
                             check=True)
                print()
                print("✓ Results generated successfully!")
            except subprocess.CalledProcessError:
                print()
                print("❌ Result generation failed")
                sys.exit(1)
        else:
            print()
            print("Run this command manually:")
            print("  python scripts\\pregenerate_results.py")
            print()
            print("Then launch the GUI:")
            print("  python gui_app.py")
            sys.exit(0)
    
    # All checks passed
    print("=" * 70)
    print("✓ ALL CHECKS PASSED")
    print("=" * 70)
    print()
    print("Launching GUI application...")
    print()
    
    try:
        subprocess.run([sys.executable, 'gui_app.py'])
    except KeyboardInterrupt:
        print()
        print("Application closed by user.")
    except Exception as e:
        print(f"❌ Failed to launch GUI: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
