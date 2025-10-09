"""
Zip Everything Up for Submission  
===============================

creates a zip file with all our code and docs for turning in
(because prof wants everything in one zip file)
"""

import zipfile
import os
from pathlib import Path
import datetime

def create_submission_package():
    """makes a zip with all our project stuff"""
    
    # add timestamp so we don't overwrite old submissions by accident  
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"CS415_Amazon_Project_{timestamp}.zip"
    
    # all the files prof wants to see
    files_to_include = [
        "data_preparation.py",        # data cleaning script
        "neo4j_ingestion.py",         # database setup 
        "validation_testing.py",      # test queries
        "run_complete_pipeline.py",   # main script that runs everything
        "requirements.txt",           # python packages needed
        "README.md",                  # setup instructions  
        "MILESTONE_2_REPORT.md"       # our write-up
    ]
    
    # Optional files (include if they exist)
    optional_files = [
        "processed_amazon_data.json",
        "validation_report.json", 
        "validation_queries.json"
    ]
    
    print(f"Creating submission package: {zip_filename}")
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add required files
        for filename in files_to_include:
            if Path(filename).exists():
                zipf.write(filename)
                print(f"Added: {filename}")
            else:
                print(f"Warning: {filename} not found")
        
        # Add optional files if they exist
        for filename in optional_files:
            if Path(filename).exists():
                zipf.write(filename)
                print(f"Added: {filename}")
    
    # Get zip file size
    zip_size = os.path.getsize(zip_filename) / (1024 * 1024)  # MB
    
    print(f"\nSubmission package created: {zip_filename}")
    print(f"Package size: {zip_size:.2f} MB")
    print("\nContents:")
    
    with zipfile.ZipFile(zip_filename, 'r') as zipf:
        for info in zipf.infolist():
            file_size = info.file_size / 1024  # KB
            print(f"  {info.filename}: {file_size:.1f} KB")
    
    print(f"\nReady for Milestone 2 submission!")
    return zip_filename

if __name__ == "__main__":
    create_submission_package()