# Neo4j Bulk Import Instructions

## Step 1: Convert Data to CSV

Run the conversion script to create CSV files:

```powershell
python scripts\convert_to_csv.py --file amazon-meta.txt --output csv_data
```

This will create CSV files in the `csv_data` directory:
- products.csv
- customers.csv
- categories.csv
- reviews.csv (REVIEWED relationships)
- similar.csv (SIMILAR_TO relationships)
- belongs_to.csv (BELONGS_TO relationships)

## Step 2: Stop Neo4j

1. Open Neo4j Desktop
2. Stop your DBMS (amazon-analysis database)

## Step 3: Delete Old Database (if exists)

In Neo4j Desktop:
1. Select your DBMS
2. In the databases list, find `amazon-analysis`
3. Delete it (three dots → Delete)

## Step 4: Run Bulk Import

Find your Neo4j installation path and run the import command.

### Find Neo4j Path:
In Neo4j Desktop, click on your DBMS, then "Open Folder" → "DBMS"
This will show you the path, typically like:
`C:\Users\Brian\.Neo4jDesktop\dbmss-xx\dbms-xxxxx\`

### Run Import Command:

Replace `<NEO4J_PATH>` with your actual Neo4j installation path:

```powershell
cd "<NEO4J_PATH>\bin"

.\neo4j-admin.bat database import full `
  --nodes=Product="C:\Users\Brian\source\repos\Project\csv_data\products.csv" `
  --nodes=Customer="C:\Users\Brian\source\repos\Project\csv_data\customers.csv" `
  --nodes=Category="C:\Users\Brian\source\repos\Project\csv_data\categories.csv" `
  --relationships=SIMILAR_TO="C:\Users\Brian\source\repos\Project\csv_data\similar.csv" `
  --relationships=BELONGS_TO="C:\Users\Brian\source\repos\Project\csv_data\belongs_to.csv" `
  --relationships=REVIEWED="C:\Users\Brian\source\repos\Project\csv_data\reviews.csv" `
  --skip-bad-relationships=true `
  --skip-duplicate-nodes=true `
  amazon-analysis
```

**Note:** Use absolute paths for CSV files.

## Step 5: Start Neo4j

1. Start your DBMS in Neo4j Desktop
2. The `amazon-analysis` database should now have all the data loaded

## Step 6: Verify

```powershell
python scripts\check_db_status.py
```

## Expected Time

- CSV conversion: 5-15 minutes (548K products)
- Bulk import: 10-30 minutes (vs 6 days with Python script!)
- **Total: 15-45 minutes**

## Troubleshooting

If import fails:
1. Make sure Neo4j is completely stopped
2. Check that CSV file paths are correct (use absolute paths)
3. Ensure no other database named `amazon-analysis` exists
4. Check Neo4j logs in `<NEO4J_PATH>\logs\`

## Alternative: Use 10MB Dataset First

To test the bulk import process with the smaller dataset first:

```powershell
python scripts\convert_to_csv.py --file amazon-meta-10mb.txt --output csv_data_10mb
```

Then use the same import command but replace `csv_data` with `csv_data_10mb`.
