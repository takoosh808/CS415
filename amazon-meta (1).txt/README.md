# Amazon Metadata Neo4j Processing - Setup Instructions

## Prerequisites

1. **Neo4j Database Setup**

   - Download and install Neo4j Desktop from https://neo4j.com/download/
   - Create a new database project
   - Set username: `neo4j`, password: `Password`
   - Start the database on default port 7687
   - Alternatively, use Neo4j Community Edition

2. **Python Environment**
   - Python 3.8 or higher
   - pip package manager

## Installation Steps

1. **Install Python Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Verify Neo4j Connection**
   - Ensure Neo4j is running on `bolt://localhost:7687`
   - Test connection using Neo4j Browser or Desktop

## Execution

1. **Run Complete Pipeline**

   ```bash
   python run_complete_pipeline.py
   ```

   This will:

   - Process the original `amazon-meta.txt` file
   - Reduce data to 50,000 products (~100-200MB)
   - Clean and transform the data
   - Create Neo4j schema with constraints and indexes
   - Ingest data into Neo4j
   - Run validation queries
   - Generate performance metrics

2. **Individual Steps** (optional)

   ```bash
   # Data preparation only
   python data_preparation.py

   # Neo4j ingestion only (requires processed data)
   python neo4j_ingestion.py
   ```

## File Structure

```
project/
├── amazon-meta.txt                 # Original dataset (977MB)
├── data_preparation.py            # Data processing pipeline
├── neo4j_ingestion.py             # Neo4j schema and ingestion
├── run_complete_pipeline.py       # Complete automation script
├── requirements.txt               # Python dependencies
├── processed_amazon_data.json     # Cleaned data (generated)
├── validation_queries.json        # Validation queries (generated)
└── README.md                      # This file
```

## Expected Output

- **Processed Data**: ~100-200MB JSON file with 50,000 products
- **Neo4j Database**: Populated with Product, Category, and Customer nodes
- **Relationships**: SIMILAR_TO, BELONGS_TO, REVIEWED
- **Performance Metrics**: Query execution times and validation results

## Troubleshooting

1. **Neo4j Connection Issues**

   - Verify Neo4j is running: Check Neo4j Desktop/Browser
   - Check credentials: Default is neo4j/Password
   - Port conflicts: Ensure 7687 is available

2. **Memory Issues**

   - Reduce `max_products` in `data_preparation.py`
   - Increase Neo4j heap size in neo4j.conf

3. **Performance Issues**
   - Adjust `BATCH_SIZE` in `neo4j_ingestion.py`
   - Monitor system resources during ingestion

## Data Schema

### Nodes

- **Product**: Core product information (ASIN, title, group, ratings)
- **Category**: Product categories and hierarchies
- **Customer**: Individual customers who wrote reviews

### Relationships

- **SIMILAR_TO**: Product similarity recommendations
- **BELONGS_TO**: Product categorization
- **REVIEWED**: Customer product reviews with ratings

This schema supports complex queries for product recommendations,
category analysis, and customer behavior patterns.
