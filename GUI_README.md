# Amazon Co-Purchasing Analysis - GUI Application

## Overview

This GUI application provides an interactive interface for the Amazon co-purchasing analysis system. It demonstrates the two main functionalities required by the project:

1. **Complex Query Execution**: Query products using searchable and non-searchable attributes with enriched operators
2. **Pattern Mining**: Discover and validate frequent co-purchasing patterns using train/test data splits

## Features

### 1. Complex Queries Tab

- **Fully Flexible Query Builder**: Type any values - no restrictions!
- **All Fields Typeable**: Product group accepts any category (not limited to dropdown)
- **Min/Max Support**: Set range bounds for rating, reviews, and sales rank
- **Searchable Attributes**: Product group, sales rank (both typeable)
- **Non-Searchable Attributes**: Number of reviews, average rating (used as filters only)
- **Enriched Operators**: Support for >=, <=, = operators (typed directly)
- **Result Limiting**: Specify top-k results
- **Clear All Button**: Reset all filters with one click
- **Input Validation**: Helpful error messages guide correct input
- **Performance Metrics**: Display query execution time

### 2. Pattern Mining Tab

- **Configurable Parameters**: Set minimum support and maximum customers
- **Train/Test Validation**: Patterns mined from training set are validated in test set
- **Visual Comparison**: Dual bar charts showing:
  - Total support (combined train + test)
  - Confidence scores with target threshold line
- **Product Names**: Display actual product titles instead of ASINs
- **Significant Pattern Identification**: Automatically identifies most significant patterns
- **Customer Discovery**: Find customers matching frequent patterns

### 3. Database Statistics Tab

- **Overall Counts**: Products, customers, reviews, relationships
- **Product Distribution**: Breakdown by product group (Book, Music, DVD, Video)
- **Rating Distribution**: Distribution of average ratings
- **Train/Test Split Info**: Shows dataset split statistics

## Requirements

```
neo4j>=5.0.0
matplotlib>=3.5.0
```

Standard library: `tkinter` (included with Python on Windows)

## Running the Application

### Step 1: Pre-generate Results (Recommended)

For the best demonstration experience, pre-generate pattern mining results:

```bash
python pregenerate_gui_results.py
```

This creates three JSON files containing pre-computed results:

- `gui_patterns.json` - Pattern mining results (train/test validation)
- `gui_stats.json` - Database statistics
- `gui_queries.json` - Sample query results

**Time**: 30-60 seconds (one-time setup)

**Benefits**:

- Pattern results load instantly during demo
- No lag during presentations
- Consistent, reproducible results
- Reduced database load

### Step 2: Launch the GUI

1. **Ensure Neo4j is running** (optional if using pre-generated results):

   ```bash
   # Default connection: bolt://localhost:7687
   # Username: neo4j
   # Password: Password
   ```

2. **Run the GUI**:

   ```bash
   python gui_app.py
   ```

3. **Connection Status**: Check the status indicator in the top-right corner

**Note**: The application will work with or without database connection if pre-generated results exist.

## Usage Guide

### Executing Complex Queries

1. Navigate to the **Complex Queries** tab
2. Set query parameters:
   - **Product Group**: Filter by Book, Music, DVD, Video, or Any
   - **Min Rating**: Minimum average rating (0-5)
   - **Min Reviews**: Minimum number of reviews
   - **Max Sales Rank**: Maximum sales rank (0 = no filter)
   - **Limit (k)**: Maximum number of results to return
3. Click **Execute Query**
4. Results display in the table with execution time shown below

**Example Queries**:

- High-rated DVDs: Group=DVD, Min Rating=4.5, Min Reviews=50
- Popular Books: Group=Book, Min Reviews=100
- Top Products: Min Rating=4.8, Min Reviews=20

### Running Pattern Mining

The application provides **two modes** for pattern mining:

#### Mode 1: Pre-Generated Results (Recommended for Demos)

1. Navigate to the **Pattern Mining** tab
2. Click **Load Pre-Generated Results**
3. Pre-generated patterns load instantly
4. Perfect for presentations and consistent results

#### Mode 2: Real-Time Mining (Live Computation)

1. Navigate to the **Pattern Mining** tab
2. Set parameters:
   - **Min Support**: Minimum number of customers who purchased both products (5-100)
   - **Max Customers**: Maximum customers to analyze (100-5000)
3. Click **Mine Patterns Now (Real-Time)**
4. Wait 30-60 seconds for mining to complete
5. See live computation with current parameters

**Results Display** (both modes):

- **Table**: Lists patterns ranked by test set support
- **Chart**: Visualizes train vs test support for top 10 patterns
- **Info**: Shows most significant pattern

#### Live Pattern Mining (If Pre-Generated Results Not Available)

1. Navigate to the **Pattern Mining** tab
2. Application will prompt to generate results
3. Click "Yes" to compute patterns (takes 30-60 seconds)
4. Results display same as pre-generated mode

**Note**: Parameters (Min Support, Max Customers) are informational only when using pre-generated results

### Viewing Statistics

1. Navigate to the **Database Statistics** tab
2. Click **Refresh Statistics**
3. View comprehensive database metrics

## Implementation Details

### Query Processing

- Uses Cypher queries against Neo4j database
- Non-SQL implementation as required
- Optimized with indexed lookups and efficient filtering
- Returns top-k results sorted by rating and review count

### Pattern Mining Algorithm

1. **Data Split**: Reviews divided into 70% training, 30% testing
2. **Frequent Itemsets**: Apriori-based algorithm finds frequent product pairs
3. **Pattern Validation**: Training patterns validated against test set
4. **Confidence Calculation**: Test support / Train support ratio
5. **Customer Matching**: Identifies customers exhibiting patterns

### Performance Considerations

- Batch processing for large datasets
- Parameterized queries to prevent injection
- Lazy loading of results
- Efficient graph traversals using Neo4j relationships

## Architecture

```
gui_app.py
├── AmazonAnalysisGUI (Main Application)
│   ├── setup_query_tab()      - Complex query interface
│   ├── setup_pattern_tab()    - Pattern mining interface
│   ├── setup_stats_tab()      - Statistics viewer
│   ├── execute_query()        - Query execution engine
│   ├── run_pattern_mining()   - Pattern mining engine
│   └── visualize_patterns()   - Matplotlib visualization
```

## Data Visualization

The application includes:

- **Tabular Results**: Sortable tables for queries and patterns
- **Bar Charts**: Compare train vs test pattern frequencies
- **Statistical Summaries**: Formatted text displays with distributions

## Error Handling

- **Database Offline**: Application runs with limited functionality if Neo4j is unavailable
- **Invalid Parameters**: Validation before query execution
- **Empty Results**: Clear messaging when no results found
- **Exception Handling**: User-friendly error messages for all operations

## Demonstration Scenarios

### Scenario 1: High-Quality Product Discovery

```
Query: Group=DVD, Min Rating=4.5, Min Reviews=30, Limit=20
Result: Top-rated DVDs with substantial customer validation
```

### Scenario 2: Popular Products

```
Query: Min Reviews=100, Limit=50
Result: Most-reviewed products across all categories
```

### Scenario 3: Co-Purchasing Patterns

```
Pattern Mining: Min Support=10, Max Customers=500
Result: Product pairs frequently purchased together, validated across train/test split
```

## Scalability Notes

- GUI connects to single or clustered Neo4j instances
- Pattern mining parameters can be adjusted based on dataset size
- Larger datasets may require higher min_support values
- Max_customers parameter controls memory usage

## Future Enhancements

Possible extensions:

- Export results to CSV/JSON
- Advanced visualization (network graphs for co-purchasing)
- Query history and saved queries
- Real-time pattern monitoring
- Multi-product pattern mining (3+ items)

## Troubleshooting

**Cannot connect to database**:

- Verify Neo4j is running: `docker ps` or Neo4j Desktop
- Check connection string in `gui_app.py` line 300
- Verify credentials (default: neo4j/Password)

**Slow pattern mining**:

- Reduce max_customers parameter
- Increase min_support parameter
- Ensure database indexes are created

**No results found**:

- Check if data is loaded in Neo4j
- Relax query conditions
- Verify train/test split exists for pattern mining

## Contact

For questions about this implementation, refer to the project documentation in the `files/` directory.
