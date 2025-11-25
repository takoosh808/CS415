# Amazon Co-Purchasing Analytics - Milestone 4 GUI Application

## Overview

This is the complete end-to-end application for Milestone 4, featuring:

- 📊 Interactive GUI with Tkinter
- 🔍 Complex query interface with live Neo4j execution
- 🔗 Co-purchasing pattern mining results
- 📈 Data visualization with Matplotlib charts
- ⚡ Pre-computed results for fast loading

## Prerequisites

1. **Python 3.11+**
2. **Neo4j Database** running on `localhost:7687`
   - Username: `neo4j`
   - Password: `Password`
3. **Data loaded** in Neo4j (36,202 products)

## Installation

### Step 1: Install Python Dependencies

```powershell
pip install -r requirements.txt
```

### Step 2: Verify Neo4j is Running

```powershell
python scripts\check_db_status.py
```

Expected output:

```
Connected to Neo4j successfully!
Products: 36,202
Customers: 75,347
Reviews: 85,551
```

If data is not loaded, run:

```powershell
python scripts\stream_load_full.py
```

## Running the Application

### Quick Start (2 steps)

#### 1. Pre-generate Results

This step runs the pattern mining algorithms and saves results to JSON:

```powershell
python scripts\pregenerate_results.py
```

⏱️ **Time:** 1-2 minutes

**Output:**

- `gui_results.json` (contains pre-computed patterns, queries, and statistics)
- Console shows progress of statistics, queries, and pattern mining

#### 2. Launch GUI

```powershell
python gui_app.py
```

The application window opens immediately with all data loaded!

## Using the Application

### Tab 1: Dashboard 📊

**Overview statistics and visualizations**

- View total products, customers, reviews, and relationships
- Pie chart showing product category distribution
- Bar chart showing rating distribution

### Tab 2: Query Interface 🔍

**Execute complex queries with multiple conditions**

**Query Builder:**

- **Product Group:** Book, DVD, Music, or Video
- **Min Rating:** Minimum average rating (0-5)
- **Max Rating:** Maximum average rating (0-5)
- **Min Reviews:** Minimum number of reviews
- **Max Sales Rank:** Maximum sales rank (lower is better)
- **Results Limit (k):** Number of results to return

**Sample Queries:**
Click any sample query to instantly see results:

- High-rated DVDs
- Popular Books
- Top Music Albums
- Highest Rated Products
- Popular Video Products

**Example Custom Query:**

1. Select Group: `DVD`
2. Enter Min Rating: `4.5`
3. Enter Min Reviews: `50`
4. Enter k: `20`
5. Click "Execute Query"

Results appear in table with columns:

- ASIN (product identifier)
- Product Title
- Category
- Rating
- Number of Reviews
- Sales Rank

### Tab 3: Co-Purchasing Patterns 🔗

**Explore frequent itemset mining results**

**Pattern List:**

- Top 30 co-purchasing patterns sorted by total support
- Shows customer count and confidence percentage

**Pattern Details:**
Click any pattern to see:

- Product 1 and Product 2 titles and ASINs
- Ratings for both products
- Training set support (70% of data)
- Test set support (30% of data)
- Confidence metric (likelihood of co-purchase)
- Sample customers who purchased both products
- Individual customer ratings for each product

**Most Significant Pattern:**
Pattern #1 typically shows the strongest co-purchasing relationship with highest confidence.

### Tab 4: Visualizations 📈

**Three sub-tabs with analytical charts:**

1. **Pattern Support**

   - Dual bar chart comparing training vs test support
   - Top 20 patterns
   - Validates pattern consistency across datasets

2. **Top Products**

   - Dual-axis chart: Rating vs Reviews
   - Top 15 highest-rated products
   - Shows correlation between ratings and review count

3. **Rating Analysis**
   - Pattern confidence distribution (histogram)
   - Product ratings in co-purchased pairs (histogram)
   - Statistical insights

## Application Features

### Interactive Elements

✅ **Live Query Execution**

- Queries execute against Neo4j in real-time
- Results appear in <2 seconds

✅ **Pre-computed Patterns**

- Pattern mining results loaded from JSON
- Instant access without re-computation

✅ **Data Visualization**

- Matplotlib charts embedded in GUI
- Pie charts, bar charts, histograms, dual-axis charts

✅ **Result Tables**

- Sortable columns
- Scrollable for large result sets
- Formatted numbers with commas

✅ **Status Bar**

- Real-time feedback on operations
- Query execution time display

### Supported Operators

The query interface supports all enriched operators:

- `>=` (greater than or equal)
- `<=` (less than or equal)
- `=` (equal)
- `>` and `<` (implied through min/max fields)

### Error Handling

- Invalid inputs show error messages
- Empty queries prompt for at least one condition
- Database connection failures handled gracefully
- Missing data files show informative messages

## Architecture

```
┌─────────────┐
│   Tkinter   │  ← GUI Framework
│     GUI     │
└──────┬──────┘
       │
       ├─────→ ┌────────────┐
       │       │ Matplotlib │  ← Data Visualization
       │       └────────────┘
       │
       ├─────→ ┌────────────┐
       │       │   Neo4j    │  ← Live Query Execution
       │       │  Database  │
       │       └────────────┘
       │
       └─────→ ┌────────────┐
               │    JSON    │  ← Pre-computed Results
               │ gui_results│
               └────────────┘
```

## File Structure

```
Project/
├── gui_app.py                      # Main GUI application (1,045 lines)
├── gui_results.json                # Pre-computed results (auto-generated)
├── scripts/
│   ├── pregenerate_results.py     # Generate results for GUI (578 lines)
│   ├── run_algorithms_full.py     # Query algorithm
│   ├── copurchase_pattern_mining.py  # Pattern mining algorithm
│   └── check_db_status.py         # Database health check
├── requirements.txt                # Python dependencies
└── files/
    ├── milestone4_report.txt      # Comprehensive report
    └── README_MILESTONE4.md       # This file
```

## Performance

### Execution Times

| Operation            | Time          |
| -------------------- | ------------- |
| Pre-generate results | 60-90 seconds |
| GUI startup          | 1-2 seconds   |
| Query execution      | 1-2 seconds   |
| Pattern selection    | <0.1 seconds  |
| Chart rendering      | 0.5-1 seconds |

### Data Size

- Products: 36,202
- Customers: 75,347
- Reviews: 85,551
- Pre-computed patterns: 30
- Sample queries: 5
- JSON file size: ~250 KB

## Troubleshooting

### Issue: "No results loaded"

**Solution:** Run pregenerate_results.py first

```powershell
python scripts\pregenerate_results.py
```

### Issue: "Neo4j connection not available"

**Solutions:**

1. Check Neo4j is running: `http://localhost:7474`
2. Verify credentials in code (neo4j/Password)
3. Check database contains data:
   ```powershell
   python scripts\check_db_status.py
   ```

### Issue: Matplotlib charts not showing

**Solution:** Install tkinter support

```powershell
pip install matplotlib --upgrade
```

### Issue: GUI window too small

**Solution:** Maximize the window or change geometry in gui_app.py line 27:

```python
self.root.geometry("1400x900")  # Increase dimensions
```

### Issue: Query returns no results

**Possible causes:**

- Conditions too restrictive (try loosening filters)
- Empty database (reload data)
- Group name case-sensitive (use: Book, DVD, Music, Video)

## Demo Workflow

**For presentation/demonstration:**

1. **Start with Dashboard**

   - Show overall statistics
   - Explain data distribution charts

2. **Execute Sample Query**

   - Click "High-rated DVDs" sample query
   - Results appear in table
   - Point out execution time

3. **Execute Custom Query**

   - Fill in: Group=Music, Min Rating=4.5, k=10
   - Click "Execute Query"
   - Show filtered results

4. **Explore Patterns**

   - Switch to Co-Purchasing Patterns tab
   - Select Pattern #1
   - Show product details and customer examples
   - Explain support and confidence metrics

5. **View Visualizations**
   - Switch to Visualizations tab
   - Show Pattern Support comparison (train vs test)
   - Show Top Products chart
   - Show Rating Analysis histograms

**Total demo time:** 5-7 minutes

## Technical Details

### Database Queries

All queries use parameterized Cypher to prevent SQL injection:

```cypher
MATCH (p:Product)
WHERE p.group = $group
  AND p.avg_rating >= $min_rating
  AND p.total_reviews >= $min_reviews
RETURN p.asin, p.title, p.avg_rating, p.total_reviews
ORDER BY p.avg_rating DESC, p.total_reviews DESC
LIMIT $k
```

### Pattern Mining Algorithm

Uses Apriori frequent itemset mining:

1. Split reviews 70/30 (train/test)
2. Find frequent 1-itemsets (products)
3. Generate candidate 2-itemsets (pairs)
4. Count support in transactions
5. Filter by minimum support
6. Validate in test set
7. Calculate confidence

### GUI Components

- **Tkinter:** Main framework
- **ttk:** Modern themed widgets
- **Matplotlib:** Chart generation
- **FigureCanvasTkAgg:** Embed charts in Tkinter
- **ScrolledText:** Text display with scrolling
- **Treeview:** Table display with sorting

## Dependencies

```
neo4j>=5.0.0              # Database driver
matplotlib>=3.5.0         # Visualization
numpy>=1.21.0             # Numerical operations
pandas>=2.0.0             # Data manipulation
pyspark>=3.5.0            # Spark (for algorithms)
python-dotenv>=0.19.0     # Environment variables
```

## Credits

**Team Baddie**

- Brian Leung
- Cayden Calo
- Jeremiah Carlo Miguel
- Travis Takushi

**Course:** Big Data Analytics
**Milestone:** 4 - End-to-End Application
**Technology Stack:** Python, Tkinter, Neo4j, Matplotlib, Apache Spark

## Report

For complete documentation, see `files/milestone4_report.txt` which includes:

1. Detailed UI component descriptions
2. All user queries with results
3. Scalability analysis
4. Hardware specifications
5. Source code documentation
6. Algorithm pseudo-code
7. Performance benchmarks

## Next Steps

For production deployment:

1. Deploy Neo4j cluster (3 core + 2 read replicas)
2. Set up Spark cluster (1 master + 4 workers)
3. Load full 548,552 product dataset
4. Implement authentication and user management
5. Add export functionality (CSV, PDF reports)
6. Optimize queries with additional indexes
7. Implement caching layer (Redis)
8. Add real-time pattern mining updates
