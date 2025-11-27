# Quick Start Guide - Amazon Co-Purchasing Analysis GUI

## Launch Instructions

### Prerequisites

1. Neo4j database running on `bolt://localhost:7687`
2. Database credentials: `neo4j` / `Password`
3. Data loaded into Neo4j (products, customers, reviews)
4. **Pre-generated results** (recommended)

### Generate Results (One-Time Setup)

```bash
python pregenerate_gui_results.py
```

This creates three JSON files (~30-60 seconds):

- `gui_patterns.json` - Pattern mining results
- `gui_stats.json` - Database statistics
- `gui_queries.json` - Sample query results

### Start the Application

```bash
python gui_app.py
```

## Quick Demo Steps

### Demo 1: Complex Product Queries (2 minutes)

**Objective**: Find high-quality DVD products

1. Click **Complex Queries** tab
2. Set parameters (all fields are typeable):
   - Product Group: **DVD**
   - Min Rating: **4.5**
   - Min Reviews: **30**
   - Limit (k): **20**
3. Click **Execute Query**
4. **Observe**: Top-rated DVDs appear in table with execution time

**Try Other Queries**:

- Popular books: Group=**Book**, Min Reviews=**100**
- Highly rated: Min Rating=**4.8**, Min Reviews=**20**
- Rating range: Min Rating=**4.0**, Max Rating=**4.5**
- Review range: Min Reviews=**50**, Max Reviews=**200**
- Sales rank range: Min Sales Rank=**1000**, Max Sales Rank=**10000**
- Any custom combination of filters!

### Demo 2: Co-Purchasing Pattern Mining (1 minute)

**Objective**: View frequent co-purchasing patterns

**Option A: Pre-Generated Results (Instant)**

1. Click **Pattern Mining** tab
2. Click **Load Pre-Generated Results**
3. Results load instantly

**Option B: Real-Time Mining (30-60 seconds)**

1. Click **Pattern Mining** tab
2. Adjust parameters if desired:
   - Min Support: Minimum occurrences (default: 10)
   - Max Customers: Sample size (default: 500)
3. Click **Mine Patterns Now (Real-Time)**
4. Wait for mining to complete

**Observe**:

- Table shows product pairs with actual product names
- Total support count (train + test combined)
- Confidence score (validation ratio)
- Bar charts show:
  - Top chart: Total support for each pattern
  - Bottom chart: Confidence scores with target line
- Table shows product pairs frequently purchased together
- Bar chart visualizes train vs test support
- Most significant pattern displayed at bottom

**Key Metrics**:

- **Product Names**: Shows actual product titles (not ASINs)
- **Total Support**: Combined frequency in both datasets
- **Confidence**: Test/Train ratio (validation strength)

### Demo 3: Database Statistics (1 minute)

**Objective**: View overall system statistics

1. Click **Database Statistics** tab
2. Click **Refresh Statistics**
3. **Observe**:
   - Total counts (products, customers, reviews)
   - Product group distribution
   - Rating distribution
   - Train/test split information

## Key Features to Highlight

### 1. Complex Query System

✓ **Non-SQL implementation** using Neo4j graph database
✓ **Flexible filters**: All fields are typeable (not restricted)
✓ **Min/Max support**: Rating, Reviews, Sales Rank all support ranges
✓ **Searchable attributes**: group, salesrank
✓ **Non-searchable attributes**: review count, average rating  
✓ **Enriched operators**: >=, <=, = (typed directly)
✓ **Top-k results** with performance metrics
✓ **Clear All button** to reset filters

### 2. Pattern Mining

✓ **Train/test split**: 70/30 automatic split
✓ **Frequent pattern discovery**: Apriori-based algorithm
✓ **Pattern validation**: Cross-validation in test set
✓ **Visual comparison**: Train vs test support charts
✓ **Significant pattern identification**: Automatic ranking

### 3. Data Visualization

✓ **Interactive tables**: Sortable result displays
✓ **Bar charts**: Pattern frequency comparison
✓ **Statistical reports**: Comprehensive database metrics

## Common Use Cases

### Use Case 1: Product Recommendation

**Query**: Find similar high-rated products in same category

```
Group: Music
Min Rating: 4.0
Min Reviews: 50
```

**Result**: Recommended music products based on quality

### Use Case 2: Market Basket Analysis

**Pattern Mining**: Discover which products are purchased together

```
Min Support: 10
Max Customers: 500
```

**Result**: Product pairs for cross-selling opportunities

### Use Case 3: Quality Assessment

**Query**: Find products with excellent customer satisfaction

```
Min Rating: 4.8
Min Reviews: 30
```

**Result**: Highest quality products across all categories

## Performance Tips

### Two Mining Modes:

1. **Pre-Generated**: Instant loading, perfect for demos
2. **Real-Time**: Live computation, shows actual mining process

### Pre-Generation Benefits:

- Pattern mining results load **instantly** (vs 30-60 seconds)
- No database load during demonstration
- Consistent, reproducible results
- Perfect for presentations

### Live Query Mode:

- Complex queries execute in real-time (0.1-0.8 seconds)
- Can adjust parameters and see immediate results
- If pre-generated files missing, offers to compute on-the-fly

## Troubleshooting

| Issue                             | Solution                                  |
| --------------------------------- | ----------------------------------------- |
| "Status: Database Offline"        | Start Neo4j OR use pre-generated results  |
| "Pre-generated results not found" | Run `python pregenerate_gui_results.py`   |
| No query results                  | Relax filters (lower min rating/reviews)  |
| "Not connected to database" error | Verify Neo4j running or use pre-gen files |

## Presentation Tips

1. **Pre-generate results before demo** - Run `pregenerate_gui_results.py`
2. **Start with statistics tab** - Show data scale
3. **Demonstrate simple query** - Group=Book, limit=20
4. **Show complex query** - Multiple filters combined
5. **Load pattern mining** - Instant results, explain train/test
6. **Highlight visualization** - Show train vs test chart
7. **Explain most significant pattern** - Business insight

## Technical Notes

### Non-SQL Implementation

- Uses Neo4j Cypher queries (graph database)
- Relationship-based traversals, not table joins
- Optimized for connected data (co-purchasing networks)

### Algorithm Efficiency

- **Query complexity**: O(n log n) with indexes
- **Pattern mining**: Aprioi algorithm, O(n²) for pairs
- **Memory usage**: Controlled by max_customers parameter

### Scalability

- Tested on datasets up to 548K products
- Cluster-ready (Neo4j connection string configurable)
- Batch processing for large result sets

## Next Steps

After demonstration:

1. Review `GUI_README.md` for detailed documentation
2. Check project files in `files/` directory
3. Examine source code structure
4. Review milestone 4 requirements checklist

## Credits

This GUI implements:

- **Project Requirements**: Complex queries + Pattern mining
- **Milestone 4**: End-to-end application with visualization
- **Technology Stack**: Neo4j (NoSQL) + Python + Tkinter + Matplotlib
