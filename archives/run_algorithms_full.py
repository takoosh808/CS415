"""
Neo4j Query and Pattern Mining Algorithms (Direct Neo4j Implementation)
=======================================================================

This module provides classes for querying Amazon product data and mining
co-purchase patterns directly using Neo4j (without Apache Spark).

This is a simpler alternative to the Spark-based implementation, suitable for:
- Smaller datasets that fit in memory
- Systems without Spark installed
- Simpler deployment requirements

COMPARISON: Neo4j Direct vs. Spark
----------------------------------
Neo4j Direct (this file):
  + Simpler setup (just Neo4j)
  + Good for smaller datasets
  - Limited parallelization
  - Pattern mining less sophisticated

Spark-based (other files):
  + Highly parallelized
  + Better for large datasets
  + More sophisticated algorithms (FP-Growth)
  - Requires Spark setup

KEY CONCEPTS:
-------------
1. Query Algorithm: Filter and retrieve products based on criteria
2. Pattern Mining: Find products frequently bought together
3. Association Rules: If customer buys A, they'll likely buy B
   - Confidence: P(B|A) - probability of B given A
   - Lift: How much more likely B is given A, compared to random chance

DEPENDENCIES:
- neo4j: Official Neo4j Python driver
"""

from neo4j import GraphDatabase
import time


class QueryAlgorithm:
    """
    Execute parameterized queries against the Neo4j product database.
    
    This class builds and executes Cypher queries dynamically based on
    user-specified filter conditions (rating, reviews, product group, etc.).
    
    Example usage:
        algo = QueryAlgorithm()
        results, time = algo.execute_query({'min_rating': 4.5, 'group': 'Book'}, k=10)
        algo.close()
    """
    
    def __init__(self):
        """
        Initialize connection to Neo4j database.
        Uses Bolt protocol for binary communication (faster than HTTP).
        """
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
    
    def execute_query(self, conditions, k=10):
        """
        Execute a filtered query for products matching specified conditions.
        
        This method dynamically builds a Cypher query based on which conditions
        are provided. It uses parameterized queries ($param syntax) for security
        and performance.
        
        Parameters:
        -----------
        conditions : dict
            Filter conditions. Supported keys:
            - 'min_rating': Minimum average rating (float, 1-5)
            - 'max_rating': Maximum average rating (float, 1-5)
            - 'min_reviews': Minimum number of reviews (int)
            - 'group': Product group ('Book', 'DVD', 'Music', 'Video')
            - 'max_salesrank': Maximum sales rank (int, lower = better selling)
        k : int
            Maximum number of results to return (default: 10)
        
        Returns:
        --------
        tuple (list, float)
            - List of product dictionaries with id, asin, title, group, etc.
            - Execution time in seconds
        
        CYPHER PARAMETERIZATION:
        -----------------------
        Using $param_name syntax instead of string concatenation:
        - Prevents SQL injection attacks
        - Allows query plan caching (faster repeated queries)
        - Handles type conversion automatically
        """
        start_time = time.time()
        
        # Build WHERE clause dynamically based on provided conditions
        where_clauses = []
        params = {}  # Parameters to pass to the query
        
        # Add each condition if present
        if 'min_rating' in conditions:
            where_clauses.append("p.avg_rating >= $min_rating")
            params['min_rating'] = conditions['min_rating']
            
        if 'max_rating' in conditions:
            where_clauses.append("p.avg_rating <= $max_rating")
            params['max_rating'] = conditions['max_rating']
            
        if 'min_reviews' in conditions:
            where_clauses.append("p.total_reviews >= $min_reviews")
            params['min_reviews'] = conditions['min_reviews']
            
        if 'group' in conditions:
            where_clauses.append("p.group = $group")
            params['group'] = conditions['group']
            
        if 'max_salesrank' in conditions:
            where_clauses.append("p.salesrank <= $max_salesrank AND p.salesrank > 0")
            params['max_salesrank'] = conditions['max_salesrank']
        
        # Combine clauses with AND, default to "true" if no conditions
        where_clause = " AND ".join(where_clauses) if where_clauses else "true"
        
        # Construct the full query
        query = f"""
            MATCH (p:Product)
            WHERE {where_clause}
            RETURN p.id as id, p.asin as asin, p.title as title, 
                   p.group as group, p.salesrank as salesrank,
                   p.avg_rating as avg_rating, p.total_reviews as total_reviews
            ORDER BY p.avg_rating DESC, p.total_reviews DESC
            LIMIT $k
        """
        params['k'] = k
        
        # Execute the query
        with self.driver.session(database="neo4j") as session:
            result = session.run(query, params)
            # Convert result records to plain Python dictionaries
            products = [dict(record) for record in result]
        
        elapsed = time.time() - start_time
        return products, elapsed
    
    def close(self):
        """Close the database connection to free resources."""
        if self.driver:
            self.driver.close()


class PatternMiningAlgorithm:
    """
    Mine co-purchase patterns directly from Neo4j graph relationships.
    
    This is a Neo4j-native approach to finding frequent patterns, leveraging
    the graph structure rather than traditional itemset mining algorithms.
    
    HOW IT WORKS:
    1. Find product pairs connected by SIMILAR_TO relationships
    2. For each pair, count customers who reviewed BOTH products
    3. Calculate confidence and lift for association rules
    
    Note: This approach relies on existing SIMILAR_TO relationships and
    customer reviews to infer co-purchase patterns. The Spark-based FP-Growth
    implementation is more sophisticated for true frequent pattern mining.
    """
    
    def __init__(self):
        """Initialize Neo4j connection."""
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
    
    def mine_frequent_patterns(self, min_support=3, max_items=100):
        """
        Find frequently co-purchased product pairs.
        
        ALGORITHM:
        1. Find similar product pairs where both have sufficient reviews
        2. Count customers who reviewed BOTH products (= support)
        3. Filter pairs with support >= min_support
        4. Generate association rules with confidence and lift
        
        Parameters:
        -----------
        min_support : int
            Minimum number of customers who must have bought both products
            for the pair to be considered "frequent" (default: 3)
        max_items : int
            Maximum number of frequent items to track (default: 100)
        
        Returns:
        --------
        dict containing:
            - 'frequent_items': Individual products that appear in patterns
            - 'frequent_pairs': Product pairs with support counts
            - 'rules': Association rules with confidence and lift
            - 'elapsed_time': Processing time in seconds
        
        CYPHER PATTERN EXPLAINED:
        -------------------------
        MATCH (p1:Product)-[:SIMILAR_TO]->(p2:Product)
        This finds product pairs already marked as similar.
        
        MATCH (c:Customer)-[:REVIEWED]->(p1)
        MATCH (c)-[:REVIEWED]->(p2)
        This finds customers who reviewed BOTH products (same 'c' variable).
        
        count(DISTINCT c) as support
        Counts unique customers = pattern support
        """
        start_time = time.time()
        
        # Find similar product pairs and count co-purchasing customers
        query_pairs = """
            MATCH (p1:Product)-[:SIMILAR_TO]->(p2:Product)
            WHERE p1.total_reviews >= $min_support AND p2.total_reviews >= $min_support
            WITH p1, p2
            MATCH (c:Customer)-[:REVIEWED]->(p1)
            MATCH (c)-[:REVIEWED]->(p2)
            WITH p1, p2, count(DISTINCT c) as support
            WHERE support >= $min_support
            RETURN p1.id as item1, p1.asin as asin1, p1.title as title1, p1.total_reviews as reviews1,
                   p2.id as item2, p2.asin as asin2, p2.title as title2, p2.total_reviews as reviews2,
                   support
            ORDER BY support DESC
            LIMIT 50
        """
        
        with self.driver.session(database="neo4j") as session:
            result = session.run(query_pairs, {'min_support': min_support})
            frequent_pairs = [dict(record) for record in result]
        
        # Extract individual frequent items from the pairs
        frequent_items = []
        seen_items = set()  # Track items we've already added
        
        for pair in frequent_pairs:
            # Add item1 if not seen yet
            if pair['item1'] not in seen_items:
                frequent_items.append({
                    'item': pair['item1'],
                    'asin': pair['asin1'],
                    'title': pair['title1'],
                    'support': pair['reviews1']  # Using review count as item support
                })
                seen_items.add(pair['item1'])
            # Add item2 if not seen yet
            if pair['item2'] not in seen_items:
                frequent_items.append({
                    'item': pair['item2'],
                    'asin': pair['asin2'],
                    'title': pair['title2'],
                    'support': pair['reviews2']
                })
                seen_items.add(pair['item2'])
        
        # Generate association rules from frequent pairs
        # Rules: A → B (if customer buys A, they'll likely buy B)
        rules = []
        for pair in frequent_pairs:
            # Find support values for individual items
            item1_support = next((item['support'] for item in frequent_items 
                                 if item['item'] == pair['item1']), 0)
            item2_support = next((item['support'] for item in frequent_items 
                                 if item['item'] == pair['item2']), 0)
            
            if item1_support > 0 and item2_support > 0:
                total_support = sum(item['support'] for item in frequent_items)
                
                # Rule 1: Item1 → Item2
                # Confidence = P(Item2 | Item1) = Support(1,2) / Support(1)
                confidence_1_2 = pair['support'] / item1_support
                # Lift = Confidence / P(Item2) = how much more likely than random
                lift_1_2 = confidence_1_2 / (item2_support / total_support)
                
                rules.append({
                    'antecedent': [pair['asin1']],  # "If customer buys this..."
                    'consequent': [pair['asin2']],  # "...they'll buy this"
                    'confidence': confidence_1_2,
                    'lift': lift_1_2,
                    'support': pair['support']
                })
                
                # Rule 2: Item2 → Item1 (reverse direction)
                confidence_2_1 = pair['support'] / item2_support
                lift_2_1 = confidence_2_1 / (item1_support / total_support)
                
                rules.append({
                    'antecedent': [pair['asin2']],
                    'consequent': [pair['asin1']],
                    'confidence': confidence_2_1,
                    'lift': lift_2_1,
                    'support': pair['support']
                })
        
        # Sort rules by confidence (highest first)
        rules.sort(key=lambda x: x['confidence'], reverse=True)
        
        elapsed = time.time() - start_time
        
        return {
            'frequent_items': frequent_items,
            'frequent_pairs': frequent_pairs,
            'rules': rules,
            'elapsed_time': elapsed
        }
    
    def close(self):
        """Close database connection."""
        if self.driver:
            self.driver.close()


def main():
    """
    Demonstration of query and pattern mining algorithms.
    
    Runs several example queries and pattern mining to show capabilities.
    """
    # QUERY DEMONSTRATIONS
    # --------------------
    query_algo = QueryAlgorithm()
    
    # Query 1: High-rated products with many reviews
    results1, time1 = query_algo.execute_query({
        'min_rating': 4.5,
        'min_reviews': 100
    }, k=10)
    
    # Query 2: Top books under certain sales rank
    results2, time2 = query_algo.execute_query({
        'group': 'Book',
        'min_rating': 4.0,
        'max_salesrank': 50000
    }, k=10)
    
    # Query 3: Popular music products
    results3, time3 = query_algo.execute_query({
        'group': 'Music',
        'min_rating': 4.0,
        'min_reviews': 50
    }, k=10)
    
    query_algo.close()
    
    # PATTERN MINING DEMONSTRATION
    # ----------------------------
    pattern_algo = PatternMiningAlgorithm()
    results = pattern_algo.mine_frequent_patterns(min_support=3, max_items=100)
    pattern_algo.close()


if __name__ == "__main__":
    main()

