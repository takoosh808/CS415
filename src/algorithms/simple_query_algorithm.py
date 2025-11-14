"""
Simple Query Algorithm (Student Implementation)
Uses basic Python operations to simulate Spark-like queries
"""

from neo4j import GraphDatabase
import time


class SimpleQueryAlgorithm:
    """
    Algorithm to answer complex queries on Amazon product dataset
    Input: Query conditions (filters), number of results k
    Output: k products matching the query conditions
    """
    
    def __init__(self):
        self.driver = None
        self.products = []
        self.reviews = []
        
    def connect_to_database(self):
        """Connect to Neo4j database"""
        print("Connecting to Neo4j...")
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
        print("Connected to Neo4j")
        
    def load_data(self):
        """Load data from Neo4j"""
        print("Loading data from Neo4j...")
        
        # Get products
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (p:Product)
                OPTIONAL MATCH (p)<-[r:REVIEWED]-()
                RETURN p.id as id, p.asin as asin, p.title as title, 
                       p.group as group, p.salesrank as salesrank,
                       p.avg_rating as avg_rating, p.total_reviews as total_reviews,
                       count(r) as review_count
            """)
            
            self.products = []
            for record in result:
                self.products.append({
                    'id': record['id'],
                    'asin': record['asin'],
                    'title': record['title'],
                    'group': record['group'],
                    'salesrank': record['salesrank'],
                    'avg_rating': record['avg_rating'],
                    'total_reviews': record['total_reviews'],
                    'review_count': record['review_count']
                })
        
        # Get reviews
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                RETURN p.id as product_id, c.customer_id as customer_id,
                       r.rating as rating
            """)
            
            self.reviews = []
            for record in result:
                self.reviews.append({
                    'product_id': record['product_id'],
                    'customer_id': record['customer_id'],
                    'rating': record['rating']
                })
        
        print(f"Loaded {len(self.products)} products and {len(self.reviews)} reviews")
        
    def execute_query(self, conditions, k=10):
        """
        Execute a complex query with given conditions
        
        Args:
            conditions: dict with query filters
            k: number of results to return
            
        Returns:
            List of products matching the conditions
        """
        print(f"\nExecuting query with conditions: {conditions}")
        start_time = time.time()
        
        # Filter products based on conditions
        filtered = self.products.copy()
        
        if 'min_rating' in conditions:
            filtered = [p for p in filtered 
                       if p['avg_rating'] and p['avg_rating'] >= conditions['min_rating']]
            
        if 'max_rating' in conditions:
            filtered = [p for p in filtered 
                       if p['avg_rating'] and p['avg_rating'] <= conditions['max_rating']]
            
        if 'min_reviews' in conditions:
            filtered = [p for p in filtered 
                       if p['review_count'] >= conditions['min_reviews']]
            
        if 'group' in conditions:
            filtered = [p for p in filtered 
                       if p['group'] == conditions['group']]
            
        if 'max_salesrank' in conditions:
            filtered = [p for p in filtered 
                       if p['salesrank'] and p['salesrank'] <= conditions['max_salesrank']]
        
        # Sort by rating and review count
        filtered.sort(key=lambda x: (x['avg_rating'] or 0, x['review_count']), reverse=True)
        
        # Limit to k results
        results = filtered[:k]
        
        execution_time = time.time() - start_time
        
        print(f"Query returned {len(results)} results in {execution_time:.3f} seconds")
        
        return results, execution_time
    
    def find_products_by_customer_reviews(self, min_customer_reviews, k=10):
        """
        Find products reviewed by customers who have reviewed at least N products
        This is a non-searchable attribute query
        """
        print(f"\nFinding products reviewed by active customers (min {min_customer_reviews} reviews)...")
        start_time = time.time()
        
        # Count reviews per customer
        customer_counts = {}
        for review in self.reviews:
            cid = review['customer_id']
            customer_counts[cid] = customer_counts.get(cid, 0) + 1
        
        # Find active customers
        active_customers = {cid for cid, count in customer_counts.items() 
                           if count >= min_customer_reviews}
        
        # Find products reviewed by active customers
        product_active_counts = {}
        product_ratings = {}
        
        for review in self.reviews:
            if review['customer_id'] in active_customers:
                pid = review['product_id']
                product_active_counts[pid] = product_active_counts.get(pid, 0) + 1
                if pid not in product_ratings:
                    product_ratings[pid] = []
                product_ratings[pid].append(review['rating'])
        
        # Calculate average ratings
        product_scores = []
        for pid, count in product_active_counts.items():
            avg_rating = sum(product_ratings[pid]) / len(product_ratings[pid])
            product = next((p for p in self.products if p['id'] == pid), None)
            if product:
                product_scores.append({
                    'product': product,
                    'active_customer_reviews': count,
                    'avg_rating_from_active': avg_rating
                })
        
        # Sort by active customer reviews
        product_scores.sort(key=lambda x: (x['active_customer_reviews'], 
                                          x['avg_rating_from_active']), 
                           reverse=True)
        
        results = [ps['product'] for ps in product_scores[:k]]
        
        execution_time = time.time() - start_time
        
        print(f"Found {len(results)} products in {execution_time:.3f} seconds")
        
        return results, execution_time
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
            print("Connection closed")


if __name__ == "__main__":
    # Test the algorithm
    algo = SimpleQueryAlgorithm()
    algo.connect_to_database()
    algo.load_data()
    
    # Test query
    results, exec_time = algo.execute_query({
        'min_rating': 4.5,
        'min_reviews': 3
    }, k=10)
    
    print("\nTop products:")
    for r in results:
        print(f"  {r['title'][:50]} - Rating: {r['avg_rating']}")
    
    algo.close()
