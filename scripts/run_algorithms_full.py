from neo4j import GraphDatabase
import time


class QueryAlgorithm:
    
    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
    
    def execute_query(self, conditions, k=10):
        start_time = time.time()
        where_clauses = []
        params = {}
        
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
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "true"
        
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
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run(query, params)
            products = [dict(record) for record in result]
        
        elapsed = time.time() - start_time
        return products, elapsed
    
    def find_products_by_active_customers(self, min_customer_reviews=5, k=10):
        start_time = time.time()
        query = """
            MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
            WITH c, count(r) as review_count, collect({product: p, rating: r.rating}) as products
            WHERE review_count >= $min_reviews
            UNWIND products as prod
            WITH prod.product as p, count(DISTINCT c) as active_customer_count, 
                 avg(prod.rating) as avg_rating
            RETURN p.id as id, p.asin as asin, p.title as title,
                   p.avg_rating as product_avg_rating, p.total_reviews as total_reviews,
                   active_customer_count, avg_rating as rating_from_active_customers
            ORDER BY active_customer_count DESC, rating_from_active_customers DESC
            LIMIT $k
        """
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run(query, {'min_reviews': min_customer_reviews, 'k': k})
            products = [dict(record) for record in result]
        
        elapsed = time.time() - start_time
        return products, elapsed
    
    def close(self):
        if self.driver:
            self.driver.close()


class PatternMiningAlgorithm:
    
    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
    
    def mine_frequent_patterns(self, min_support=10, max_items=100):
        start_time = time.time()
        query1 = """
            MATCH (p:Product)
            WHERE p.total_reviews >= $min_support
            RETURN p.id as item, p.asin as asin, p.title as title, 
                   p.total_reviews as support
            ORDER BY p.total_reviews DESC
            LIMIT $max_items
        """
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run(query1, {'min_support': min_support, 'max_items': max_items})
            frequent_items = [dict(record) for record in result]
        
        top_product_ids = [item['item'] for item in frequent_items[:50]]
        
        query2 = """
            UNWIND $product_ids as pid1
            UNWIND $product_ids as pid2
            WITH pid1, pid2 WHERE pid1 < pid2
            MATCH (c:Customer)-[:REVIEWED]->(p1:Product {id: pid1})
            MATCH (c)-[:REVIEWED]->(p2:Product {id: pid2})
            WITH p1, p2, count(DISTINCT c) as support
            WHERE support >= $min_support
            RETURN p1.id as item1, p1.asin as asin1, p1.title as title1,
                   p2.id as item2, p2.asin as asin2, p2.title as title2,
                   support
            ORDER BY support DESC
            LIMIT 50
        """
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run(query2, {
                'product_ids': top_product_ids,
                'min_support': min_support
            })
            frequent_pairs = [dict(record) for record in result]
        
        rules = []
        
        for pair in frequent_pairs:
            item1_support = next((item['support'] for item in frequent_items 
                                 if item['item'] == pair['item1']), 0)
            item2_support = next((item['support'] for item in frequent_items 
                                 if item['item'] == pair['item2']), 0)
            
            if item1_support > 0 and item2_support > 0:
                confidence_1_2 = pair['support'] / item1_support
                lift_1_2 = confidence_1_2 / (item2_support / sum(item['support'] for item in frequent_items))
                
                confidence_2_1 = pair['support'] / item2_support
                lift_2_1 = confidence_2_1 / (item1_support / sum(item['support'] for item in frequent_items))
                
                rules.append({
                    'antecedent': [pair['asin1']],
                    'consequent': [pair['asin2']],
                    'confidence': confidence_1_2,
                    'lift': lift_1_2,
                    'support': pair['support']
                })
                
                rules.append({
                    'antecedent': [pair['asin2']],
                    'consequent': [pair['asin1']],
                    'confidence': confidence_2_1,
                    'lift': lift_2_1,
                    'support': pair['support']
                })
        
        rules.sort(key=lambda x: x['confidence'], reverse=True)
        elapsed = time.time() - start_time
        
        return {
            'frequent_items': frequent_items,
            'frequent_pairs': frequent_pairs,
            'rules': rules,
            'elapsed_time': elapsed
        }
    
    def close(self):
        if self.driver:
            self.driver.close()


def main():
    query_algo = QueryAlgorithm()
    
    print("\n=== Query Algorithm Results ===\n")
    
    results1, time1 = query_algo.execute_query({
        'min_rating': 4.5,
        'min_reviews': 100
    }, k=10)
    print(f"Query 1 (High-rated popular products): {len(results1)} results in {time1:.3f}s")
    
    results2, time2 = query_algo.execute_query({
        'group': 'Book',
        'min_rating': 4.0,
        'max_salesrank': 50000
    }, k=10)
    print(f"Query 2 (Book recommendations): {len(results2)} results in {time2:.3f}s")
    
    results3, time3 = query_algo.execute_query({
        'group': 'Music',
        'min_rating': 4.0,
        'min_reviews': 50
    }, k=10)
    print(f"Query 3 (Music recommendations): {len(results3)} results in {time3:.3f}s")
    
    results4, time4 = query_algo.find_products_by_active_customers(min_customer_reviews=5, k=10)
    print(f"Query 4 (Active customer products): {len(results4)} results in {time4:.3f}s")
    query_algo.close()
    
    print("\n=== Pattern Mining Results ===\n")
    
    pattern_algo = PatternMiningAlgorithm()
    results = pattern_algo.mine_frequent_patterns(min_support=100, max_items=100)
    print(f"Found {results['patterns_found']} patterns in {results['elapsed_time']:.3f}s")
    print(f"Top pattern: {results['top_patterns'][0]['pattern']} (support: {results['top_patterns'][0]['support']})")
    pattern_algo.close()


if __name__ == "__main__":
    main()

