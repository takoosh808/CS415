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
        
        with self.driver.session(database="neo4j") as session:
            result = session.run(query, params)
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
    
    def mine_frequent_patterns(self, min_support=3, max_items=100):
        start_time = time.time()
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
        frequent_items = []
        seen_items = set()
        for pair in frequent_pairs:
            if pair['item1'] not in seen_items:
                frequent_items.append({
                    'item': pair['item1'],
                    'asin': pair['asin1'],
                    'title': pair['title1'],
                    'support': pair['reviews1']
                })
                seen_items.add(pair['item1'])
            if pair['item2'] not in seen_items:
                frequent_items.append({
                    'item': pair['item2'],
                    'asin': pair['asin2'],
                    'title': pair['title2'],
                    'support': pair['reviews2']
                })
                seen_items.add(pair['item2'])
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
    results1, time1 = query_algo.execute_query({
        'min_rating': 4.5,
        'min_reviews': 100
    }, k=10)
    results2, time2 = query_algo.execute_query({
        'group': 'Book',
        'min_rating': 4.0,
        'max_salesrank': 50000
    }, k=10)
    results3, time3 = query_algo.execute_query({
        'group': 'Music',
        'min_rating': 4.0,
        'min_reviews': 50
    }, k=10)
    query_algo.close()
    pattern_algo = PatternMiningAlgorithm()
    results = pattern_algo.mine_frequent_patterns(min_support=3, max_items=100)
    pattern_algo.close()


if __name__ == "__main__":
    main()

