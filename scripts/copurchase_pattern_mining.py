from neo4j import GraphDatabase
import random
import time
from collections import defaultdict


class PatternMining:
    
    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
    
    def split_train_test(self, train_ratio=0.7):
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                RETURN elementId(r) as rel_id
            """)
            rel_ids = [record['rel_id'] for record in result]
            
            random.shuffle(rel_ids)
            split_point = int(len(rel_ids) * train_ratio)
            train_ids = rel_ids[:split_point]
            test_ids = rel_ids[split_point:]
            
            for i in range(0, len(train_ids), 10000):
                batch = train_ids[i:i+10000]
                session.run("""
                    UNWIND $ids as rel_id
                    MATCH ()-[r:REVIEWED]->()
                    WHERE elementId(r) = rel_id
                    SET r.dataset = 'train'
                """, ids=batch)
            
            for i in range(0, len(test_ids), 10000):
                batch = test_ids[i:i+10000]
                session.run("""
                    UNWIND $ids as rel_id
                    MATCH ()-[r:REVIEWED]->()
                    WHERE elementId(r) = rel_id
                    SET r.dataset = 'test'
                """, ids=batch)
        
        return len(train_ids), len(test_ids)
    
    def find_frequent_patterns(self, min_support=10, max_customers=500, dataset='train'):
        start_time = time.time()
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.dataset = $dataset
                WITH c, collect(p.asin) as products
                WHERE size(products) >= 2
                RETURN c.id as customer_id, products
                LIMIT $max_customers
            """, dataset=dataset, max_customers=max_customers)
            
            transactions = []
            for record in result:
                transactions.append(set(record['products']))
            
            item_counts = defaultdict(int)
            for transaction in transactions:
                for item in transaction:
                    item_counts[item] += 1
            
            frequent_items = {item: count for item, count in item_counts.items() 
                            if count >= min_support}
            
            pair_counts = defaultdict(int)
            for transaction in transactions:
                items = [item for item in transaction if item in frequent_items]
                for i, item1 in enumerate(items):
                    for item2 in items[i+1:]:
                        pair = tuple(sorted([item1, item2]))
                        pair_counts[pair] += 1
            
            frequent_pairs = {pair: count for pair, count in pair_counts.items() 
                            if count >= min_support}
            
            top_pairs = sorted(frequent_pairs.items(), key=lambda x: x[1], reverse=True)[:20]
            
            patterns = []
            for pair, support in top_pairs:
                result = session.run("""
                    MATCH (p1:Product {asin: $asin1})
                    MATCH (p2:Product {asin: $asin2})
                    RETURN p1.title as title1, p2.title as title2
                """, asin1=pair[0], asin2=pair[1])
                
                record = result.single()
                if record:
                    confidence = support / item_counts[pair[0]]
                    patterns.append({
                        'products': pair,
                        'titles': (record['title1'], record['title2']),
                        'support': support,
                        'confidence': confidence
                    })
        
        elapsed = time.time() - start_time
        
        return {
            'frequent_items': frequent_items,
            'frequent_pairs': frequent_pairs,
            'top_patterns': patterns,
            'num_transactions': len(transactions),
            'elapsed_time': elapsed
        }
    
    def validate_patterns_in_test(self, train_patterns, min_support=10, max_customers=500):
        train_pairs = [p['products'] for p in train_patterns['top_patterns']]
        
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.dataset = 'test'
                WITH c, collect(p.asin) as products
                WHERE size(products) >= 2
                RETURN c.id as customer_id, products
                LIMIT $max_customers
            """, max_customers=max_customers)
            
            test_transactions = []
            for record in result:
                test_transactions.append(set(record['products']))
            
            test_pair_counts = defaultdict(int)
            for transaction in test_transactions:
                for pair in train_pairs:
                    if pair[0] in transaction and pair[1] in transaction:
                        test_pair_counts[pair] += 1
            
            validated_patterns = []
            for pattern in train_patterns['top_patterns'][:10]:
                pair = pattern['products']
                test_support = test_pair_counts.get(pair, 0)
                
                validated_patterns.append({
                    'products': pair,
                    'titles': pattern['titles'],
                    'train_support': pattern['support'],
                    'test_support': test_support,
                    'confidence': pattern['confidence']
                })
        
        return validated_patterns
    
    def find_customers_for_pattern(self, product_pair, dataset='train'):
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[r1:REVIEWED]->(p1:Product {asin: $asin1})
                MATCH (c)-[r2:REVIEWED]->(p2:Product {asin: $asin2})
                WHERE r1.dataset = $dataset AND r2.dataset = $dataset
                RETURN c.id as customer_id, 
                       r1.rating as rating1, r2.rating as rating2,
                       r1.date as date1, r2.date as date2
                LIMIT 100
            """, asin1=product_pair[0], asin2=product_pair[1], dataset=dataset)
            
            customers = []
            for record in result:
                customers.append({
                    'customer_id': record['customer_id'],
                    'rating1': record['rating1'],
                    'rating2': record['rating2'],
                    'date1': record['date1'],
                    'date2': record['date2']
                })
        
        return customers
    
    def close(self):
        if self.driver:
            self.driver.close()


def main():
    miner = PatternMining()
    
    train_count, test_count = miner.split_train_test(train_ratio=0.7)
    train_patterns = miner.find_frequent_patterns(min_support=10, max_customers=500, dataset='train')
    
    print("\n=== Top Co-Purchasing Patterns (Training Set) ===\n")
    for i, pattern in enumerate(train_patterns['top_patterns'][:5], 1):
        print(f"{i}. Support: {pattern['support']:,} | Confidence: {pattern['confidence']:.3f}")
        print(f"   {pattern['titles'][0][:60]}")
        print(f"   {pattern['titles'][1][:60]}\n")
    
    validated = miner.validate_patterns_in_test(train_patterns, min_support=10, max_customers=500)
    
    print("=== Pattern Validation (Test Set) ===\n")
    for i, pattern in enumerate(validated[:5], 1):
        print(f"{i}. Train: {pattern['train_support']:,} | Test: {pattern['test_support']:,} | Confidence: {pattern['confidence']:.3f}")
        print(f"   {pattern['titles'][0][:50]}")
        print(f"   {pattern['titles'][1][:50]}\n")
    
    miner.close()


if __name__ == "__main__":
    main()
