"""
Simple Co-purchasing Pattern Algorithm (Student Implementation)
Uses basic Python operations to simulate Spark MLlib operations
"""

from neo4j import GraphDatabase
import time
from collections import defaultdict


class SimpleCoPurchasingAlgorithm:
    """
    Algorithm to find frequent co-purchasing patterns
    Input: Transaction data, minimum support threshold
    Output: Frequent patterns and their frequencies
    """
    
    def __init__(self):
        self.driver = None
        self.transactions = []
        self.copurchase_pairs = []
        
    def connect_to_database(self):
        """Connect to Neo4j database"""
        print("Connecting to Neo4j...")
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
        print("Connected to Neo4j")
        
    def load_data(self):
        """Load co-purchasing data from Neo4j"""
        print("Loading co-purchasing data from Neo4j...")
        
        # Get customer transactions
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (c:Customer)-[:REVIEWED]->(p:Product)
                RETURN c.customer_id as customer_id, 
                       collect(p.asin) as products
                ORDER BY customer_id
            """)
            
            self.transactions = []
            for record in result:
                if len(record['products']) > 1:  # Only customers with multiple products
                    self.transactions.append({
                        'customer_id': record['customer_id'],
                        'products': record['products']
                    })
        
        # Get co-purchase relationships
        with self.driver.session(database="amazon-analysis") as session:
            result = session.run("""
                MATCH (p1:Product)-[co:CO_PURCHASED_WITH]-(p2:Product)
                WHERE p1.asin < p2.asin
                RETURN p1.asin as product1, p1.title as title1,
                       p2.asin as product2, p2.title as title2,
                       co.count as count
                ORDER BY count DESC
                LIMIT 100
            """)
            
            self.copurchase_pairs = []
            for record in result:
                self.copurchase_pairs.append({
                    'product1': record['product1'],
                    'title1': record['title1'],
                    'product2': record['product2'],
                    'title2': record['title2'],
                    'count': record['count']
                })
        
        print(f"Loaded {len(self.transactions)} customer transactions")
        print(f"Loaded {len(self.copurchase_pairs)} co-purchase relationships")
        
    def find_frequent_patterns(self, min_support=3):
        """
        Find frequent co-purchasing patterns
        Simulates FP-Growth algorithm with simple frequent itemset mining
        
        Args:
            min_support: minimum number of transactions a pattern must appear in
            
        Returns:
            frequent patterns
        """
        print(f"\nFinding frequent patterns (min_support={min_support})...")
        start_time = time.time()
        
        # Count individual items
        item_counts = defaultdict(int)
        for trans in self.transactions:
            for item in trans['products']:
                item_counts[item] += 1
        
        # Find frequent individual items
        frequent_items = {item: count for item, count in item_counts.items() 
                         if count >= min_support}
        
        # Count pairs
        pair_counts = defaultdict(int)
        for trans in self.transactions:
            products = sorted(trans['products'])
            for i in range(len(products)):
                for j in range(i + 1, len(products)):
                    if products[i] in frequent_items and products[j] in frequent_items:
                        pair = (products[i], products[j])
                        pair_counts[pair] += 1
        
        # Find frequent pairs
        frequent_pairs = {pair: count for pair, count in pair_counts.items() 
                         if count >= min_support}
        
        # Count triplets
        triplet_counts = defaultdict(int)
        for trans in self.transactions:
            products = sorted([p for p in trans['products'] if p in frequent_items])
            for i in range(len(products)):
                for j in range(i + 1, len(products)):
                    for k in range(j + 1, len(products)):
                        triplet = (products[i], products[j], products[k])
                        triplet_counts[triplet] += 1
        
        # Find frequent triplets
        frequent_triplets = {triplet: count for triplet, count in triplet_counts.items() 
                            if count >= min_support}
        
        execution_time = time.time() - start_time
        
        patterns = {
            'individual': frequent_items,
            'pairs': frequent_pairs,
            'triplets': frequent_triplets
        }
        
        print(f"Found {len(frequent_items)} frequent items")
        print(f"Found {len(frequent_pairs)} frequent pairs")
        print(f"Found {len(frequent_triplets)} frequent triplets")
        print(f"Pattern mining completed in {execution_time:.3f} seconds")
        
        return patterns, execution_time
    
    def generate_association_rules(self, patterns, min_confidence=0.3):
        """
        Generate association rules from frequent patterns
        
        Args:
            patterns: frequent patterns from find_frequent_patterns
            min_confidence: minimum confidence threshold
            
        Returns:
            list of association rules
        """
        print(f"\nGenerating association rules (min_confidence={min_confidence})...")
        start_time = time.time()
        
        rules = []
        
        # Generate rules from pairs: {A} => {B}
        for (item1, item2), count in patterns['pairs'].items():
            support_a = patterns['individual'][item1]
            confidence = count / support_a
            
            if confidence >= min_confidence:
                rules.append({
                    'antecedent': [item1],
                    'consequent': [item2],
                    'support': count,
                    'confidence': confidence,
                    'lift': confidence / (patterns['individual'][item2] / len(self.transactions))
                })
        
        # Generate rules from triplets: {A, B} => {C}
        for (item1, item2, item3), count in patterns['triplets'].items():
            pair_key = (item1, item2)
            if pair_key in patterns['pairs']:
                support_ab = patterns['pairs'][pair_key]
                confidence = count / support_ab
                
                if confidence >= min_confidence:
                    rules.append({
                        'antecedent': [item1, item2],
                        'consequent': [item3],
                        'support': count,
                        'confidence': confidence,
                        'lift': confidence / (patterns['individual'][item3] / len(self.transactions))
                    })
        
        # Sort rules by confidence
        rules.sort(key=lambda x: x['confidence'], reverse=True)
        
        execution_time = time.time() - start_time
        
        print(f"Generated {len(rules)} association rules in {execution_time:.3f} seconds")
        
        return rules, execution_time
    
    def split_data_train_test(self, train_ratio=0.7):
        """Split transaction data into training and testing sets"""
        print(f"\nSplitting data: {train_ratio*100}% training, {(1-train_ratio)*100}% testing...")
        
        import random
        random.seed(42)
        
        shuffled = self.transactions.copy()
        random.shuffle(shuffled)
        
        split_index = int(len(shuffled) * train_ratio)
        train_data = shuffled[:split_index]
        test_data = shuffled[split_index:]
        
        print(f"Training set: {len(train_data)} transactions")
        print(f"Testing set: {len(test_data)} transactions")
        
        return train_data, test_data
    
    def validate_patterns(self, min_support=3):
        """Validate patterns found in training set against test set"""
        print("\nValidating patterns in training vs testing data...")
        start_time = time.time()
        
        # Split data
        train_data, test_data = self.split_data_train_test(0.7)
        
        # Find patterns in training set
        original_transactions = self.transactions
        self.transactions = train_data
        train_patterns, _ = self.find_frequent_patterns(min_support)
        
        # Find patterns in testing set
        self.transactions = test_data
        test_patterns, _ = self.find_frequent_patterns(min_support)
        
        # Restore original
        self.transactions = original_transactions
        
        execution_time = time.time() - start_time
        
        print(f"Validation completed in {execution_time:.3f} seconds")
        
        return train_patterns, test_patterns, execution_time
    
    def get_most_significant_patterns(self, patterns):
        """Identify the most significant co-purchasing patterns"""
        print("\nIdentifying most significant co-purchasing patterns...")
        
        significant = {
            'top_pairs': sorted(patterns['pairs'].items(), 
                              key=lambda x: x[1], reverse=True)[:10],
            'top_triplets': sorted(patterns['triplets'].items(), 
                                  key=lambda x: x[1], reverse=True)[:10]
        }
        
        return significant
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()
            print("Connection closed")


if __name__ == "__main__":
    # Test the algorithm
    algo = SimpleCoPurchasingAlgorithm()
    algo.connect_to_database()
    algo.load_data()
    
    # Find patterns
    patterns, exec_time = algo.find_frequent_patterns(min_support=3)
    
    print("\nTop 5 frequent pairs:")
    for i, (pair, count) in enumerate(sorted(patterns['pairs'].items(), 
                                             key=lambda x: x[1], reverse=True)[:5]):
        print(f"{i+1}. {pair[0]} + {pair[1]} - Frequency: {count}")
    
    algo.close()
