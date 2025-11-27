"""
Pre-generate pattern mining results for the GUI
This script runs the algorithms and saves results to JSON files
"""
from neo4j import GraphDatabase
import json
import time
from collections import defaultdict
import random


class ResultGenerator:
    
    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
        self.database = "neo4j"
    
    def get_database_stats(self):
        """Get overall database statistics"""
        with self.driver.session(database=self.database) as session:
            stats = {}
            
            # Count products
            result = session.run("MATCH (p:Product) RETURN count(p) as count")
            stats['total_products'] = result.single()['count']
            
            # Count customers
            result = session.run("MATCH (c:Customer) RETURN count(c) as count")
            stats['total_customers'] = result.single()['count']
            
            # Count reviews
            result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
            stats['total_reviews'] = result.single()['count']
            
            # Count similar relationships
            result = session.run("MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as count")
            stats['total_similar'] = result.single()['count']
            
            # Rating distribution
            result = session.run("""
                MATCH (p:Product)
                WHERE p.avg_rating > 0
                WITH 
                    round(p.avg_rating) as rating_bucket,
                    count(p) as count
                RETURN rating_bucket, count
                ORDER BY rating_bucket
            """)
            stats['rating_distribution'] = {int(r['rating_bucket']): r['count'] 
                                           for r in result}
            
            # Group distribution
            result = session.run("""
                MATCH (p:Product)
                WITH p.group as group, count(p) as count
                RETURN group, count
                ORDER BY count DESC
            """)
            stats['group_distribution'] = {r['group']: r['count'] for r in result}
            
        return stats
    
    def generate_sample_queries(self):
        """Generate results for sample queries"""
        queries = []
        
        # Query 1: High-rated DVDs
        query1 = {
            'name': 'High-rated DVDs',
            'description': 'DVDs with rating >= 4.5 and at least 50 reviews',
            'conditions': {'group': 'DVD', 'min_rating': 4.5, 'min_reviews': 50},
            'k': 20
        }
        query1['results'], query1['execution_time'] = self.execute_query(
            query1['conditions'], query1['k']
        )
        queries.append(query1)
        
        # Query 2: Popular Books
        query2 = {
            'name': 'Popular Books',
            'description': 'Books with at least 100 reviews',
            'conditions': {'group': 'Book', 'min_reviews': 100},
            'k': 20
        }
        query2['results'], query2['execution_time'] = self.execute_query(
            query2['conditions'], query2['k']
        )
        queries.append(query2)
        
        # Query 3: Top Music Albums
        query3 = {
            'name': 'Top Music Albums',
            'description': 'Music with rating >= 4.0 and good sales rank',
            'conditions': {'group': 'Music', 'min_rating': 4.0, 'max_salesrank': 100000},
            'k': 20
        }
        query3['results'], query3['execution_time'] = self.execute_query(
            query3['conditions'], query3['k']
        )
        queries.append(query3)
        
        # Query 4: High-rated Products
        query4 = {
            'name': 'Highest Rated Products',
            'description': 'All products with rating >= 4.8',
            'conditions': {'min_rating': 4.8, 'min_reviews': 20},
            'k': 30
        }
        query4['results'], query4['execution_time'] = self.execute_query(
            query4['conditions'], query4['k']
        )
        queries.append(query4)
        
        # Query 5: Video Products
        query5 = {
            'name': 'Popular Video Products',
            'description': 'Video products with good ratings',
            'conditions': {'group': 'Video', 'min_rating': 4.0},
            'k': 20
        }
        query5['results'], query5['execution_time'] = self.execute_query(
            query5['conditions'], query5['k']
        )
        queries.append(query5)
        
        return queries
    
    def execute_query(self, conditions, k=10):
        """Execute a query against the database"""
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
            RETURN p.asin as asin, p.title as title, 
                   p.group as group, p.salesrank as salesrank,
                   p.avg_rating as avg_rating, p.total_reviews as total_reviews
            ORDER BY p.avg_rating DESC, p.total_reviews DESC
            LIMIT $k
        """
        params['k'] = k
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query, params)
            products = [dict(record) for record in result]
        
        elapsed = time.time() - start_time
        return products, elapsed
    
    def generate_pattern_mining_results(self):
        """Generate pattern mining results"""
        print("Splitting data into train/test sets...")
        train_count, test_count = self.split_train_test()
        
        print(f"Training set: {train_count:,} reviews")
        print(f"Testing set: {test_count:,} reviews")
        
        print("\nMining patterns in training set...")
        # Use larger min_support for big dataset
        min_support = 50 if train_count > 100000 else 5
        max_customers = 2000 if train_count > 100000 else 1000
        print(f"  Parameters: min_support={min_support}, max_customers={max_customers}")
        
        train_patterns = self.find_frequent_patterns(
            min_support=min_support, max_customers=max_customers, dataset='train'
        )
        
        print(f"Found {len(train_patterns['top_patterns'])} patterns")
        
        print("\nValidating patterns in test set...")
        validated = self.validate_patterns_in_test(train_patterns, max_customers=max_customers)
        
        print("\nFinding customers for top patterns...")
        for pattern in validated[:5]:
            customers = self.find_customers_for_pattern(
                pattern['products'], dataset='train'
            )
            pattern['sample_customers'] = customers[:10]  # Store sample
        
        results = {
            'train_count': train_count,
            'test_count': test_count,
            'train_stats': {
                'num_transactions': train_patterns['num_transactions'],
                'num_frequent_items': len(train_patterns['frequent_items']),
                'num_frequent_pairs': len(train_patterns['frequent_pairs']),
                'elapsed_time': train_patterns['elapsed_time']
            },
            'patterns': validated
        }
        
        return results
    
    def split_train_test(self, train_ratio=0.7):
        """Split reviews into train/test sets using efficient batch processing"""
        print("  Using batch processing for large dataset...")
        
        with self.driver.session(database=self.database) as session:
            # Get total count first
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                RETURN count(r) as total
            """)
            total_reviews = result.single()['total']
            print(f"  Total reviews: {total_reviews:,}")
            
            # Process in batches to avoid memory issues
            batch_size = 50000
            print(f"  Processing in batches of {batch_size:,}...")
            
            # Process train and test together in batches
            processed = 0
            while processed < total_reviews:
                print(f"    Processed {processed:,} / {total_reviews:,} ({100*processed/total_reviews:.1f}%)", end='\r')
                
                session.run("""
                    MATCH ()-[r:REVIEWED]->()
                    WHERE r.dataset IS NULL
                    WITH r, rand() as random_val
                    LIMIT $batch_size
                    SET r.dataset = CASE 
                        WHEN random_val < $train_ratio THEN 'train'
                        ELSE 'test'
                    END
                """, batch_size=batch_size, train_ratio=train_ratio)
                
                # Check progress
                result = session.run("""
                    MATCH ()-[r:REVIEWED]->()
                    WHERE r.dataset IS NOT NULL
                    RETURN count(r) as count
                """)
                processed = result.single()['count']
            
            print(f"    Processed {total_reviews:,} / {total_reviews:,} (100.0%)")
            
            # Count splits
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                WHERE r.dataset = 'train'
                RETURN count(r) as count
            """)
            train_count = result.single()['count']
            
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                WHERE r.dataset = 'test'
                RETURN count(r) as count
            """)
            test_count = result.single()['count']
        
        return train_count, test_count
    
    def find_frequent_patterns(self, min_support=5, max_customers=500, dataset='train'):
        """Find frequent co-purchasing patterns"""
        start_time = time.time()
        
        with self.driver.session(database=self.database) as session:
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
            
            # Count individual items
            item_counts = defaultdict(int)
            for transaction in transactions:
                for item in transaction:
                    item_counts[item] += 1
            
            # Filter frequent items
            frequent_items = {item: count for item, count in item_counts.items() 
                            if count >= min_support}
            
            # Count pairs
            pair_counts = defaultdict(int)
            for transaction in transactions:
                items = [item for item in transaction if item in frequent_items]
                for i, item1 in enumerate(items):
                    for item2 in items[i+1:]:
                        pair = tuple(sorted([item1, item2]))
                        pair_counts[pair] += 1
            
            # Filter frequent pairs
            frequent_pairs = {pair: count for pair, count in pair_counts.items() 
                            if count >= min_support}
            
            # Get top patterns
            top_pairs = sorted(frequent_pairs.items(), key=lambda x: x[1], reverse=True)[:30]
            
            # Get true support counts from full dataset for accurate confidence
            print("Calculating true support counts for confidence...")
            true_support = {}
            for pair, _ in top_pairs:
                for asin in pair:
                    if asin not in true_support:
                        result = session.run("""
                            MATCH (c:Customer)-[r:REVIEWED]->(p:Product {asin: $asin})
                            WHERE r.dataset = $dataset
                            RETURN count(DISTINCT c) as count
                        """, asin=asin, dataset=dataset)
                        record = result.single()
                        true_support[asin] = record['count'] if record else 0
            
            patterns = []
            for pair, support in top_pairs:
                result = session.run("""
                    MATCH (p1:Product {asin: $asin1})
                    MATCH (p2:Product {asin: $asin2})
                    RETURN p1.title as title1, p2.title as title2,
                           p1.avg_rating as rating1, p2.avg_rating as rating2
                """, asin1=pair[0], asin2=pair[1])
                
                record = result.single()
                if record:
                    # Calculate confidence using true support from full dataset
                    # This shows: P(bought B | bought A) across all customers
                    confidence = support / true_support[pair[0]] if true_support[pair[0]] > 0 else 0
                    
                    # Also calculate "lift" which accounts for item popularity
                    # Lift > 1 means items are purchased together more than random
                    expected = (true_support[pair[0]] * true_support[pair[1]]) / len(transactions)
                    lift = support / expected if expected > 0 else 0
                    
                    patterns.append({
                        'products': pair,
                        'titles': [record['title1'], record['title2']],
                        'ratings': [record['rating1'], record['rating2']],
                        'support': support,
                        'confidence': float(confidence),
                        'lift': float(lift)
                    })
        
        elapsed = time.time() - start_time
        
        return {
            'frequent_items': {k: v for k, v in frequent_items.items()},
            'frequent_pairs': {str(k): v for k, v in frequent_pairs.items()},
            'top_patterns': patterns,
            'num_transactions': len(transactions),
            'elapsed_time': elapsed
        }
    
    def validate_patterns_in_test(self, train_patterns, max_customers=500):
        """Validate patterns in test set"""
        train_pairs = [p['products'] for p in train_patterns['top_patterns']]
        
        with self.driver.session(database=self.database) as session:
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
            for pattern in train_patterns['top_patterns']:
                pair = pattern['products']
                test_support = test_pair_counts.get(pair, 0)
                
                validated_patterns.append({
                    'products': list(pair),
                    'titles': pattern['titles'],
                    'ratings': pattern['ratings'],
                    'train_support': pattern['support'],
                    'test_support': test_support,
                    'total_support': pattern['support'] + test_support,
                    'confidence': pattern['confidence']
                })
        
        # Sort by total support
        validated_patterns.sort(key=lambda x: x['total_support'], reverse=True)
        
        return validated_patterns
    
    def find_customers_for_pattern(self, product_pair, dataset='train'):
        """Find customers who purchased both products"""
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH (c:Customer)-[r1:REVIEWED]->(p1:Product {asin: $asin1})
                MATCH (c)-[r2:REVIEWED]->(p2:Product {asin: $asin2})
                WHERE r1.dataset = $dataset AND r2.dataset = $dataset
                RETURN c.id as customer_id, 
                       r1.rating as rating1, r2.rating as rating2
                LIMIT 100
            """, asin1=product_pair[0], asin2=product_pair[1], dataset=dataset)
            
            customers = []
            for record in result:
                customers.append({
                    'customer_id': record['customer_id'],
                    'rating1': int(record['rating1']),
                    'rating2': int(record['rating2'])
                })
        
        return customers
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()


def main():
    """Main execution function"""
    generator = ResultGenerator()
    
    print("=" * 70)
    print("PRE-GENERATING RESULTS FOR GUI")
    print("=" * 70)
    
    # Check dataset size
    with generator.driver.session(database=generator.database) as session:
        result = session.run("MATCH ()-[r:REVIEWED]->() RETURN count(r) as count")
        review_count = result.single()['count']
        
    if review_count > 1000000:
        print("\n⚠️  LARGE DATASET DETECTED")
        print(f"   Reviews: {review_count:,}")
        print("   This will take 5-10 minutes. Progress will be shown...\n")
    
    # Generate database stats
    print("\n[1/3] Gathering database statistics...")
    stats = generator.get_database_stats()
    print(f"  Products: {stats['total_products']:,}")
    print(f"  Customers: {stats['total_customers']:,}")
    print(f"  Reviews: {stats['total_reviews']:,}")
    
    # Generate query results
    print("\n[2/3] Generating sample query results...")
    queries = generator.generate_sample_queries()
    print(f"  Generated {len(queries)} sample queries")
    
    # Generate pattern mining results
    print("\n[3/3] Running pattern mining algorithms...")
    patterns = generator.generate_pattern_mining_results()
    print(f"  Found {len(patterns['patterns'])} validated patterns")
    
    # Save all results
    results = {
        'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'stats': stats,
        'queries': queries,
        'patterns': patterns
    }
    
    output_file = 'gui_results.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✓ Results saved to {output_file}")
    print(f"  File size: {len(json.dumps(results)) / 1024:.1f} KB")
    
    generator.close()
    print("\n" + "=" * 70)
    print("COMPLETE - Ready to launch GUI")
    print("=" * 70)


if __name__ == "__main__":
    main()
