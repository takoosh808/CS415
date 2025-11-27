"""
Pre-generate pattern mining results for GUI demonstration
Saves results to JSON files that can be loaded quickly by the GUI
"""
from neo4j import GraphDatabase
import json
import time
from collections import defaultdict


class ResultGenerator:
    
    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                          auth=("neo4j", "Password"))
        self.database = "neo4j"
    
    def generate_all_results(self):
        """Generate and save all results"""
        print("="*60)
        print("PRE-GENERATING GUI RESULTS")
        print("="*60)
        
        # Generate database stats
        print("\n1. Generating database statistics...")
        stats = self.get_database_stats()
        with open('gui_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"   ✓ Saved to gui_stats.json")
        
        # Generate sample queries
        print("\n2. Generating sample query results...")
        queries = self.generate_sample_queries()
        with open('gui_queries.json', 'w') as f:
            json.dump(queries, f, indent=2)
        print(f"   ✓ Saved {len(queries)} queries to gui_queries.json")
        
        # Generate pattern mining results
        print("\n3. Generating pattern mining results...")
        patterns = self.generate_pattern_mining_results()
        with open('gui_patterns.json', 'w') as f:
            json.dump(patterns, f, indent=2)
        print(f"   ✓ Saved {len(patterns['patterns'])} patterns to gui_patterns.json")
        
        print("\n" + "="*60)
        print("GENERATION COMPLETE")
        print("="*60)
        print("\nFiles created:")
        print("  - gui_stats.json")
        print("  - gui_queries.json")
        print("  - gui_patterns.json")
        print("\nYou can now run the GUI application.")
    
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
            
            # Group distribution
            result = session.run("""
                MATCH (p:Product)
                WITH p.group as group, count(p) as count
                RETURN group, count
                ORDER BY count DESC
            """)
            stats['group_distribution'] = {r['group']: r['count'] for r in result}
            
            # Rating distribution
            result = session.run("""
                MATCH (p:Product)
                WHERE p.avg_rating > 0
                WITH round(p.avg_rating) as rating_bucket, count(p) as count
                RETURN rating_bucket, count
                ORDER BY rating_bucket
            """)
            stats['rating_distribution'] = {int(r['rating_bucket']): r['count'] 
                                           for r in result}
            
            # Train/test split
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                WHERE r.dataset = 'train'
                RETURN count(r) as count
            """)
            stats['train_count'] = result.single()['count']
            
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                WHERE r.dataset = 'test'
                RETURN count(r) as count
            """)
            stats['test_count'] = result.single()['count']
            
        return stats
    
    def generate_sample_queries(self):
        """Generate results for sample queries"""
        queries = []
        
        # Query 1: High-rated DVDs
        print("   - High-rated DVDs...")
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
        print("   - Popular Books...")
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
        print("   - Top Music Albums...")
        query3 = {
            'name': 'Top Music Albums',
            'description': 'Music with rating >= 4.0',
            'conditions': {'group': 'Music', 'min_rating': 4.0},
            'k': 20
        }
        query3['results'], query3['execution_time'] = self.execute_query(
            query3['conditions'], query3['k']
        )
        queries.append(query3)
        
        # Query 4: Highest Rated Products
        print("   - Highest Rated Products...")
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
        
        return queries
    
    def execute_query(self, conditions, k=10):
        """Execute a query against the database"""
        start_time = time.time()
        
        where_clauses = []
        params = {}
        
        if 'min_rating' in conditions:
            where_clauses.append("p.avg_rating >= $min_rating")
            params['min_rating'] = conditions['min_rating']
            
        if 'min_reviews' in conditions:
            where_clauses.append("p.total_reviews >= $min_reviews")
            params['min_reviews'] = conditions['min_reviews']
            
        if 'group' in conditions:
            where_clauses.append("p.group = $group")
            params['group'] = conditions['group']
        
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
        # Check if train/test split exists
        with self.driver.session(database=self.database) as session:
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                WHERE r.dataset IS NOT NULL
                RETURN count(r) as count
            """)
            split_count = result.single()['count']
            
            if split_count == 0:
                print("   - Creating train/test split...")
                train_count, test_count = self.split_train_test()
            else:
                print("   - Train/test split already exists")
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
        
        print(f"     Training set: {train_count:,} reviews")
        print(f"     Testing set: {test_count:,} reviews")
        
        # Mine patterns
        print("   - Mining patterns in training set...")
        min_support = 10
        max_customers = 500
        
        train_patterns = self.mine_patterns('train', min_support, max_customers)
        print(f"     Found {len(train_patterns)} frequent patterns")
        
        # Validate in test set
        print("   - Validating patterns in test set...")
        validated = self.validate_patterns(train_patterns)
        
        # Sort by test support
        validated.sort(key=lambda x: x['test_support'], reverse=True)
        
        results = {
            'train_count': train_count,
            'test_count': test_count,
            'min_support': min_support,
            'max_customers': max_customers,
            'patterns': validated[:50]  # Top 50 patterns
        }
        
        return results
    
    def split_train_test(self, train_ratio=0.7):
        """Split reviews into train/test sets"""
        with self.driver.session(database=self.database) as session:
            # Get total count
            result = session.run("""
                MATCH ()-[r:REVIEWED]->()
                RETURN count(r) as total
            """)
            total_reviews = result.single()['total']
            
            # Split in batches
            batch_size = 10000
            processed = 0
            
            while processed < total_reviews:
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
                processed += batch_size
            
            # Count final results
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
    
    def mine_patterns(self, dataset, min_support, max_customers):
        """Mine frequent co-purchasing patterns"""
        with self.driver.session(database=self.database) as session:
            # Get customer transactions
            result = session.run("""
                MATCH (c:Customer)-[r:REVIEWED]->(p:Product)
                WHERE r.dataset = $dataset
                WITH c, collect(p.asin) as products
                WHERE size(products) >= 2
                RETURN c.id as customer_id, products
                LIMIT $max_customers
            """, dataset=dataset, max_customers=max_customers)
            
            transactions = [set(record['products']) for record in result]
            
            # Count item frequencies
            item_counts = defaultdict(int)
            for transaction in transactions:
                for item in transaction:
                    item_counts[item] += 1
            
            # Find frequent items
            frequent_items = {item for item, count in item_counts.items() 
                            if count >= min_support}
            
            # Count pair frequencies
            pair_counts = defaultdict(int)
            for transaction in transactions:
                items = [item for item in transaction if item in frequent_items]
                for i, item1 in enumerate(items):
                    for item2 in items[i+1:]:
                        pair = tuple(sorted([item1, item2]))
                        pair_counts[pair] += 1
            
            # Find frequent pairs
            frequent_pairs = [(pair, count) for pair, count in pair_counts.items() 
                            if count >= min_support]
            
            # Sort by support
            frequent_pairs.sort(key=lambda x: x[1], reverse=True)
            
            return [{'products': list(pair), 'support': count} 
                   for pair, count in frequent_pairs[:100]]  # Top 100
    
    def validate_patterns(self, train_patterns):
        """Validate patterns in test set"""
        validated = []
        
        with self.driver.session(database=self.database) as session:
            for pattern_info in train_patterns[:50]:  # Validate top 50
                products = pattern_info['products']
                train_support = pattern_info['support']
                
                # Count in test set
                result = session.run("""
                    MATCH (c:Customer)-[r1:REVIEWED]->(p1:Product {asin: $asin1})
                    MATCH (c)-[r2:REVIEWED]->(p2:Product {asin: $asin2})
                    WHERE r1.dataset = 'test' AND r2.dataset = 'test'
                    RETURN count(DISTINCT c) as support
                """, asin1=products[0], asin2=products[1])
                
                test_support = result.single()['support']
                confidence = test_support / train_support if train_support > 0 else 0
                
                # Get product titles
                result = session.run("""
                    MATCH (p1:Product {asin: $asin1})
                    MATCH (p2:Product {asin: $asin2})
                    RETURN p1.title as title1, p2.title as title2
                """, asin1=products[0], asin2=products[1])
                
                record = result.single()
                
                validated.append({
                    'products': products,
                    'titles': [record['title1'] or 'Unknown', 
                              record['title2'] or 'Unknown'],
                    'train_support': train_support,
                    'test_support': test_support,
                    'confidence': round(confidence, 3)
                })
        
        return validated
    
    def close(self):
        """Close database connection"""
        if self.driver:
            self.driver.close()


def main():
    """Main entry point"""
    try:
        generator = ResultGenerator()
        generator.generate_all_results()
        generator.close()
        print("\n✓ Success! Results are ready for the GUI.")
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        print("\nMake sure Neo4j is running and data is loaded.")


if __name__ == "__main__":
    main()
