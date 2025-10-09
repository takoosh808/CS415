"""
Test Script to Check if Everything Worked
========================================

runs a bunch of queries to make sure our data loaded correctly 
and measures how fast neo4j can answer them (hopefully fast enough for good grades)
"""

from neo4j import GraphDatabase
import time
import json
import logging
from typing import Dict, List, Tuple, Any

# logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class Neo4jValidator:
    """runs test queries and checks if our database actually works"""
    
    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "Password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.validation_results = {}  # stores test results
        self.performance_results = {}  # stores timing info
    
    def close(self):
        if self.driver:
            self.driver.close()
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run all validation tests and collect results"""
        logger.info("Starting comprehensive validation...")
        
        # Data volume validation
        self.validate_data_volume()
        
        # Data quality validation
        self.validate_data_quality()
        
        # Relationship validation
        self.validate_relationships()
        
        # Performance benchmarks
        self.run_performance_benchmarks()
        
        # Business intelligence queries
        self.run_business_queries()
        
        return {
            'validation_results': self.validation_results,
            'performance_results': self.performance_results
        }
    
    def validate_data_volume(self):
        """Validate that expected data volumes are present"""
        logger.info("Validating data volume...")
        
        volume_queries = {
            'total_products': "MATCH (p:Product) RETURN count(p) as count",
            'total_categories': "MATCH (c:Category) RETURN count(c) as count",
            'total_customers': "MATCH (c:Customer) RETURN count(c) as count",
            'similar_relationships': "MATCH ()-[r:SIMILAR_TO]->() RETURN count(r) as count",
            'category_relationships': "MATCH ()-[r:BELONGS_TO]->() RETURN count(r) as count",
            'review_relationships': "MATCH ()-[r:REVIEWED]->() RETURN count(r) as count"
        }
        
        volume_results = {}
        with self.driver.session() as session:
            for metric, query in volume_queries.items():
                result = session.run(query).single()
                count = result['count']
                volume_results[metric] = count
                logger.info(f"{metric}: {count:,}")
        
        self.validation_results['data_volume'] = volume_results
    
    def validate_data_quality(self):
        """Validate data quality and integrity"""
        logger.info("Validating data quality...")
        
        quality_queries = {
            'products_with_title': "MATCH (p:Product) WHERE p.title IS NOT NULL AND p.title <> '' RETURN count(p) as count",
            'products_with_group': "MATCH (p:Product) WHERE p.group IS NOT NULL AND p.group <> '' RETURN count(p) as count",
            'products_with_valid_ratings': "MATCH (p:Product) WHERE p.avg_rating >= 1 AND p.avg_rating <= 5 RETURN count(p) as count",
            'products_with_categories': "MATCH (p:Product)-[:BELONGS_TO]->() RETURN count(DISTINCT p) as count",
            'products_with_reviews': "MATCH (p:Product)<-[:REVIEWED]-() RETURN count(DISTINCT p) as count",
            'unique_asins': "MATCH (p:Product) RETURN count(DISTINCT p.asin) as count",
            'duplicate_asins': "MATCH (p:Product) WITH p.asin as asin, count(p) as cnt WHERE cnt > 1 RETURN count(asin) as count"
        }
        
        quality_results = {}
        with self.driver.session() as session:
            for metric, query in quality_queries.items():
                result = session.run(query).single()
                count = result['count']
                quality_results[metric] = count
                logger.info(f"{metric}: {count:,}")
        
        # Calculate quality percentages
        total_products = self.validation_results['data_volume']['total_products']
        quality_results['title_coverage'] = (quality_results['products_with_title'] / total_products) * 100
        quality_results['group_coverage'] = (quality_results['products_with_group'] / total_products) * 100
        quality_results['category_coverage'] = (quality_results['products_with_categories'] / total_products) * 100
        
        self.validation_results['data_quality'] = quality_results
    
    def validate_relationships(self):
        """Validate relationship integrity"""
        logger.info("Validating relationships...")
        
        relationship_queries = {
            'products_with_similarities': "MATCH (p:Product)-[:SIMILAR_TO]->() RETURN count(DISTINCT p) as count",
            'avg_similarities_per_product': "MATCH (p:Product)-[:SIMILAR_TO]->(s) RETURN avg(count(s)) as avg",
            'categories_with_products': "MATCH (c:Category)<-[:BELONGS_TO]-() RETURN count(DISTINCT c) as count",
            'customers_with_reviews': "MATCH (c:Customer)-[:REVIEWED]->() RETURN count(DISTINCT c) as count",
            'avg_reviews_per_customer': "MATCH (c:Customer)-[r:REVIEWED]->() RETURN avg(count(r)) as avg",
            'avg_reviews_per_product': "MATCH (p:Product)<-[r:REVIEWED]-() RETURN avg(count(r)) as avg"
        }
        
        relationship_results = {}
        with self.driver.session() as session:
            for metric, query in relationship_queries.items():
                result = session.run(query).single()
                value = result.get('count') or result.get('avg', 0)
                relationship_results[metric] = round(value, 2) if isinstance(value, float) else value
                logger.info(f"{metric}: {relationship_results[metric]}")
        
        self.validation_results['relationships'] = relationship_results
    
    def run_performance_benchmarks(self):
        """Run performance benchmarks on common queries"""
        logger.info("Running performance benchmarks...")
        
        benchmark_queries = [
            ("Point lookup by ASIN", "MATCH (p:Product {asin: $asin}) RETURN p", {"asin": "0827229534"}),
            ("Category filter", "MATCH (p:Product)-[:BELONGS_TO]->(c:Category {name: $category}) RETURN count(p)", {"category": "Book"}),
            ("Similarity traversal", "MATCH (p:Product {asin: $asin})-[:SIMILAR_TO]->(s) RETURN s.title, s.asin LIMIT 10", {"asin": "0827229534"}),
            ("Rating aggregation", "MATCH (p:Product) WHERE p.avg_rating IS NOT NULL RETURN avg(p.avg_rating) as avg_rating", {}),
            ("Top rated products", "MATCH (p:Product) WHERE p.avg_rating IS NOT NULL RETURN p.title, p.avg_rating ORDER BY p.avg_rating DESC LIMIT 10", {}),
            ("Customer review count", "MATCH (c:Customer)-[r:REVIEWED]->() RETURN c.customer_id, count(r) as reviews ORDER BY reviews DESC LIMIT 10", {}),
            ("Multi-hop similarity", "MATCH path = (p1:Product {asin: $asin})-[:SIMILAR_TO*1..2]->(p2) RETURN count(path)", {"asin": "0827229534"}),
            ("Complex join query", "MATCH (p:Product)-[:BELONGS_TO]->(c:Category)<-[:BELONGS_TO]-(similar:Product)-[:SIMILAR_TO]->(p) RETURN count(similar)", {})
        ]
        
        performance_results = []
        with self.driver.session() as session:
            for description, query, params in benchmark_queries:
                # Warm up
                session.run(query, params)
                
                # Actual benchmark (run 3 times, take average)
                times = []
                for _ in range(3):
                    start_time = time.time()
                    result = session.run(query, params)
                    list(result)  # Consume all results
                    end_time = time.time()
                    times.append(end_time - start_time)
                
                avg_time = sum(times) / len(times)
                performance_results.append({
                    'query': description,
                    'avg_time_ms': round(avg_time * 1000, 2),
                    'min_time_ms': round(min(times) * 1000, 2),
                    'max_time_ms': round(max(times) * 1000, 2)
                })
                
                logger.info(f"{description}: {avg_time * 1000:.2f}ms avg")
        
        self.performance_results['query_benchmarks'] = performance_results
    
    def run_business_queries(self):
        """Run business intelligence queries to demonstrate database capabilities"""
        logger.info("Running business intelligence queries...")
        
        business_queries = [
            ("Product group distribution", "MATCH (p:Product) RETURN p.group, count(p) as count ORDER BY count DESC"),
            ("Top categories by product count", "MATCH (c:Category)<-[:BELONGS_TO]-(p:Product) RETURN c.name, count(p) as products ORDER BY products DESC LIMIT 10"),
            ("Rating distribution", "MATCH (p:Product) WHERE p.avg_rating IS NOT NULL WITH floor(p.avg_rating) as rating_bucket RETURN rating_bucket, count(p) as products ORDER BY rating_bucket"),
            ("Most reviewed products", "MATCH (p:Product)<-[r:REVIEWED]-() RETURN p.title, p.asin, count(r) as review_count ORDER BY review_count DESC LIMIT 10"),
            ("Most active reviewers", "MATCH (c:Customer)-[r:REVIEWED]->() RETURN c.customer_id, count(r) as reviews, avg(r.rating) as avg_rating ORDER BY reviews DESC LIMIT 10"),
            ("Products with best salesrank", "MATCH (p:Product) WHERE p.salesrank IS NOT NULL RETURN p.title, p.salesrank, p.avg_rating ORDER BY p.salesrank ASC LIMIT 10"),
            ("Category recommendation strength", "MATCH (c:Category)<-[:BELONGS_TO]-(p1:Product)-[:SIMILAR_TO]->(p2:Product)-[:BELONGS_TO]->(c) RETURN c.name, count(*) as internal_similarities ORDER BY internal_similarities DESC LIMIT 10")
        ]
        
        business_results = {}
        with self.driver.session() as session:
            for description, query in business_queries:
                start_time = time.time()
                result = session.run(query)
                records = [dict(record) for record in result]
                query_time = time.time() - start_time
                
                business_results[description] = {
                    'results': records[:5],  # Store top 5 results
                    'total_results': len(records),
                    'query_time_ms': round(query_time * 1000, 2)
                }
                
                logger.info(f"{description}: {len(records)} results in {query_time * 1000:.2f}ms")
        
        self.validation_results['business_intelligence'] = business_results
    
    def generate_validation_report(self, output_file: str = "validation_report.json"):
        """Generate comprehensive validation report"""
        logger.info("Generating validation report...")
        
        report = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'database_info': self._get_database_info(),
            'validation_results': self.validation_results,
            'performance_results': self.performance_results,
            'summary': self._generate_summary()
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Validation report saved to {output_file}")
        return report
    
    def _get_database_info(self) -> Dict[str, Any]:
        """Get Neo4j database information"""
        with self.driver.session() as session:
            # Neo4j version
            version_result = session.run("CALL dbms.components() YIELD name, versions RETURN name, versions[0] as version")
            version_info = {record['name']: record['version'] for record in version_result}
            
            # Database size
            size_result = session.run("CALL apoc.meta.stats() YIELD nodeCount, relCount, labelCount, relTypeCount").single()
            
            return {
                'version_info': version_info,
                'node_count': size_result['nodeCount'] if size_result else 0,
                'relationship_count': size_result['relCount'] if size_result else 0,
                'label_count': size_result['labelCount'] if size_result else 0,
                'relationship_type_count': size_result['relTypeCount'] if size_result else 0
            }
    
    def _generate_summary(self) -> Dict[str, Any]:
        """Generate validation summary"""
        total_products = self.validation_results['data_volume']['total_products']
        
        return {
            'data_ingestion_success': total_products > 0,
            'target_size_met': 10000 <= total_products <= 100000,  # Within milestone requirements
            'data_quality_score': self._calculate_quality_score(),
            'performance_grade': self._assess_performance(),
            'recommendation': self._generate_recommendation()
        }
    
    def _calculate_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)"""
        quality = self.validation_results['data_quality']
        total_products = self.validation_results['data_volume']['total_products']
        
        scores = [
            quality['title_coverage'],
            quality['group_coverage'],
            quality['category_coverage'],
            100 - (quality['duplicate_asins'] / total_products * 100)  # Penalize duplicates
        ]
        
        return round(sum(scores) / len(scores), 2)
    
    def _assess_performance(self) -> str:
        """Assess query performance"""
        benchmarks = self.performance_results['query_benchmarks']
        avg_time = sum(b['avg_time_ms'] for b in benchmarks) / len(benchmarks)
        
        if avg_time < 50:
            return "Excellent"
        elif avg_time < 200:
            return "Good"
        elif avg_time < 500:
            return "Fair"
        else:
            return "Needs Optimization"
    
    def _generate_recommendation(self) -> str:
        """Generate recommendations based on validation results"""
        quality_score = self._calculate_quality_score()
        performance_grade = self._assess_performance()
        
        if quality_score > 90 and performance_grade in ["Excellent", "Good"]:
            return "Database is ready for production use with excellent data quality and performance."
        elif quality_score > 80:
            return "Database has good data quality. Consider performance tuning for optimal results."
        else:
            return "Consider data cleansing improvements and performance optimization before production use."

def main():
    """Main validation execution"""
    validator = Neo4jValidator()
    
    try:
        # Run comprehensive validation
        results = validator.run_comprehensive_validation()
        
        # Generate report
        report = validator.generate_validation_report()
        
        # Print summary
        logger.info("=" * 60)
        logger.info("VALIDATION SUMMARY:")
        logger.info(f"Data Quality Score: {report['summary']['data_quality_score']}/100")
        logger.info(f"Performance Grade: {report['summary']['performance_grade']}")
        logger.info(f"Recommendation: {report['summary']['recommendation']}")
        logger.info("=" * 60)
        
    finally:
        validator.close()

if __name__ == "__main__":
    main()