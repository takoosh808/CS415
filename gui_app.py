"""
Amazon Co-Purchasing Analysis GUI
A demonstration application for querying product data and analyzing co-purchasing patterns
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from neo4j import GraphDatabase
import time


class AmazonAnalysisGUI:
    """Main GUI application for Amazon co-purchasing analysis"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Amazon Co-Purchasing Analysis System")
        self.root.geometry("1200x800")
        
        # Database connection
        self.driver = None
        self.connected = False
        
        # Pre-generated results
        self.pregenerated_patterns = None
        self.pregenerated_stats = None
        
        # Setup UI
        self.setup_ui()
        
        # Try to connect to database
        self.connect_database()
        
        # Load pre-generated results
        self.load_pregenerated_results()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Create main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        # Title
        title = ttk.Label(main_frame, text="Amazon Co-Purchasing Analysis", 
                         font=('Arial', 16, 'bold'))
        title.grid(row=0, column=0, pady=(0, 10))
        
        # Connection status
        self.status_label = ttk.Label(main_frame, text="Status: Disconnected", 
                                     font=('Arial', 9))
        self.status_label.grid(row=0, column=0, sticky=tk.E, pady=(0, 10))
        
        # Create notebook (tabs)
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Create tabs
        self.setup_query_tab()
        self.setup_pattern_tab()
    
    def setup_query_tab(self):
        """Setup the complex query tab"""
        query_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(query_frame, text="Complex Queries")
        
        query_frame.columnconfigure(0, weight=1)
        query_frame.rowconfigure(2, weight=1)
        
        # Query builder section
        builder_frame = ttk.LabelFrame(query_frame, text="Query Builder", padding="10")
        builder_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Product group filter
        ttk.Label(builder_frame, text="Product Group:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.group_var = tk.StringVar(value="")
        group_entry = ttk.Entry(builder_frame, textvariable=self.group_var, width=15)
        group_entry.grid(row=0, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Rating filters
        ttk.Label(builder_frame, text="Min Rating:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.min_rating_var = tk.StringVar(value="")
        min_rating_entry = ttk.Entry(builder_frame, textvariable=self.min_rating_var, width=10)
        min_rating_entry.grid(row=1, column=1, sticky=tk.W, pady=5, padx=5)
        
        ttk.Label(builder_frame, text="Max Rating:").grid(row=1, column=3, sticky=tk.W, pady=5, padx=(20, 0))
        self.max_rating_var = tk.StringVar(value="")
        max_rating_entry = ttk.Entry(builder_frame, textvariable=self.max_rating_var, width=10)
        max_rating_entry.grid(row=1, column=4, sticky=tk.W, pady=5, padx=5)
        
        # Review count filters
        ttk.Label(builder_frame, text="Min Reviews:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.min_reviews_var = tk.StringVar(value="")
        min_reviews_entry = ttk.Entry(builder_frame, textvariable=self.min_reviews_var, width=10)
        min_reviews_entry.grid(row=2, column=1, sticky=tk.W, pady=5, padx=5)
        
        ttk.Label(builder_frame, text="Max Reviews:").grid(row=2, column=3, sticky=tk.W, pady=5, padx=(20, 0))
        self.max_reviews_var = tk.StringVar(value="")
        max_reviews_entry = ttk.Entry(builder_frame, textvariable=self.max_reviews_var, width=10)
        max_reviews_entry.grid(row=2, column=4, sticky=tk.W, pady=5, padx=5)
        
        # Salesrank filters
        ttk.Label(builder_frame, text="Min Sales Rank:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.min_salesrank_var = tk.StringVar(value="")
        min_salesrank_entry = ttk.Entry(builder_frame, textvariable=self.min_salesrank_var, width=15)
        min_salesrank_entry.grid(row=3, column=1, sticky=tk.W, pady=5, padx=5)
        
        ttk.Label(builder_frame, text="Max Sales Rank:").grid(row=3, column=3, sticky=tk.W, pady=5, padx=(20, 0))
        self.max_salesrank_var = tk.StringVar(value="")
        max_salesrank_entry = ttk.Entry(builder_frame, textvariable=self.max_salesrank_var, width=15)
        max_salesrank_entry.grid(row=3, column=4, sticky=tk.W, pady=5, padx=5)
        
        # Result limit
        ttk.Label(builder_frame, text="Limit (k):").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.limit_var = tk.StringVar(value="20")
        limit_entry = ttk.Entry(builder_frame, textvariable=self.limit_var, width=10)
        limit_entry.grid(row=4, column=1, sticky=tk.W, pady=5, padx=5)
        
        # Execute and Clear buttons
        btn_frame = ttk.Frame(builder_frame)
        btn_frame.grid(row=4, column=3, columnspan=2, sticky=tk.E, pady=5, padx=5)
        
        clear_btn = ttk.Button(btn_frame, text="Clear All", 
                              command=self.clear_query_fields)
        clear_btn.grid(row=0, column=0, padx=(0, 5))
        
        execute_btn = ttk.Button(btn_frame, text="Execute Query", 
                                command=self.execute_query)
        execute_btn.grid(row=0, column=1)
        
        # Results section
        results_frame = ttk.LabelFrame(query_frame, text="Query Results", padding="10")
        results_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Results treeview
        columns = ("ASIN", "Title", "Group", "Rating", "Reviews", "Sales Rank")
        self.query_tree = ttk.Treeview(results_frame, columns=columns, show='headings', height=15)
        
        # Configure columns
        self.query_tree.heading("ASIN", text="ASIN")
        self.query_tree.heading("Title", text="Title")
        self.query_tree.heading("Group", text="Group")
        self.query_tree.heading("Rating", text="Avg Rating")
        self.query_tree.heading("Reviews", text="Reviews")
        self.query_tree.heading("Sales Rank", text="Sales Rank")
        
        self.query_tree.column("ASIN", width=100)
        self.query_tree.column("Title", width=400)
        self.query_tree.column("Group", width=80)
        self.query_tree.column("Rating", width=80)
        self.query_tree.column("Reviews", width=80)
        self.query_tree.column("Sales Rank", width=100)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.query_tree.yview)
        self.query_tree.configure(yscroll=scrollbar.set)
        
        self.query_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Execution info
        self.query_info_label = ttk.Label(query_frame, text="", font=('Arial', 9))
        self.query_info_label.grid(row=3, column=0, sticky=tk.W, pady=(5, 0))
    
    def setup_pattern_tab(self):
        """Setup the pattern mining tab"""
        pattern_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(pattern_frame, text="Pattern Mining")
        
        pattern_frame.columnconfigure(0, weight=1)
        pattern_frame.rowconfigure(1, weight=1)
        
        # Control section
        control_frame = ttk.LabelFrame(pattern_frame, text="Pattern Mining Parameters", padding="10")
        control_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        ttk.Label(control_frame, text="Min Support:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.min_support_var = tk.StringVar(value="10")
        support_spin = ttk.Spinbox(control_frame, from_=5, to=100, increment=5,
                                  textvariable=self.min_support_var, width=10)
        support_spin.grid(row=0, column=1, sticky=tk.W, pady=5, padx=5)
        
        ttk.Label(control_frame, text="Max Customers:").grid(row=0, column=2, sticky=tk.W, pady=5, padx=(20, 0))
        self.max_customers_var = tk.StringVar(value="500")
        customers_spin = ttk.Spinbox(control_frame, from_=100, to=5000, increment=100,
                                    textvariable=self.max_customers_var, width=10)
        customers_spin.grid(row=0, column=3, sticky=tk.W, pady=5, padx=5)
        
        # Button frame for two buttons
        button_frame = ttk.Frame(control_frame)
        button_frame.grid(row=0, column=4, rowspan=2, padx=20)
        
        load_btn = ttk.Button(button_frame, text="Load Pre-Generated Results", 
                             command=self.load_pregenerated_patterns, width=24)
        load_btn.grid(row=0, column=0, pady=(0, 5))
        
        mine_btn = ttk.Button(button_frame, text="Mine Patterns Now (Real-Time)", 
                             command=self.mine_patterns_realtime, width=24)
        mine_btn.grid(row=1, column=0)
        
        # Results with visualization
        results_container = ttk.Frame(pattern_frame)
        results_container.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        results_container.columnconfigure(0, weight=2)
        results_container.columnconfigure(1, weight=1)
        results_container.rowconfigure(0, weight=1)
        
        # Pattern list
        list_frame = ttk.LabelFrame(results_container, text="Frequent Co-Purchasing Patterns", padding="10")
        list_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 5))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        
        columns = ("Rank", "Product 1", "Product 2", "Total Support", "Confidence")
        self.pattern_tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=20)
        
        self.pattern_tree.heading("Rank", text="Rank")
        self.pattern_tree.heading("Product 1", text="Product 1")
        self.pattern_tree.heading("Product 2", text="Product 2")
        self.pattern_tree.heading("Total Support", text="Total Support")
        self.pattern_tree.heading("Confidence", text="Confidence")
        
        self.pattern_tree.column("Rank", width=50)
        self.pattern_tree.column("Product 1", width=250)
        self.pattern_tree.column("Product 2", width=250)
        self.pattern_tree.column("Total Support", width=100)
        self.pattern_tree.column("Confidence", width=80)
        
        pattern_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.pattern_tree.yview)
        self.pattern_tree.configure(yscroll=pattern_scrollbar.set)
        
        self.pattern_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        pattern_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Visualization panel
        viz_frame = ttk.LabelFrame(results_container, text="Pattern Visualization", padding="10")
        viz_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S))
        viz_frame.columnconfigure(0, weight=1)
        viz_frame.rowconfigure(0, weight=1)
        
        self.pattern_canvas_frame = ttk.Frame(viz_frame)
        self.pattern_canvas_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Info label
        self.pattern_info_label = ttk.Label(pattern_frame, text="", font=('Arial', 9))
        self.pattern_info_label.grid(row=2, column=0, sticky=tk.W, pady=(5, 0))
    

    
    def connect_database(self):
        """Connect to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                              auth=("neo4j", "Password"))
            # Test connection
            with self.driver.session() as session:
                session.run("RETURN 1")
            self.connected = True
            self.status_label.config(text="Status: Connected to Neo4j", foreground="green")
        except Exception as e:
            self.connected = False
            self.status_label.config(text="Status: Database Offline", foreground="red")
            messagebox.showwarning("Connection Warning", 
                                 f"Could not connect to Neo4j database.\n{str(e)}\n\n"
                                 "The application will run with limited functionality.")
    
    def clear_query_fields(self):
        """Clear all query input fields"""
        self.group_var.set("")
        self.min_rating_var.set("")
        self.max_rating_var.set("")
        self.min_reviews_var.set("")
        self.max_reviews_var.set("")
        self.min_salesrank_var.set("")
        self.max_salesrank_var.set("")
        self.limit_var.set("20")
        
        # Clear results
        for item in self.query_tree.get_children():
            self.query_tree.delete(item)
        self.query_info_label.config(text="")
    
    def execute_query(self):
        """Execute a complex query against the database"""
        if not self.connected:
            messagebox.showerror("Error", "Not connected to database")
            return
        
        # Clear previous results
        for item in self.query_tree.get_children():
            self.query_tree.delete(item)
        
        # Build query conditions
        conditions = {}
        where_clauses = []
        params = {}
        
        # Product group filter
        group = self.group_var.get().strip()
        if group:
            where_clauses.append("p.group = $group")
            params['group'] = group
            conditions['group'] = group
        
        # Min rating filter
        min_rating_str = self.min_rating_var.get().strip()
        if min_rating_str:
            try:
                min_rating = float(min_rating_str)
                where_clauses.append("p.avg_rating >= $min_rating")
                params['min_rating'] = min_rating
                conditions['min_rating'] = f">= {min_rating}"
            except ValueError:
                messagebox.showerror("Input Error", f"Invalid Min Rating: '{min_rating_str}'\nPlease enter a number.")
                return
        
        # Max rating filter
        max_rating_str = self.max_rating_var.get().strip()
        if max_rating_str:
            try:
                max_rating = float(max_rating_str)
                where_clauses.append("p.avg_rating <= $max_rating")
                params['max_rating'] = max_rating
                conditions['max_rating'] = f"<= {max_rating}"
            except ValueError:
                messagebox.showerror("Input Error", f"Invalid Max Rating: '{max_rating_str}'\nPlease enter a number.")
                return
        
        # Min reviews filter
        min_reviews_str = self.min_reviews_var.get().strip()
        if min_reviews_str:
            try:
                min_reviews = int(min_reviews_str)
                where_clauses.append("p.total_reviews >= $min_reviews")
                params['min_reviews'] = min_reviews
                conditions['min_reviews'] = f">= {min_reviews}"
            except ValueError:
                messagebox.showerror("Input Error", f"Invalid Min Reviews: '{min_reviews_str}'\nPlease enter an integer.")
                return
        
        # Max reviews filter
        max_reviews_str = self.max_reviews_var.get().strip()
        if max_reviews_str:
            try:
                max_reviews = int(max_reviews_str)
                where_clauses.append("p.total_reviews <= $max_reviews")
                params['max_reviews'] = max_reviews
                conditions['max_reviews'] = f"<= {max_reviews}"
            except ValueError:
                messagebox.showerror("Input Error", f"Invalid Max Reviews: '{max_reviews_str}'\nPlease enter an integer.")
                return
        
        # Min salesrank filter
        min_salesrank_str = self.min_salesrank_var.get().strip()
        if min_salesrank_str:
            try:
                min_salesrank = int(min_salesrank_str)
                where_clauses.append("p.salesrank >= $min_salesrank AND p.salesrank > 0")
                params['min_salesrank'] = min_salesrank
                conditions['min_salesrank'] = f">= {min_salesrank}"
            except ValueError:
                messagebox.showerror("Input Error", f"Invalid Min Sales Rank: '{min_salesrank_str}'\nPlease enter an integer.")
                return
        
        # Max salesrank filter
        max_salesrank_str = self.max_salesrank_var.get().strip()
        if max_salesrank_str:
            try:
                max_salesrank = int(max_salesrank_str)
                where_clauses.append("p.salesrank <= $max_salesrank AND p.salesrank > 0")
                params['max_salesrank'] = max_salesrank
                conditions['max_salesrank'] = f"<= {max_salesrank}"
            except ValueError:
                messagebox.showerror("Input Error", f"Invalid Max Sales Rank: '{max_salesrank_str}'\nPlease enter an integer.")
                return
        
        # Limit
        try:
            limit = int(self.limit_var.get().strip())
            if limit <= 0:
                limit = 20
        except ValueError:
            limit = 20
        params['limit'] = limit
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "true"
        
        # Build and execute query
        query = f"""
            MATCH (p:Product)
            WHERE {where_clause}
            RETURN p.asin as asin, p.title as title, 
                   p.group as group, p.salesrank as salesrank,
                   p.avg_rating as avg_rating, p.total_reviews as total_reviews
            ORDER BY p.avg_rating DESC, p.total_reviews DESC
            LIMIT $limit
        """
        
        try:
            start_time = time.time()
            with self.driver.session() as session:
                result = session.run(query, params)
                products = [dict(record) for record in result]
            elapsed = time.time() - start_time
            
            # Display results
            for product in products:
                self.query_tree.insert('', tk.END, values=(
                    product.get('asin', 'N/A'),
                    product.get('title', 'N/A')[:60],
                    product.get('group', 'N/A'),
                    f"{product.get('avg_rating', 0):.2f}",
                    product.get('total_reviews', 0),
                    product.get('salesrank', 'N/A')
                ))
            
            # Update info
            self.query_info_label.config(
                text=f"Found {len(products)} products in {elapsed:.3f} seconds | Conditions: {conditions}"
            )
            
        except Exception as e:
            messagebox.showerror("Query Error", f"Error executing query:\n{str(e)}")
    
    def load_pregenerated_results(self):
        """Load pre-generated pattern mining results from JSON file"""
        try:
            if os.path.exists('gui_patterns.json'):
                with open('gui_patterns.json', 'r') as f:
                    self.pregenerated_patterns = json.load(f)
            
            if os.path.exists('gui_stats.json'):
                with open('gui_stats.json', 'r') as f:
                    self.pregenerated_stats = json.load(f)
        except Exception as e:
            print(f"Note: Could not load pre-generated results: {e}")
    
    def load_pregenerated_patterns(self):
        """Load and display pre-generated pattern mining results"""
        # Clear previous results
        for item in self.pattern_tree.get_children():
            self.pattern_tree.delete(item)
        
        # Check if pre-generated results exist
        if not self.pregenerated_patterns:
            messagebox.showwarning(
                "No Pre-Generated Results", 
                "Pre-generated results not found.\n\n"
                "Run: python pregenerate_gui_results.py\n\n"
                "Or use 'Mine Patterns Now' button for real-time mining."
            )
            self.pattern_info_label.config(
                text="Run: python pregenerate_gui_results.py to generate results"
            )
            return
        
        try:
            self.pattern_info_label.config(text="Loading pre-generated results...")
            self.root.update()
            
            patterns = self.pregenerated_patterns['patterns']
            train_count = self.pregenerated_patterns['train_count']
            test_count = self.pregenerated_patterns['test_count']
            
            # Display results
            for rank, pattern in enumerate(patterns, 1):
                # Get product titles, truncate if too long
                title1 = pattern.get('titles', ['Unknown', 'Unknown'])[0][:40]
                title2 = pattern.get('titles', ['Unknown', 'Unknown'])[1][:40]
                total_support = pattern['train_support'] + pattern['test_support']
                
                self.pattern_tree.insert('', tk.END, values=(
                    rank,
                    title1,
                    title2,
                    total_support,
                    f"{pattern['confidence']:.2f}"
                ))
            
            # Update visualization
            self.visualize_patterns(patterns[:10])
            
            # Update info
            most_significant = patterns[0] if patterns else None
            if most_significant:
                sig_text = f"Most significant pattern: {most_significant['products'][0]} + {most_significant['products'][1]} " \
                          f"(Train: {most_significant['train_support']}, Test: {most_significant['test_support']})"
            else:
                sig_text = "No patterns found"
            
            self.pattern_info_label.config(
                text=f"Loaded {len(patterns)} pre-generated patterns | Train: {train_count:,} reviews, Test: {test_count:,} reviews | {sig_text}"
            )
            
        except Exception as e:
            messagebox.showerror("Load Error", f"Error loading pattern results:\n{str(e)}")
            self.pattern_info_label.config(text="Error loading pre-generated results")
    
    def mine_patterns_realtime(self):
        """Mine patterns in real-time using current parameters"""
        if not self.connected:
            messagebox.showerror("Error", "Not connected to database.\n\nReal-time mining requires an active Neo4j connection.")
            return
        
        # Clear previous results
        for item in self.pattern_tree.get_children():
            self.pattern_tree.delete(item)
        
        try:
            self.pattern_info_label.config(text="Mining patterns in real-time... This may take 30-60 seconds...")
            self.root.update()
            
            start_time = time.time()
            
            # Get parameters from UI
            min_support = int(self.min_support_var.get())
            max_customers = int(self.max_customers_var.get())
            
            # Mine patterns
            train_patterns = self.mine_patterns('train', min_support, max_customers)
            
            # Validate in test set
            validated_patterns = []
            with self.driver.session() as session:
                for pattern_info in train_patterns[:20]:
                    products = pattern_info['products']
                    train_support = pattern_info['support']
                    
                    result = session.run("""
                        MATCH (c:Customer)-[r1:REVIEWED]->(p1:Product {asin: $asin1})
                        MATCH (c)-[r2:REVIEWED]->(p2:Product {asin: $asin2})
                        WHERE r1.dataset = 'test' AND r2.dataset = 'test'
                        RETURN count(DISTINCT c) as support
                    """, asin1=products[0], asin2=products[1])
                    
                    test_support = result.single()['support']
                    confidence = test_support / train_support if train_support > 0 else 0
                    
                    validated_patterns.append({
                        'products': products,
                        'train_support': train_support,
                        'test_support': test_support,
                        'confidence': confidence
                    })
            
            elapsed = time.time() - start_time
            validated_patterns.sort(key=lambda x: x['test_support'], reverse=True)
            
            # Display results
            for rank, pattern in enumerate(validated_patterns, 1):
                # Get product titles from database
                title1 = "Product 1"
                title2 = "Product 2"
                try:
                    with self.driver.session() as session:
                        result = session.run("""
                            MATCH (p1:Product {asin: $asin1})
                            MATCH (p2:Product {asin: $asin2})
                            RETURN p1.title as title1, p2.title as title2
                        """, asin1=pattern['products'][0], asin2=pattern['products'][1])
                        record = result.single()
                        if record:
                            title1 = (record['title1'] or 'Unknown')[:40]
                            title2 = (record['title2'] or 'Unknown')[:40]
                except:
                    pass
                
                total_support = pattern['train_support'] + pattern['test_support']
                
                self.pattern_tree.insert('', tk.END, values=(
                    rank,
                    title1,
                    title2,
                    total_support,
                    f"{pattern['confidence']:.2f}"
                ))
            
            self.visualize_patterns(validated_patterns[:10])
            
            most_significant = validated_patterns[0] if validated_patterns else None
            if most_significant:
                sig_text = f"Most significant: {most_significant['products'][0]} + {most_significant['products'][1]}"
            else:
                sig_text = "No patterns found"
            
            self.pattern_info_label.config(
                text=f"Real-time: Found {len(validated_patterns)} patterns in {elapsed:.2f}s (min_support={min_support}, max_customers={max_customers}) | {sig_text}"
            )
            
        except Exception as e:
            messagebox.showerror("Mining Error", f"Error during real-time mining:\n{str(e)}")
            self.pattern_info_label.config(text="Error occurred during real-time mining")
    
    def mine_patterns(self, dataset, min_support, max_customers):
        """Mine frequent co-purchasing patterns"""
        with self.driver.session() as session:
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
            from collections import defaultdict
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
                   for pair, count in frequent_pairs]
    
    def visualize_patterns(self, patterns):
        """Visualize top patterns"""
        # Clear previous canvas
        for widget in self.pattern_canvas_frame.winfo_children():
            widget.destroy()
        
        if not patterns:
            return
        
        # Create figure with two subplots
        fig = Figure(figsize=(5, 6), dpi=80)
        
        # Subplot 1: Total Support
        ax1 = fig.add_subplot(211)
        total_supports = [p['train_support'] + p['test_support'] for p in patterns]
        labels = [f"P{i+1}" for i in range(len(patterns))]
        
        bars1 = ax1.bar(range(len(patterns)), total_supports, color='steelblue', alpha=0.8)
        ax1.set_ylabel('Total Support', fontsize=9)
        ax1.set_title('Pattern Support & Confidence', fontsize=10, fontweight='bold')
        ax1.set_xticks(range(len(patterns)))
        ax1.set_xticklabels(labels, fontsize=8)
        ax1.grid(axis='y', alpha=0.3)
        
        # Subplot 2: Confidence
        ax2 = fig.add_subplot(212)
        confidences = [p['confidence'] for p in patterns]
        
        bars2 = ax2.bar(range(len(patterns)), confidences, color='coral', alpha=0.8)
        ax2.set_xlabel('Pattern Rank', fontsize=9)
        ax2.set_ylabel('Confidence (Test/Train)', fontsize=9)
        ax2.set_xticks(range(len(patterns)))
        ax2.set_xticklabels(labels, fontsize=8)
        ax2.set_ylim(0, max(confidences) * 1.2 if confidences else 1)
        ax2.grid(axis='y', alpha=0.3)
        ax2.axhline(y=0.4, color='red', linestyle='--', linewidth=1, alpha=0.5, label='Target: 0.4')
        ax2.legend(fontsize=8)
        
        fig.tight_layout()
        
        # Embed in tkinter
        canvas = FigureCanvasTkAgg(fig, master=self.pattern_canvas_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    

    

    
    def close(self):
        """Clean up resources"""
        if self.driver:
            self.driver.close()


def main():
    """Main entry point"""
    root = tk.Tk()
    app = AmazonAnalysisGUI(root)
    
    # Handle window close
    def on_closing():
        app.close()
        root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
