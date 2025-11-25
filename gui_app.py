"""
Amazon Co-Purchasing Analytics - GUI Application
Milestone 4: End-to-End Application with Data Visualization
Team Baddie
"""
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import json
import os
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib
matplotlib.use('TkAgg')

# Import Neo4j for live queries
try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False


class AmazonAnalyticsGUI:
    """Main GUI application for Amazon co-purchasing analytics"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Amazon Co-Purchasing Analytics Engine")
        self.root.geometry("1400x900")
        
        # Configure style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Load pregenerated results
        self.results = self.load_results()
        
        # Initialize Neo4j connection for live queries
        self.driver = None
        if NEO4J_AVAILABLE:
            try:
                self.driver = GraphDatabase.driver("bolt://localhost:7687", 
                                                   auth=("neo4j", "Password"))
            except:
                pass
        
        # Setup UI
        self.setup_ui()
        
    def load_results(self):
        """Load pregenerated results from JSON file"""
        results_file = 'gui_results.json'
        if os.path.exists(results_file):
            with open(results_file, 'r') as f:
                return json.load(f)
        return None
    
    def setup_ui(self):
        """Setup the main user interface"""
        # Title
        title_frame = ttk.Frame(self.root)
        title_frame.pack(fill=tk.X, padx=10, pady=10)
        
        title_label = ttk.Label(title_frame, 
                               text="Amazon Co-Purchasing Analytics Engine",
                               font=('Arial', 20, 'bold'))
        title_label.pack(side=tk.LEFT)
        
        if self.results:
            subtitle = ttk.Label(title_frame,
                               text=f"Data loaded: {self.results.get('generated_at', 'N/A')}",
                               font=('Arial', 10))
            subtitle.pack(side=tk.RIGHT, padx=20)
        
        # Create notebook (tabs)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Tab 1: Dashboard
        self.create_dashboard_tab()
        
        # Tab 2: Query Interface
        self.create_query_tab()
        
        # Tab 3: Pattern Mining Results
        self.create_patterns_tab()
        
        # Tab 4: Data Visualization
        self.create_visualization_tab()
        
        # Status bar
        self.status_bar = ttk.Label(self.root, text="Ready", relief=tk.SUNKEN)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def create_dashboard_tab(self):
        """Create dashboard overview tab"""
        dashboard = ttk.Frame(self.notebook)
        self.notebook.add(dashboard, text="📊 Dashboard")
        
        if not self.results:
            ttk.Label(dashboard, 
                     text="No results loaded. Please run pregenerate_results.py first.",
                     font=('Arial', 12)).pack(pady=50)
            return
        
        # Statistics cards
        stats_frame = ttk.Frame(dashboard)
        stats_frame.pack(fill=tk.X, padx=20, pady=20)
        
        stats = self.results.get('stats', {})
        
        # Create stat cards
        self.create_stat_card(stats_frame, "Total Products", 
                             f"{stats.get('total_products', 0):,}", 0, 0)
        self.create_stat_card(stats_frame, "Total Customers", 
                             f"{stats.get('total_customers', 0):,}", 0, 1)
        self.create_stat_card(stats_frame, "Total Reviews", 
                             f"{stats.get('total_reviews', 0):,}", 0, 2)
        self.create_stat_card(stats_frame, "Similar Links", 
                             f"{stats.get('total_similar', 0):,}", 0, 3)
        
        # Charts frame
        charts_frame = ttk.Frame(dashboard)
        charts_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # Group distribution chart
        left_frame = ttk.Frame(charts_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        ttk.Label(left_frame, text="Product Categories Distribution", 
                 font=('Arial', 12, 'bold')).pack()
        
        self.create_pie_chart(left_frame, 
                             stats.get('group_distribution', {}),
                             "Product Groups")
        
        # Rating distribution chart
        right_frame = ttk.Frame(charts_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5)
        
        ttk.Label(right_frame, text="Rating Distribution", 
                 font=('Arial', 12, 'bold')).pack()
        
        self.create_bar_chart(right_frame,
                            stats.get('rating_distribution', {}),
                            "Ratings")
    
    def create_stat_card(self, parent, title, value, row, col):
        """Create a statistics card"""
        card = ttk.Frame(parent, relief=tk.RAISED, borderwidth=2)
        card.grid(row=row, column=col, padx=10, pady=10, sticky='ew')
        parent.columnconfigure(col, weight=1)
        
        ttk.Label(card, text=title, font=('Arial', 10)).pack(pady=(10, 5))
        ttk.Label(card, text=value, font=('Arial', 18, 'bold')).pack(pady=(5, 10))
    
    def create_query_tab(self):
        """Create query interface tab"""
        query_tab = ttk.Frame(self.notebook)
        self.notebook.add(query_tab, text="🔍 Query Interface")
        
        # Split into input and results
        paned = ttk.PanedWindow(query_tab, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left panel: Query inputs
        input_frame = ttk.Frame(paned)
        paned.add(input_frame, weight=1)
        
        ttk.Label(input_frame, text="Complex Query Builder", 
                 font=('Arial', 14, 'bold')).pack(pady=10)
        
        # Query form
        form_frame = ttk.Frame(input_frame)
        form_frame.pack(fill=tk.X, padx=20, pady=10)
        
        # Product Group
        ttk.Label(form_frame, text="Product Group:").grid(row=0, column=0, sticky='w', pady=5)
        self.group_var = tk.StringVar()
        group_combo = ttk.Combobox(form_frame, textvariable=self.group_var, width=20)
        group_combo['values'] = ['', 'Book', 'DVD', 'Music', 'Video']
        group_combo.grid(row=0, column=1, sticky='ew', pady=5, padx=5)
        
        # Min Rating
        ttk.Label(form_frame, text="Min Rating:").grid(row=1, column=0, sticky='w', pady=5)
        self.min_rating_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.min_rating_var, width=20).grid(
            row=1, column=1, sticky='ew', pady=5, padx=5)
        
        # Max Rating
        ttk.Label(form_frame, text="Max Rating:").grid(row=2, column=0, sticky='w', pady=5)
        self.max_rating_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.max_rating_var, width=20).grid(
            row=2, column=1, sticky='ew', pady=5, padx=5)
        
        # Min Reviews
        ttk.Label(form_frame, text="Min Reviews:").grid(row=3, column=0, sticky='w', pady=5)
        self.min_reviews_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.min_reviews_var, width=20).grid(
            row=3, column=1, sticky='ew', pady=5, padx=5)
        
        # Max Sales Rank
        ttk.Label(form_frame, text="Max Sales Rank:").grid(row=4, column=0, sticky='w', pady=5)
        self.max_salesrank_var = tk.StringVar()
        ttk.Entry(form_frame, textvariable=self.max_salesrank_var, width=20).grid(
            row=4, column=1, sticky='ew', pady=5, padx=5)
        
        # K (limit)
        ttk.Label(form_frame, text="Results Limit (k):").grid(row=5, column=0, sticky='w', pady=5)
        self.k_var = tk.StringVar(value="20")
        ttk.Entry(form_frame, textvariable=self.k_var, width=20).grid(
            row=5, column=1, sticky='ew', pady=5, padx=5)
        
        form_frame.columnconfigure(1, weight=1)
        
        # Buttons
        button_frame = ttk.Frame(input_frame)
        button_frame.pack(fill=tk.X, padx=20, pady=10)
        
        ttk.Button(button_frame, text="Execute Query", 
                  command=self.execute_custom_query).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Clear", 
                  command=self.clear_query_form).pack(side=tk.LEFT, padx=5)
        
        # Pregenerated queries
        ttk.Separator(input_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=20)
        
        ttk.Label(input_frame, text="Sample Queries", 
                 font=('Arial', 12, 'bold')).pack(pady=10)
        
        if self.results and 'queries' in self.results:
            sample_frame = ttk.Frame(input_frame)
            sample_frame.pack(fill=tk.BOTH, expand=True, padx=20)
            
            for i, query in enumerate(self.results['queries']):
                btn = ttk.Button(sample_frame, text=query['name'],
                               command=lambda q=query: self.show_sample_query(q))
                btn.pack(fill=tk.X, pady=2)
        
        # Right panel: Results
        results_frame = ttk.Frame(paned)
        paned.add(results_frame, weight=2)
        
        ttk.Label(results_frame, text="Query Results", 
                 font=('Arial', 14, 'bold')).pack(pady=10)
        
        # Results table
        table_frame = ttk.Frame(results_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Scrollbars
        y_scroll = ttk.Scrollbar(table_frame)
        y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        x_scroll = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        x_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Treeview
        self.results_tree = ttk.Treeview(table_frame, 
                                        columns=('ASIN', 'Title', 'Group', 'Rating', 'Reviews', 'SalesRank'),
                                        show='headings',
                                        yscrollcommand=y_scroll.set,
                                        xscrollcommand=x_scroll.set)
        
        self.results_tree.heading('ASIN', text='ASIN')
        self.results_tree.heading('Title', text='Product Title')
        self.results_tree.heading('Group', text='Category')
        self.results_tree.heading('Rating', text='Rating')
        self.results_tree.heading('Reviews', text='Reviews')
        self.results_tree.heading('SalesRank', text='Sales Rank')
        
        self.results_tree.column('ASIN', width=100)
        self.results_tree.column('Title', width=400)
        self.results_tree.column('Group', width=80)
        self.results_tree.column('Rating', width=70)
        self.results_tree.column('Reviews', width=80)
        self.results_tree.column('SalesRank', width=100)
        
        self.results_tree.pack(fill=tk.BOTH, expand=True)
        
        y_scroll.config(command=self.results_tree.yview)
        x_scroll.config(command=self.results_tree.xview)
        
        # Query info label
        self.query_info_label = ttk.Label(results_frame, text="", font=('Arial', 10))
        self.query_info_label.pack(pady=5)
    
    def create_patterns_tab(self):
        """Create pattern mining results tab"""
        patterns_tab = ttk.Frame(self.notebook)
        self.notebook.add(patterns_tab, text="🔗 Co-Purchasing Patterns")
        
        if not self.results or 'patterns' not in self.results:
            ttk.Label(patterns_tab, 
                     text="No pattern results loaded.",
                     font=('Arial', 12)).pack(pady=50)
            return
        
        patterns = self.results['patterns']
        
        # Header with stats
        header_frame = ttk.Frame(patterns_tab)
        header_frame.pack(fill=tk.X, padx=20, pady=20)
        
        ttk.Label(header_frame, text="Frequent Co-Purchasing Patterns", 
                 font=('Arial', 16, 'bold')).pack()
        
        stats_text = f"Training Set: {patterns.get('train_count', 0):,} reviews | "
        stats_text += f"Test Set: {patterns.get('test_count', 0):,} reviews | "
        stats_text += f"Patterns Found: {len(patterns.get('patterns', []))}"
        
        ttk.Label(header_frame, text=stats_text, font=('Arial', 10)).pack(pady=5)
        
        # Pattern list
        paned = ttk.PanedWindow(patterns_tab, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left: Pattern list
        list_frame = ttk.Frame(paned)
        paned.add(list_frame, weight=1)
        
        ttk.Label(list_frame, text="Top Patterns (sorted by total support)", 
                 font=('Arial', 12, 'bold')).pack(pady=5)
        
        # Scrollable list
        list_scroll = ttk.Scrollbar(list_frame)
        list_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.pattern_listbox = tk.Listbox(list_frame, yscrollcommand=list_scroll.set,
                                         font=('Arial', 10))
        self.pattern_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        list_scroll.config(command=self.pattern_listbox.yview)
        
        # Populate patterns
        self.pattern_data = patterns.get('patterns', [])
        for i, pattern in enumerate(self.pattern_data[:30]):
            text = f"{i+1}. Support: {pattern['total_support']:,} | "
            text += f"Conf: {pattern['confidence']:.1%}"
            if 'lift' in pattern:
                text += f" | Lift: {pattern['lift']:.2f}"
            self.pattern_listbox.insert(tk.END, text)
        
        self.pattern_listbox.bind('<<ListboxSelect>>', self.on_pattern_select)
        
        # Right: Pattern details
        detail_frame = ttk.Frame(paned)
        paned.add(detail_frame, weight=2)
        
        ttk.Label(detail_frame, text="Pattern Details", 
                 font=('Arial', 12, 'bold')).pack(pady=5)
        
        self.pattern_detail_text = scrolledtext.ScrolledText(detail_frame, 
                                                             wrap=tk.WORD,
                                                             font=('Arial', 10))
        self.pattern_detail_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Select first pattern by default
        if self.pattern_data:
            self.pattern_listbox.selection_set(0)
            self.on_pattern_select(None)
    
    def create_visualization_tab(self):
        """Create data visualization tab"""
        viz_tab = ttk.Frame(self.notebook)
        self.notebook.add(viz_tab, text="📈 Visualizations")
        
        if not self.results:
            ttk.Label(viz_tab, text="No data loaded for visualization.",
                     font=('Arial', 12)).pack(pady=50)
            return
        
        # Create notebook for different visualizations
        viz_notebook = ttk.Notebook(viz_tab)
        viz_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Viz 1: Pattern Support Chart
        self.create_pattern_support_viz(viz_notebook)
        
        # Viz 2: Top Products Chart
        self.create_top_products_viz(viz_notebook)
        
        # Viz 3: Rating Analysis
        self.create_rating_analysis_viz(viz_notebook)
    
    def create_pie_chart(self, parent, data, title):
        """Create a pie chart"""
        fig = Figure(figsize=(5, 4), dpi=100)
        ax = fig.add_subplot(111)
        
        if data:
            labels = list(data.keys())
            sizes = list(data.values())
            colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff99cc']
            
            ax.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors[:len(labels)],
                  startangle=90)
            ax.set_title(title)
        else:
            ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        
        canvas = FigureCanvasTkAgg(fig, parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def create_bar_chart(self, parent, data, title):
        """Create a bar chart"""
        fig = Figure(figsize=(5, 4), dpi=100)
        ax = fig.add_subplot(111)
        
        if data:
            keys = sorted(data.keys())
            values = [data[k] for k in keys]
            
            ax.bar(keys, values, color='#66b3ff')
            ax.set_xlabel('Rating')
            ax.set_ylabel('Count')
            ax.set_title(title)
            ax.grid(axis='y', alpha=0.3)
        else:
            ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        
        canvas = FigureCanvasTkAgg(fig, parent)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def create_pattern_support_viz(self, parent):
        """Create pattern support visualization"""
        frame = ttk.Frame(parent)
        parent.add(frame, text="Pattern Support")
        
        if not self.pattern_data:
            return
        
        fig = Figure(figsize=(10, 6), dpi=100)
        ax = fig.add_subplot(111)
        
        # Top 20 patterns
        patterns = self.pattern_data[:20]
        indices = range(len(patterns))
        train_support = [p['train_support'] for p in patterns]
        test_support = [p['test_support'] for p in patterns]
        
        x = range(len(patterns))
        width = 0.35
        
        ax.bar([i - width/2 for i in x], train_support, width, label='Training', color='#66b3ff')
        ax.bar([i + width/2 for i in x], test_support, width, label='Testing', color='#ff9999')
        
        ax.set_xlabel('Pattern Rank')
        ax.set_ylabel('Support (Customer Count)')
        ax.set_title('Top 20 Co-Purchasing Patterns: Train vs Test Support')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
        
        canvas = FigureCanvasTkAgg(fig, frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def create_top_products_viz(self, parent):
        """Create top products visualization"""
        frame = ttk.Frame(parent)
        parent.add(frame, text="Top Products")
        
        if not self.results or 'queries' not in self.results:
            return
        
        # Get highest rated products query
        query_results = None
        for q in self.results['queries']:
            if 'Highest Rated' in q['name']:
                query_results = q['results']
                break
        
        if not query_results:
            query_results = self.results['queries'][0]['results']
        
        fig = Figure(figsize=(10, 6), dpi=100)
        ax = fig.add_subplot(111)
        
        # Top 15 products
        products = query_results[:15]
        titles = [p['title'][:30] + '...' if len(p['title']) > 30 else p['title'] 
                 for p in products]
        ratings = [p['avg_rating'] for p in products]
        reviews = [p['total_reviews'] for p in products]
        
        x = range(len(products))
        
        ax2 = ax.twinx()
        
        bars1 = ax.bar([i - 0.2 for i in x], ratings, 0.4, label='Rating', color='#ffcc99')
        bars2 = ax2.bar([i + 0.2 for i in x], reviews, 0.4, label='Reviews', color='#99ff99')
        
        ax.set_xlabel('Products')
        ax.set_ylabel('Average Rating', color='#ffcc99')
        ax2.set_ylabel('Number of Reviews', color='#99ff99')
        ax.set_title('Top 15 Products: Rating vs Reviews')
        ax.set_xticks(x)
        ax.set_xticklabels(titles, rotation=45, ha='right', fontsize=8)
        ax.set_ylim(0, 5.5)
        
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')
        
        fig.tight_layout()
        
        canvas = FigureCanvasTkAgg(fig, frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def create_rating_analysis_viz(self, parent):
        """Create rating analysis visualization"""
        frame = ttk.Frame(parent)
        parent.add(frame, text="Rating Analysis")
        
        if not self.pattern_data:
            return
        
        fig = Figure(figsize=(10, 6), dpi=100)
        
        # Two subplots
        ax1 = fig.add_subplot(121)
        ax2 = fig.add_subplot(122)
        
        # Pattern confidence distribution
        confidences = [p['confidence'] for p in self.pattern_data[:30]]
        ax1.hist(confidences, bins=20, color='#66b3ff', edgecolor='black')
        ax1.set_xlabel('Confidence')
        ax1.set_ylabel('Frequency')
        ax1.set_title('Pattern Confidence Distribution')
        ax1.grid(axis='y', alpha=0.3)
        
        # Product ratings in patterns
        all_ratings = []
        for p in self.pattern_data[:30]:
            if 'ratings' in p and p['ratings']:
                all_ratings.extend([r for r in p['ratings'] if r])
        
        if all_ratings:
            ax2.hist(all_ratings, bins=10, color='#99ff99', edgecolor='black')
            ax2.set_xlabel('Product Rating')
            ax2.set_ylabel('Frequency')
            ax2.set_title('Ratings of Co-Purchased Products')
            ax2.grid(axis='y', alpha=0.3)
        
        fig.tight_layout()
        
        canvas = FigureCanvasTkAgg(fig, frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def execute_custom_query(self):
        """Execute a custom query with user inputs"""
        try:
            # Build conditions
            conditions = {}
            
            if self.group_var.get():
                conditions['group'] = self.group_var.get()
            
            if self.min_rating_var.get():
                conditions['min_rating'] = float(self.min_rating_var.get())
            
            if self.max_rating_var.get():
                conditions['max_rating'] = float(self.max_rating_var.get())
            
            if self.min_reviews_var.get():
                conditions['min_reviews'] = int(self.min_reviews_var.get())
            
            if self.max_salesrank_var.get():
                conditions['max_salesrank'] = int(self.max_salesrank_var.get())
            
            k = int(self.k_var.get()) if self.k_var.get() else 20
            
            if not conditions:
                messagebox.showwarning("No Filters", 
                                      "Please specify at least one filter condition.")
                return
            
            # Execute query
            if self.driver:
                self.status_bar.config(text="Executing query...")
                self.root.update()
                
                results, exec_time = self.execute_query_live(conditions, k)
                
                self.display_query_results(results, exec_time, "Custom Query")
                self.status_bar.config(text=f"Query completed in {exec_time:.3f}s")
            else:
                messagebox.showerror("Database Unavailable", 
                                   "Neo4j connection not available. Cannot execute live query.")
        
        except ValueError as e:
            messagebox.showerror("Invalid Input", 
                               f"Please check your input values:\n{str(e)}")
        except Exception as e:
            messagebox.showerror("Query Error", f"Error executing query:\n{str(e)}")
            self.status_bar.config(text="Query failed")
    
    def execute_query_live(self, conditions, k):
        """Execute query against live database"""
        import time
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
    
    def show_sample_query(self, query):
        """Display results from a pregenerated sample query"""
        results = query['results']
        exec_time = query['execution_time']
        name = query['name']
        
        self.display_query_results(results, exec_time, name)
        self.status_bar.config(text=f"Loaded sample query: {name}")
    
    def display_query_results(self, results, exec_time, query_name):
        """Display query results in the treeview"""
        # Clear existing results
        for item in self.results_tree.get_children():
            self.results_tree.delete(item)
        
        # Insert new results
        for product in results:
            self.results_tree.insert('', tk.END, values=(
                product.get('asin', 'N/A'),
                product.get('title', 'N/A')[:80],
                product.get('group', 'N/A'),
                f"{product.get('avg_rating', 0):.2f}",
                f"{product.get('total_reviews', 0):,}",
                f"{product.get('salesrank', 0):,}" if product.get('salesrank', 0) > 0 else 'N/A'
            ))
        
        # Update info label
        info_text = f"{query_name}: {len(results)} results in {exec_time:.3f} seconds"
        self.query_info_label.config(text=info_text)
    
    def clear_query_form(self):
        """Clear all query form inputs"""
        self.group_var.set('')
        self.min_rating_var.set('')
        self.max_rating_var.set('')
        self.min_reviews_var.set('')
        self.max_salesrank_var.set('')
        self.k_var.set('20')
        self.status_bar.config(text="Form cleared")
    
    def on_pattern_select(self, event):
        """Handle pattern selection"""
        selection = self.pattern_listbox.curselection()
        if not selection:
            return
        
        index = selection[0]
        pattern = self.pattern_data[index]
        
        # Build detail text
        detail = f"PATTERN #{index + 1}\n"
        detail += "=" * 80 + "\n\n"
        
        detail += f"Product 1: {pattern['titles'][0]}\n"
        if pattern.get('ratings') and len(pattern['ratings']) > 0:
            detail += f"  Rating: {pattern['ratings'][0]:.2f}\n"
        detail += f"  ASIN: {pattern['products'][0]}\n\n"
        
        detail += f"Product 2: {pattern['titles'][1]}\n"
        if pattern.get('ratings') and len(pattern['ratings']) > 1:
            detail += f"  Rating: {pattern['ratings'][1]:.2f}\n"
        detail += f"  ASIN: {pattern['products'][1]}\n\n"
        
        detail += "=" * 80 + "\n"
        detail += "STATISTICS\n"
        detail += "=" * 80 + "\n\n"
        detail += f"Training Set Support: {pattern['train_support']:,} customers\n"
        detail += f"Test Set Support: {pattern['test_support']:,} customers\n"
        detail += f"Total Support: {pattern['total_support']:,} customers\n\n"
        
        detail += f"Confidence: {pattern['confidence']:.2%}\n"
        detail += "  (Probability: bought Product 2 given bought Product 1)\n"
        
        if 'lift' in pattern:
            detail += f"\nLift: {pattern['lift']:.3f}\n"
            if pattern['lift'] > 1:
                detail += f"  (Items purchased together {pattern['lift']:.1f}x more than random)\n"
            else:
                detail += "  (Items not strongly associated)\n"
        
        detail += "\n"
        
        # Interpretation
        if pattern['confidence'] > 0.9:
            detail += "⭐ Very Strong Pattern: High confidence indicates these products\n"
            detail += "   are frequently purchased together (possibly duplicate listings).\n\n"
        elif pattern['confidence'] > 0.7:
            detail += "✓ Strong Pattern: Customers who buy one often buy the other.\n\n"
        elif pattern['confidence'] > 0.5:
            detail += "✓ Moderate Pattern: Noticeable co-purchasing tendency.\n\n"
        else:
            detail += "→ Weak Pattern: Some co-purchasing but not dominant.\n\n"
        
        if 'sample_customers' in pattern and pattern['sample_customers']:
            detail += "=" * 80 + "\n"
            detail += "SAMPLE CUSTOMERS\n"
            detail += "=" * 80 + "\n\n"
            
            for i, customer in enumerate(pattern['sample_customers'][:10], 1):
                detail += f"{i}. Customer ID: {customer['customer_id']}\n"
                detail += f"   Rated Product 1: {customer['rating1']}/5 stars\n"
                detail += f"   Rated Product 2: {customer['rating2']}/5 stars\n\n"
        
        # Display
        self.pattern_detail_text.delete(1.0, tk.END)
        self.pattern_detail_text.insert(1.0, detail)
    
    def __del__(self):
        """Cleanup on exit"""
        if self.driver:
            self.driver.close()


def main():
    """Main entry point"""
    root = tk.Tk()
    app = AmazonAnalyticsGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
