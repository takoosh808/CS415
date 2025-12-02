import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import json
from neo4j import GraphDatabase
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

class AmazonAnalyticsGUI:
    
    def __init__(self, root):
        self.root = root
        self.root.title("Amazon Co-Purchase Analytics")
        self.root.geometry("1200x800")
        
        # Neo4j connection
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "Password"
        self.driver = None
        
        # Connect to Neo4j
        self.connect_neo4j()
        
        # Create notebook (tabs)
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Create tabs
        self.query_tab = ttk.Frame(self.notebook)
        self.pattern_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.query_tab, text="Complex Queries")
        self.notebook.add(self.pattern_tab, text="Co-Purchase Patterns")
        
        # Setup tabs
        self.setup_query_tab()
        self.setup_pattern_tab()
        
        # Status bar
        self.status_bar = tk.Label(root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def connect_neo4j(self):
        try:
            self.driver = GraphDatabase.driver(
                self.neo4j_uri, 
                auth=(self.neo4j_user, self.neo4j_password)
            )
            with self.driver.session() as session:
                session.run("RETURN 1")
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to Neo4j:\n{str(e)}")
    
    def setup_query_tab(self):
        # Left panel - Query inputs
        left_frame = ttk.Frame(self.query_tab)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=10, pady=10)
        
        ttk.Label(left_frame, text="Query Parameters", font=('Arial', 14, 'bold')).pack(pady=5)
        
        # Product Group
        ttk.Label(left_frame, text="Product Group:").pack(anchor=tk.W, pady=5)
        self.group_var = tk.StringVar()
        group_combo = ttk.Combobox(left_frame, textvariable=self.group_var, width=30)
        group_combo['values'] = ('All', 'Book', 'DVD', 'Music', 'Video')
        group_combo.current(0)
        group_combo.pack(pady=5)
        
        # Min Rating
        ttk.Label(left_frame, text="Min Rating:").pack(anchor=tk.W, pady=5)
        self.min_rating_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.min_rating_var, width=32).pack(pady=5)
        
        # Max Rating
        ttk.Label(left_frame, text="Max Rating:").pack(anchor=tk.W, pady=5)
        self.max_rating_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.max_rating_var, width=32).pack(pady=5)
        
        # Min Reviews
        ttk.Label(left_frame, text="Min Reviews:").pack(anchor=tk.W, pady=5)
        self.min_reviews_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.min_reviews_var, width=32).pack(pady=5)
        
        # Max Reviews
        ttk.Label(left_frame, text="Max Reviews:").pack(anchor=tk.W, pady=5)
        self.max_reviews_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.max_reviews_var, width=32).pack(pady=5)
        
        # Min Sales Rank
        ttk.Label(left_frame, text="Min Sales Rank:").pack(anchor=tk.W, pady=5)
        self.min_salesrank_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.min_salesrank_var, width=32).pack(pady=5)
        
        # Max Sales Rank
        ttk.Label(left_frame, text="Max Sales Rank:").pack(anchor=tk.W, pady=5)
        self.max_salesrank_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.max_salesrank_var, width=32).pack(pady=5)
        
        # Limit
        ttk.Label(left_frame, text="Limit (k):").pack(anchor=tk.W, pady=5)
        self.limit_var = tk.StringVar(value="20")
        ttk.Entry(left_frame, textvariable=self.limit_var, width=32).pack(pady=5)
        
        # Buttons
        button_frame = ttk.Frame(left_frame)
        button_frame.pack(pady=20)
        
        ttk.Button(button_frame, text="Execute Query", command=self.execute_query).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Clear All", command=self.clear_query_inputs).pack(side=tk.LEFT, padx=5)
        
        # Right panel - Results
        right_frame = ttk.Frame(self.query_tab)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(right_frame, text="Query Results", font=('Arial', 14, 'bold')).pack(pady=5)
        
        # Results text area
        self.results_text = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, width=70, height=40)
        self.results_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_pattern_tab(self):
        # Control panel
        control_frame = ttk.Frame(self.pattern_tab)
        control_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)
        
        ttk.Label(control_frame, text="Co-Purchase Pattern Mining Results", 
                 font=('Arial', 14, 'bold')).pack(side=tk.LEFT, padx=10)
        
        ttk.Button(control_frame, text="Load Patterns", command=self.load_patterns).pack(side=tk.RIGHT, padx=5)
        
        # Create split view - patterns list and details
        main_frame = ttk.Frame(self.pattern_tab)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Left panel - Pattern list
        left_panel = ttk.Frame(main_frame)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Label that will be updated with pattern count
        self.pattern_count_label = ttk.Label(left_panel, text="Patterns", font=('Arial', 12, 'bold'))
        self.pattern_count_label.pack(pady=5)
        
        # Treeview for patterns
        columns = ('Rank', 'Product 1', 'Product 2', 'Support', 'Confidence')
        self.pattern_tree = ttk.Treeview(left_panel, columns=columns, show='headings', height=15)
        
        for col in columns:
            self.pattern_tree.heading(col, text=col)
            if col == 'Rank':
                self.pattern_tree.column(col, width=50)
            elif col == 'Support':
                self.pattern_tree.column(col, width=80)
            elif col == 'Confidence':
                self.pattern_tree.column(col, width=100)
            else:
                self.pattern_tree.column(col, width=200)
        
        self.pattern_tree.pack(fill=tk.BOTH, expand=True)
        self.pattern_tree.bind('<<TreeviewSelect>>', self.on_pattern_select)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(left_panel, orient=tk.VERTICAL, command=self.pattern_tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.pattern_tree.configure(yscrollcommand=scrollbar.set)
        
        # Overview Charts Section (under the tree)
        ttk.Label(left_panel, text="Overview: Top 20 Patterns", font=('Arial', 11, 'bold')).pack(pady=5)
        overview_frame = ttk.Frame(left_panel, height=250)
        overview_frame.pack(fill=tk.X, pady=5)
        overview_frame.pack_propagate(False)
        
        self.overview_fig = Figure(figsize=(8, 2.5), dpi=80)
        self.overview_canvas = FigureCanvasTkAgg(self.overview_fig, master=overview_frame)
        self.overview_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Right panel - Pattern details and visualization
        right_panel = ttk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(10, 0))
        
        ttk.Label(right_panel, text="Pattern Details", font=('Arial', 12, 'bold')).pack(pady=5)
        
        # Details text area
        self.pattern_details = scrolledtext.ScrolledText(right_panel, wrap=tk.WORD, height=15)
        self.pattern_details.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Section: Selected Pattern Rating Distribution
        ttk.Label(right_panel, text="Selected Pattern: Rating Distribution", font=('Arial', 11, 'bold')).pack(pady=5)
        pattern_frame = ttk.Frame(right_panel, height=300)
        pattern_frame.pack(fill=tk.X, pady=5)
        pattern_frame.pack_propagate(False)
        
        self.pattern_fig = Figure(figsize=(8, 3), dpi=80)
        self.pattern_canvas = FigureCanvasTkAgg(self.pattern_fig, master=pattern_frame)
        self.pattern_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Store patterns data
        self.patterns_data = None
    
    def clear_query_inputs(self):
        self.group_var.set('All')
        self.min_rating_var.set('')
        self.max_rating_var.set('')
        self.min_reviews_var.set('')
        self.max_reviews_var.set('')
        self.min_salesrank_var.set('')
        self.max_salesrank_var.set('')
        self.limit_var.set('20')
    
    def execute_query(self):
        if not self.driver:
            messagebox.showerror("Error", "Not connected to Neo4j database")
            return
        
        try:
            # Build query - filter for products with meaningful data
            query = "MATCH (p:Product) WHERE p.title IS NOT NULL "
            params = {}
            
            # Group filter
            group = self.group_var.get()
            if group and group != 'All':
                query += "AND p.group = $group "
                params['group'] = group
            else:
                # If no specific group, at least require group to be set
                query += "AND p.group IS NOT NULL "
            
            # Rating filters
            min_rating = self.min_rating_var.get()
            if min_rating:
                query += "AND p.avg_rating >= $min_rating "
                params['min_rating'] = float(min_rating)
            
            max_rating = self.max_rating_var.get()
            if max_rating:
                query += "AND p.avg_rating <= $max_rating "
                params['max_rating'] = float(max_rating)
            
            # If no rating filter specified, require rating to exist
            if not min_rating and not max_rating:
                query += "AND p.avg_rating IS NOT NULL "
            
            # Review count filters
            min_reviews = self.min_reviews_var.get()
            if min_reviews:
                query += "AND p.total_reviews >= $min_reviews "
                params['min_reviews'] = int(min_reviews)
            
            max_reviews = self.max_reviews_var.get()
            if max_reviews:
                query += "AND p.total_reviews <= $max_reviews "
                params['max_reviews'] = int(max_reviews)
            
            # If no review filter specified, require reviews to exist
            if not min_reviews and not max_reviews:
                query += "AND p.total_reviews IS NOT NULL AND p.total_reviews > 0 "
            
            # Sales rank filters
            min_salesrank = self.min_salesrank_var.get()
            if min_salesrank:
                query += "AND p.salesrank >= $min_salesrank "
                params['min_salesrank'] = int(min_salesrank)
            
            max_salesrank = self.max_salesrank_var.get()
            if max_salesrank:
                query += "AND p.salesrank <= $max_salesrank "
                params['max_salesrank'] = int(max_salesrank)
            
            # Return with limit
            limit = int(self.limit_var.get()) if self.limit_var.get() else 20
            query += "RETURN p.asin as asin, p.title as title, p.group as group, "
            query += "p.avg_rating as rating, p.total_reviews as reviews, p.salesrank as salesrank "
            query += "ORDER BY p.avg_rating DESC, p.total_reviews DESC "
            query += f"LIMIT {limit}"
            
            self.status_bar.config(text="Executing query...")
            self.root.update()
            
            # Execute query
            with self.driver.session() as session:
                result = session.run(query, params)
                records = list(result)
            
            # Display results
            self.results_text.delete(1.0, tk.END)
            
            if not records:
                self.results_text.insert(tk.END, "No results found matching the criteria.\n")
            else:
                self.results_text.insert(tk.END, f"Found {len(records)} products:\n")
                self.results_text.insert(tk.END, "="*80 + "\n\n")
                
                for i, record in enumerate(records, 1):
                    asin = record['asin']
                    title = record['title'] or 'Unknown'
                    group = record['group'] or 'Unknown'
                    rating = record['rating'] if record['rating'] else 'N/A'
                    reviews = record['reviews'] if record['reviews'] else 0
                    salesrank = record['salesrank'] if record['salesrank'] else 'N/A'
                    
                    self.results_text.insert(tk.END, f"{i}. {title[:60]}\n")
                    self.results_text.insert(tk.END, f"   ASIN: {asin}\n")
                    self.results_text.insert(tk.END, f"   Group: {group}\n")
                    self.results_text.insert(tk.END, f"   Rating: {rating} | Reviews: {reviews} | Sales Rank: {salesrank}\n")
                    self.results_text.insert(tk.END, "\n")
            
            self.status_bar.config(text=f"Query completed. Found {len(records)} results.")
            
        except Exception as e:
            messagebox.showerror("Query Error", f"Error executing query:\n{str(e)}")
            self.status_bar.config(text="Query failed")
    
    def load_patterns(self):
        try:
            with open('gui_patterns.json', 'r', encoding='utf-8') as f:
                self.patterns_data = json.load(f)
            
            # Clear existing items
            for item in self.pattern_tree.get_children():
                self.pattern_tree.delete(item)
            
            # Populate tree with ALL patterns
            patterns = self.patterns_data.get('patterns', [])
            total_patterns = len(patterns)
            
            # Update the label to show actual count
            self.pattern_count_label.config(text=f"All {total_patterns:,} Patterns")
            
            self.status_bar.config(text=f"Loading {total_patterns:,} patterns...")
            self.root.update()
            
            # Insert all patterns into the tree
            for i, pattern in enumerate(patterns, 1):
                rank = pattern['rank']
                title1 = pattern['products']['title1'][:40]
                title2 = pattern['products']['title2'][:40]
                support = pattern['support']
                confidence = f"{pattern['confidence']:.4f}"
                
                self.pattern_tree.insert('', tk.END, values=(rank, title1, title2, support, confidence))
                
                # Update progress for large datasets
                if i % 100 == 0:
                    self.status_bar.config(text=f"Loading patterns... {i:,}/{total_patterns:,}")
                    self.root.update()
            
            self.status_bar.config(text=f"Loaded {total_patterns:,} patterns")
            messagebox.showinfo("Success", f"Loaded {total_patterns:,} co-purchase patterns")
            
            # Display overview charts
            self.display_overview_charts()
            
        except FileNotFoundError:
            messagebox.showerror("Error", "Pattern file 'gui_patterns.json' not found")
        except Exception as e:
            messagebox.showerror("Error", f"Error loading patterns:\n{str(e)}")
    
    def on_pattern_select(self, event):
        selection = self.pattern_tree.selection()
        if not selection or not self.patterns_data:
            return
        
        # Get selected item
        item = self.pattern_tree.item(selection[0])
        rank = int(item['values'][0])
        
        # Find pattern data
        pattern = None
        for p in self.patterns_data['patterns']:
            if p['rank'] == rank:
                pattern = p
                break
        
        if not pattern:
            return
        
        # Display pattern details
        self.pattern_details.delete(1.0, tk.END)
        
        self.pattern_details.insert(tk.END, f"Rank: {pattern['rank']}\n")
        self.pattern_details.insert(tk.END, "="*60 + "\n\n")
        
        self.pattern_details.insert(tk.END, "Product 1:\n")
        self.pattern_details.insert(tk.END, f"  ASIN: {pattern['products']['asin1']}\n")
        self.pattern_details.insert(tk.END, f"  Title: {pattern['products']['title1']}\n\n")
        
        self.pattern_details.insert(tk.END, "Product 2:\n")
        self.pattern_details.insert(tk.END, f"  ASIN: {pattern['products']['asin2']}\n")
        self.pattern_details.insert(tk.END, f"  Title: {pattern['products']['title2']}\n\n")
        
        self.pattern_details.insert(tk.END, f"Support: {pattern['support']} customers\n")
        self.pattern_details.insert(tk.END, f"Confidence: {pattern['confidence']:.4f}\n\n")
        
        # Sample customers
        samples = pattern.get('sample_customers', [])
        self.pattern_details.insert(tk.END, f"Sample Customers:\n")
        for i, customer in enumerate(samples[:5], 1):
            self.pattern_details.insert(tk.END, f"  {i}. Customer {customer['customer_id']}: ")
            self.pattern_details.insert(tk.END, f"Rating1={customer['rating1']:.1f}, Rating2={customer['rating2']:.1f}\n")
        
        # Visualize pattern
        self.visualize_pattern(pattern)
    
    def display_overview_charts(self):
        if not self.patterns_data:
            return
        
        self.overview_fig.clear()
        
        # Create two subplots
        ax1 = self.overview_fig.add_subplot(121)
        ax2 = self.overview_fig.add_subplot(122)
        
        # Get top 20 patterns
        patterns = self.patterns_data.get('patterns', [])[:20]
        ranks = [p['rank'] for p in patterns]
        supports = [p['support'] for p in patterns]
        confidences = [p['confidence'] * 100 for p in patterns]
        
        # Left chart: Support
        ax1.bar(ranks, supports, color='steelblue', alpha=0.8)
        ax1.set_xlabel('Pattern Rank')
        ax1.set_ylabel('Support (Customers)')
        ax1.set_title('Top 20 Patterns by Support')
        ax1.grid(axis='y', alpha=0.3)
        
        # Right chart: Confidence
        ax2.bar(ranks, confidences, color='green', alpha=0.8)
        ax2.set_xlabel('Pattern Rank')
        ax2.set_ylabel('Confidence (%)')
        ax2.set_title('Top 20 Patterns by Confidence')
        ax2.grid(axis='y', alpha=0.3)
        
        self.overview_fig.tight_layout()
        self.overview_canvas.draw()
    
    def visualize_pattern(self, pattern):
        self.pattern_fig.clear()
        
        # Get average ratings for the two products from sample customers
        samples = pattern.get('sample_customers', [])
        
        if samples:
            # Calculate average ratings
            ratings1 = [s['rating1'] for s in samples]
            ratings2 = [s['rating2'] for s in samples]
            avg_rating1 = sum(ratings1) / len(ratings1)
            avg_rating2 = sum(ratings2) / len(ratings2)
            
            # Create two pie charts
            ax1 = self.pattern_fig.add_subplot(121)
            ax2 = self.pattern_fig.add_subplot(122)
            
            # Product 1 pie chart - rating distribution
            rating_counts1 = {}
            for r in ratings1:
                rating_counts1[r] = rating_counts1.get(r, 0) + 1
            
            colors1 = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff6666']
            ax1.pie(rating_counts1.values(), labels=[f'{r}★' for r in rating_counts1.keys()],
                   autopct='%1.1f%%', colors=colors1[:len(rating_counts1)], startangle=90)
            ax1.set_title(f'Product 1 Ratings\nAvg: {avg_rating1:.2f}★')
            
            # Product 2 pie chart - rating distribution
            rating_counts2 = {}
            for r in ratings2:
                rating_counts2[r] = rating_counts2.get(r, 0) + 1
            
            colors2 = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff6666']
            ax2.pie(rating_counts2.values(), labels=[f'{r}★' for r in rating_counts2.keys()],
                   autopct='%1.1f%%', colors=colors2[:len(rating_counts2)], startangle=90)
            ax2.set_title(f'Product 2 Ratings\nAvg: {avg_rating2:.2f}★')
        else:
            ax = self.pattern_fig.add_subplot(111)
            ax.text(0.5, 0.5, 'No sample data available', 
                   ha='center', va='center', transform=ax.transAxes)
        
        self.pattern_fig.tight_layout()
        self.pattern_canvas.draw()
    
    def on_closing(self):
        if self.driver:
            self.driver.close()
        self.root.destroy()


def main():
    root = tk.Tk()
    app = AmazonAnalyticsGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
