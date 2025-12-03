"""
Amazon Co-Purchase Analytics GUI Application
============================================

This file creates a graphical user interface (GUI) for analyzing Amazon product data
stored in a Neo4j graph database. The application allows users to:

1. Query products based on various filters (rating, reviews, sales rank, product group)
2. View co-purchase patterns (products frequently bought together)
3. Visualize pattern statistics through interactive charts

KEY CONCEPTS FOR BEGINNERS:
---------------------------
- Neo4j: A "graph database" that stores data as nodes (entities like Products, Customers)
  connected by relationships (like REVIEWED, SIMILAR). Unlike traditional databases with
  tables, graph databases are excellent for finding connections between data.
  
- ASIN: Amazon Standard Identification Number - a unique product identifier
  
- Co-Purchase Pattern: When customers frequently buy two products together, this reveals
  a pattern. Support = how many customers bought both; Confidence = probability of
  buying product B given that they bought product A.

- Tkinter: Python's standard library for creating desktop GUI applications

DEPENDENCIES:
- tkinter: Built-in Python GUI library
- neo4j: Official Neo4j Python driver for database connections  
- matplotlib: Plotting library for creating charts and visualizations
- json: For reading/writing pattern data files
"""

import tkinter as tk  # Main GUI framework - creates windows, buttons, text fields
from tkinter import ttk, messagebox, scrolledtext  # Extended widgets and dialog boxes
import json  # For reading the pre-generated pattern mining results
from neo4j import GraphDatabase  # Official driver to connect and query Neo4j database
import matplotlib.pyplot as plt  # Plotting library (not directly used but imported)
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg  # Embed matplotlib charts in tkinter
from matplotlib.figure import Figure  # Create figure objects for charts


class AmazonAnalyticsGUI:
    """
    Main application class that creates and manages the entire GUI.
    
    This class handles:
    - Connecting to the Neo4j database
    - Building the user interface with tabs for queries and patterns
    - Executing database queries based on user input
    - Loading and displaying pre-computed co-purchase patterns
    - Creating visualizations of the data
    
    The GUI has two main tabs:
    1. Complex Queries: Filter and search products in the database
    2. Co-Purchase Patterns: View products frequently bought together
    """
    
    def __init__(self, root):
        """
        Initialize the application.
        
        Parameters:
        -----------
        root : tk.Tk
            The main window (root window) of the tkinter application.
            All other widgets are placed inside this window.
        """
        self.root = root
        self.root.title("Amazon Co-Purchase Analytics")  # Window title
        self.root.geometry("1200x800")  # Initial window size: 1200 pixels wide, 800 tall
        
        # Neo4j connection settings
        # --------------------------
        # These credentials connect to a local Neo4j database instance.
        # "bolt://" is Neo4j's binary protocol for fast communication.
        # Port 7687 is the default Neo4j Bolt port.
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"  # Default Neo4j username
        self.neo4j_password = "Password"  # Database password (change for your setup)
        self.driver = None  # Will hold the database connection object
        
        # Establish connection to Neo4j database
        self.connect_neo4j()
        
        # Create notebook widget (tabbed interface)
        # ------------------------------------------
        # A "Notebook" in tkinter is a container that holds multiple tabs,
        # allowing users to switch between different views/panels.
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)  # Fill entire window
        
        # Create the two main tab frames
        # Frames are containers that hold other widgets
        self.query_tab = ttk.Frame(self.notebook)   # Tab for database queries
        self.pattern_tab = ttk.Frame(self.notebook)  # Tab for co-purchase patterns
        
        # Add tabs to the notebook with labels
        self.notebook.add(self.query_tab, text="Complex Queries")
        self.notebook.add(self.pattern_tab, text="Co-Purchase Patterns")
        
        # Build the user interface for each tab
        self.setup_query_tab()    # Create query input fields and results area
        self.setup_pattern_tab()  # Create pattern list and visualization area
        
        # Status bar at bottom of window
        # Shows feedback about operations (e.g., "Query completed", "Loading...")
        self.status_bar = tk.Label(root, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def connect_neo4j(self):
        """
        Establish a connection to the Neo4j graph database.
        
        This method:
        1. Creates a "driver" object that manages the connection
        2. Tests the connection by running a simple query
        3. Shows an error message if connection fails
        
        The driver is reused throughout the application lifetime.
        It handles connection pooling automatically.
        """
        try:
            # Create the database driver (connection manager)
            # auth=(username, password) provides credentials
            self.driver = GraphDatabase.driver(
                self.neo4j_uri, 
                auth=(self.neo4j_user, self.neo4j_password)
            )
            # Test the connection by running a trivial query
            # "RETURN 1" is the simplest possible Cypher query
            with self.driver.session() as session:
                session.run("RETURN 1")
        except Exception as e:
            # Show error dialog if connection fails
            messagebox.showerror("Connection Error", f"Failed to connect to Neo4j:\n{str(e)}")
    
    def setup_query_tab(self):
        """
        Build the user interface for the "Complex Queries" tab.
        
        This tab allows users to filter products based on:
        - Product Group (Book, DVD, Music, Video)
        - Rating range (min/max average rating)
        - Review count range
        - Sales rank range
        - Result limit (top k products)
        
        Layout:
        - Left panel: Input fields for filter criteria
        - Right panel: Text area showing query results
        """
        # LEFT PANEL - Query Input Controls
        # ----------------------------------
        left_frame = ttk.Frame(self.query_tab)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=10, pady=10)
        
        # Section header
        ttk.Label(left_frame, text="Query Parameters", font=('Arial', 14, 'bold')).pack(pady=5)
        
        # Product Group Dropdown
        # ----------------------
        # A Combobox is a dropdown menu that lets users select from predefined options
        ttk.Label(left_frame, text="Product Group:").pack(anchor=tk.W, pady=5)
        self.group_var = tk.StringVar()  # Variable to store selected value
        group_combo = ttk.Combobox(left_frame, textvariable=self.group_var, width=30)
        group_combo['values'] = ('All', 'Book', 'DVD', 'Music', 'Video')  # Available options
        group_combo.current(0)  # Default selection: 'All'
        group_combo.pack(pady=5)
        
        # Rating Filter Inputs
        # --------------------
        # Min Rating: Products must have at least this average rating
        ttk.Label(left_frame, text="Min Rating:").pack(anchor=tk.W, pady=5)
        self.min_rating_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.min_rating_var, width=32).pack(pady=5)
        
        # Max Rating: Products must have at most this average rating
        ttk.Label(left_frame, text="Max Rating:").pack(anchor=tk.W, pady=5)
        self.max_rating_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.max_rating_var, width=32).pack(pady=5)
        
        # Review Count Filter Inputs
        # --------------------------
        # Filter by how many customer reviews a product has received
        ttk.Label(left_frame, text="Min Reviews:").pack(anchor=tk.W, pady=5)
        self.min_reviews_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.min_reviews_var, width=32).pack(pady=5)
        
        ttk.Label(left_frame, text="Max Reviews:").pack(anchor=tk.W, pady=5)
        self.max_reviews_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.max_reviews_var, width=32).pack(pady=5)
        
        # Sales Rank Filter Inputs
        # ------------------------
        # Sales rank indicates how well a product sells (lower = better selling)
        ttk.Label(left_frame, text="Min Sales Rank:").pack(anchor=tk.W, pady=5)
        self.min_salesrank_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.min_salesrank_var, width=32).pack(pady=5)
        
        ttk.Label(left_frame, text="Max Sales Rank:").pack(anchor=tk.W, pady=5)
        self.max_salesrank_var = tk.StringVar()
        ttk.Entry(left_frame, textvariable=self.max_salesrank_var, width=32).pack(pady=5)
        
        # Result Limit Input
        # ------------------
        # How many results to return (top k products matching criteria)
        ttk.Label(left_frame, text="Limit (k):").pack(anchor=tk.W, pady=5)
        self.limit_var = tk.StringVar(value="20")  # Default: return top 20
        ttk.Entry(left_frame, textvariable=self.limit_var, width=32).pack(pady=5)
        
        # Action Buttons
        # --------------
        button_frame = ttk.Frame(left_frame)
        button_frame.pack(pady=20)
        
        # Execute Query button - runs the database query
        ttk.Button(button_frame, text="Execute Query", command=self.execute_query).pack(side=tk.LEFT, padx=5)
        # Clear All button - resets all input fields
        ttk.Button(button_frame, text="Clear All", command=self.clear_query_inputs).pack(side=tk.LEFT, padx=5)
        
        # RIGHT PANEL - Query Results Display
        # -----------------------------------
        right_frame = ttk.Frame(self.query_tab)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(right_frame, text="Query Results", font=('Arial', 14, 'bold')).pack(pady=5)
        
        # ScrolledText is a text area with a built-in scrollbar
        # wrap=tk.WORD means lines wrap at word boundaries
        self.results_text = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, width=70, height=40)
        self.results_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_pattern_tab(self):
        """
        Build the user interface for the "Co-Purchase Patterns" tab.
        
        This tab displays pre-computed patterns from FP-Growth algorithm analysis,
        showing products that are frequently purchased together.
        
        FP-Growth (Frequent Pattern Growth):
        ------------------------------------
        A data mining algorithm that finds items frequently occurring together.
        Example: "Customers who bought Product A also bought Product B"
        
        Key metrics:
        - Support: Number of customers who bought BOTH products
        - Confidence: Probability of buying Product B given purchase of Product A
        
        Layout:
        - Top: Control buttons (Load Patterns)
        - Left: Scrollable list of all patterns in a table format
        - Bottom-left: Overview charts showing top 20 patterns
        - Right: Detailed view of selected pattern with visualizations
        """
        # CONTROL PANEL - Top of the tab
        # -------------------------------
        control_frame = ttk.Frame(self.pattern_tab)
        control_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)
        
        ttk.Label(control_frame, text="Co-Purchase Pattern Mining Results", 
                 font=('Arial', 14, 'bold')).pack(side=tk.LEFT, padx=10)
        
        # Button to load patterns from the pre-generated JSON file
        ttk.Button(control_frame, text="Load Patterns", command=self.load_patterns).pack(side=tk.RIGHT, padx=5)
        
        # MAIN CONTENT AREA - Split into left and right panels
        # -----------------------------------------------------
        main_frame = ttk.Frame(self.pattern_tab)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # LEFT PANEL - Pattern List and Overview Charts
        # ----------------------------------------------
        left_panel = ttk.Frame(main_frame)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Label showing total pattern count (updated when patterns are loaded)
        self.pattern_count_label = ttk.Label(left_panel, text="Patterns", font=('Arial', 12, 'bold'))
        self.pattern_count_label.pack(pady=5)
        
        # Treeview Widget - A table-like display for patterns
        # ---------------------------------------------------
        # Treeview shows data in columns with sortable headers
        columns = ('Rank', 'Product 1', 'Product 2', 'Support', 'Confidence')
        self.pattern_tree = ttk.Treeview(left_panel, columns=columns, show='headings', height=15)
        
        # Configure column headers and widths
        for col in columns:
            self.pattern_tree.heading(col, text=col)  # Set header text
            # Adjust column widths based on content type
            if col == 'Rank':
                self.pattern_tree.column(col, width=50)
            elif col == 'Support':
                self.pattern_tree.column(col, width=80)
            elif col == 'Confidence':
                self.pattern_tree.column(col, width=100)
            else:  # Product columns need more space
                self.pattern_tree.column(col, width=200)
        
        self.pattern_tree.pack(fill=tk.BOTH, expand=True)
        # Bind selection event - when user clicks a row, show details
        self.pattern_tree.bind('<<TreeviewSelect>>', self.on_pattern_select)
        
        # Add vertical scrollbar to pattern list
        scrollbar = ttk.Scrollbar(left_panel, orient=tk.VERTICAL, command=self.pattern_tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.pattern_tree.configure(yscrollcommand=scrollbar.set)
        
        # OVERVIEW CHARTS SECTION - Below the pattern list
        # -------------------------------------------------
        # Shows bar charts of top 20 patterns by support and confidence
        ttk.Label(left_panel, text="Overview: Top 20 Patterns", font=('Arial', 11, 'bold')).pack(pady=5)
        overview_frame = ttk.Frame(left_panel, height=250)
        overview_frame.pack(fill=tk.X, pady=5)
        overview_frame.pack_propagate(False)  # Prevent frame from shrinking
        
        # Create matplotlib figure for overview charts
        # FigureCanvasTkAgg embeds matplotlib plots in tkinter
        self.overview_fig = Figure(figsize=(8, 2.5), dpi=80)
        self.overview_canvas = FigureCanvasTkAgg(self.overview_fig, master=overview_frame)
        self.overview_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # RIGHT PANEL - Pattern Details and Individual Visualization
        # -----------------------------------------------------------
        right_panel = ttk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(10, 0))
        
        ttk.Label(right_panel, text="Pattern Details", font=('Arial', 12, 'bold')).pack(pady=5)
        
        # Text area showing detailed information about selected pattern
        self.pattern_details = scrolledtext.ScrolledText(right_panel, wrap=tk.WORD, height=15)
        self.pattern_details.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # RATING DISTRIBUTION CHART - Shows how customers rated each product
        # -------------------------------------------------------------------
        ttk.Label(right_panel, text="Selected Pattern: Rating Distribution", font=('Arial', 11, 'bold')).pack(pady=5)
        pattern_frame = ttk.Frame(right_panel, height=300)
        pattern_frame.pack(fill=tk.X, pady=5)
        pattern_frame.pack_propagate(False)
        
        # Matplotlib figure for rating pie charts
        self.pattern_fig = Figure(figsize=(8, 3), dpi=80)
        self.pattern_canvas = FigureCanvasTkAgg(self.pattern_fig, master=pattern_frame)
        self.pattern_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Storage for loaded pattern data
        self.patterns_data = None
    
    def clear_query_inputs(self):
        """
        Reset all query input fields to their default values.
        
        Called when user clicks "Clear All" button, allowing them
        to start fresh with a new query.
        """
        self.group_var.set('All')       # Reset dropdown to "All"
        self.min_rating_var.set('')      # Clear text fields
        self.max_rating_var.set('')
        self.min_reviews_var.set('')
        self.max_reviews_var.set('')
        self.min_salesrank_var.set('')
        self.max_salesrank_var.set('')
        self.limit_var.set('20')         # Reset limit to default
    
    def execute_query(self):
        """
        Execute a database query based on user-specified filter criteria.
        
        This method:
        1. Validates the database connection
        2. Builds a Cypher query dynamically based on filled-in filters
        3. Executes the query against Neo4j
        4. Displays formatted results in the results text area
        
        CYPHER QUERY LANGUAGE:
        ----------------------
        Cypher is Neo4j's query language (similar to SQL for relational databases).
        
        Basic structure:
        - MATCH: Find patterns in the graph (like SELECT in SQL)
        - WHERE: Filter conditions
        - RETURN: Specify what data to return
        - ORDER BY: Sort results
        - LIMIT: Restrict number of results
        
        Example: MATCH (p:Product) WHERE p.rating >= 4.5 RETURN p.title LIMIT 10
        This finds Product nodes with rating >= 4.5 and returns their titles.
        """
        # Verify database connection exists
        if not self.driver:
            messagebox.showerror("Error", "Not connected to Neo4j database")
            return
        
        try:
            # BUILD THE CYPHER QUERY DYNAMICALLY
            # ----------------------------------
            # Start with base query - find all Products with a title
            query = "MATCH (p:Product) WHERE p.title IS NOT NULL "
            params = {}  # Dictionary to hold query parameters (safer than string concatenation)
            
            # Add Product Group filter if specified
            # -------------------------------------
            group = self.group_var.get()
            if group and group != 'All':
                query += "AND p.group = $group "
                params['group'] = group
            else:
                # Require group to exist even if not filtering by specific group
                query += "AND p.group IS NOT NULL "
            
            # Add Rating filters if specified
            # --------------------------------
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
            
            # Add Review Count filters if specified
            # -------------------------------------
            min_reviews = self.min_reviews_var.get()
            if min_reviews:
                query += "AND p.total_reviews >= $min_reviews "
                params['min_reviews'] = int(min_reviews)
            
            max_reviews = self.max_reviews_var.get()
            if max_reviews:
                query += "AND p.total_reviews <= $max_reviews "
                params['max_reviews'] = int(max_reviews)
            
            # Require products to have at least some reviews
            if not min_reviews and not max_reviews:
                query += "AND p.total_reviews IS NOT NULL AND p.total_reviews > 0 "
            
            # Add Sales Rank filters if specified
            # -----------------------------------
            min_salesrank = self.min_salesrank_var.get()
            if min_salesrank:
                query += "AND p.salesrank >= $min_salesrank "
                params['min_salesrank'] = int(min_salesrank)
            
            max_salesrank = self.max_salesrank_var.get()
            if max_salesrank:
                query += "AND p.salesrank <= $max_salesrank "
                params['max_salesrank'] = int(max_salesrank)
            
            # COMPLETE THE QUERY
            # ------------------
            # Specify what fields to return and how to sort
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
        """
        Load pre-computed co-purchase patterns from a JSON file.
        
        The patterns are generated by the pregenerate_patterns.py script using
        the FP-Growth algorithm on customer purchase data. This file contains
        pairs of products frequently bought together.
        
        The JSON structure contains:
        - metadata: Generation timestamp, algorithm info
        - patterns: List of pattern objects with:
          - rank: Pattern ranking by support
          - products: ASIN and titles of both products
          - support: Number of customers who bought both
          - confidence: Probability measure
          - sample_customers: Example customers who bought both
        """
        try:
            # Read the pre-generated patterns file
            with open('gui_patterns.json', 'r', encoding='utf-8') as f:
                self.patterns_data = json.load(f)
            
            # Clear existing items from the treeview table
            for item in self.pattern_tree.get_children():
                self.pattern_tree.delete(item)
            
            # Get all patterns from the loaded data
            patterns = self.patterns_data.get('patterns', [])
            total_patterns = len(patterns)
            
            # Update the label to show actual count
            self.pattern_count_label.config(text=f"All {total_patterns:,} Patterns")
            
            self.status_bar.config(text=f"Loading {total_patterns:,} patterns...")
            self.root.update()
            
            # Insert all patterns into the treeview table
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
            
            # Display overview charts showing aggregate statistics
            self.display_overview_charts()
            
        except FileNotFoundError:
            messagebox.showerror("Error", "Pattern file 'gui_patterns.json' not found")
        except Exception as e:
            messagebox.showerror("Error", f"Error loading patterns:\n{str(e)}")
    
    def on_pattern_select(self, event):
        """
        Handle user selection of a pattern in the treeview table.
        
        When a user clicks on a pattern row, this method:
        1. Retrieves the full pattern data
        2. Displays detailed information in the right panel
        3. Creates rating distribution visualizations
        
        Parameters:
        -----------
        event : tk.Event
            The selection event (automatically passed by tkinter)
        """
        selection = self.pattern_tree.selection()
        if not selection or not self.patterns_data:
            return
        
        # Get the selected row's data
        item = self.pattern_tree.item(selection[0])
        rank = int(item['values'][0])  # First column is rank
        
        # Find the full pattern data by matching rank
        pattern = None
        for p in self.patterns_data['patterns']:
            if p['rank'] == rank:
                pattern = p
                break
        
        if not pattern:
            return
        
        # DISPLAY DETAILED PATTERN INFORMATION
        # ------------------------------------
        self.pattern_details.delete(1.0, tk.END)  # Clear previous content
        
        # Header with rank
        self.pattern_details.insert(tk.END, f"Rank: {pattern['rank']}\n")
        self.pattern_details.insert(tk.END, "="*60 + "\n\n")
        
        # Product 1 details
        self.pattern_details.insert(tk.END, "Product 1:\n")
        self.pattern_details.insert(tk.END, f"  ASIN: {pattern['products']['asin1']}\n")
        self.pattern_details.insert(tk.END, f"  Title: {pattern['products']['title1']}\n\n")
        
        # Product 2 details
        self.pattern_details.insert(tk.END, "Product 2:\n")
        self.pattern_details.insert(tk.END, f"  ASIN: {pattern['products']['asin2']}\n")
        self.pattern_details.insert(tk.END, f"  Title: {pattern['products']['title2']}\n\n")
        
        # Pattern metrics
        self.pattern_details.insert(tk.END, f"Support: {pattern['support']} customers\n")
        self.pattern_details.insert(tk.END, f"Confidence: {pattern['confidence']:.4f}\n\n")
        
        # Sample customers who bought both products
        # Shows example ratings given by these customers
        samples = pattern.get('sample_customers', [])
        self.pattern_details.insert(tk.END, f"Sample Customers:\n")
        for i, customer in enumerate(samples[:5], 1):
            self.pattern_details.insert(tk.END, f"  {i}. Customer {customer['customer_id']}: ")
            self.pattern_details.insert(tk.END, f"Rating1={customer['rating1']:.1f}, Rating2={customer['rating2']:.1f}\n")
        
        # Create visualization for this pattern
        self.visualize_pattern(pattern)
    
    def display_overview_charts(self):
        """
        Create overview bar charts showing statistics for the top 20 patterns.
        
        Displays two side-by-side charts:
        1. Support chart: Number of customers for each pattern
        2. Confidence chart: Confidence percentage for each pattern
        
        These charts give users a quick visual summary of pattern quality.
        """
        if not self.patterns_data:
            return
        
        # Clear any existing charts
        self.overview_fig.clear()
        
        # Create two subplots side by side (1 row, 2 columns)
        ax1 = self.overview_fig.add_subplot(121)  # Left chart
        ax2 = self.overview_fig.add_subplot(122)  # Right chart
        
        # Extract data from top 20 patterns
        patterns = self.patterns_data.get('patterns', [])[:20]
        ranks = [p['rank'] for p in patterns]
        supports = [p['support'] for p in patterns]
        confidences = [p['confidence'] * 100 for p in patterns]
        
        # LEFT CHART: Support Bar Chart
        # -------------------------------
        # Support = number of customers who bought both products
        ax1.bar(ranks, supports, color='steelblue', alpha=0.8)
        ax1.set_xlabel('Pattern Rank')
        ax1.set_ylabel('Support (Customers)')
        ax1.set_title('Top 20 Patterns by Support')
        ax1.grid(axis='y', alpha=0.3)  # Light horizontal grid lines
        
        # RIGHT CHART: Confidence Bar Chart
        # ----------------------------------
        # Confidence = probability of buying B given purchase of A
        ax2.bar(ranks, confidences, color='green', alpha=0.8)
        ax2.set_xlabel('Pattern Rank')
        ax2.set_ylabel('Confidence (%)')
        ax2.set_title('Top 20 Patterns by Confidence')
        ax2.grid(axis='y', alpha=0.3)
        
        # Adjust layout to prevent overlapping
        self.overview_fig.tight_layout()
        self.overview_canvas.draw()  # Render the charts
    
    def visualize_pattern(self, pattern):
        """
        Create pie charts showing rating distributions for a selected pattern.
        
        For each of the two products in the pattern, this method creates a pie
        chart showing how sample customers rated that product. This helps users
        understand the quality perception of products in the pattern.
        
        Parameters:
        -----------
        pattern : dict
            The pattern data containing product info and sample customer ratings
        """
        self.pattern_fig.clear()
        
        # Get sample customer ratings for both products
        samples = pattern.get('sample_customers', [])
        
        if samples:
            # Extract ratings for each product from sample customers
            ratings1 = [s['rating1'] for s in samples]  # Product 1 ratings
            ratings2 = [s['rating2'] for s in samples]  # Product 2 ratings
            
            # Calculate average ratings
            avg_rating1 = sum(ratings1) / len(ratings1)
            avg_rating2 = sum(ratings2) / len(ratings2)
            
            # Create two pie charts side by side
            ax1 = self.pattern_fig.add_subplot(121)  # Left pie chart
            ax2 = self.pattern_fig.add_subplot(122)  # Right pie chart
            
            # PRODUCT 1 PIE CHART
            # -------------------
            # Count how many times each rating appears
            rating_counts1 = {}
            for r in ratings1:
                rating_counts1[r] = rating_counts1.get(r, 0) + 1
            
            # Create pie chart with star labels (e.g., "5★")
            colors1 = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff6666']
            ax1.pie(rating_counts1.values(), labels=[f'{r}★' for r in rating_counts1.keys()],
                   autopct='%1.1f%%', colors=colors1[:len(rating_counts1)], startangle=90)
            ax1.set_title(f'Product 1 Ratings\nAvg: {avg_rating1:.2f}★')
            
            # PRODUCT 2 PIE CHART
            # -------------------
            rating_counts2 = {}
            for r in ratings2:
                rating_counts2[r] = rating_counts2.get(r, 0) + 1
            
            colors2 = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff6666']
            ax2.pie(rating_counts2.values(), labels=[f'{r}★' for r in rating_counts2.keys()],
                   autopct='%1.1f%%', colors=colors2[:len(rating_counts2)], startangle=90)
            ax2.set_title(f'Product 2 Ratings\nAvg: {avg_rating2:.2f}★')
        else:
            # No sample data available - show placeholder message
            ax = self.pattern_fig.add_subplot(111)
            ax.text(0.5, 0.5, 'No sample data available', 
                   ha='center', va='center', transform=ax.transAxes)
        
        self.pattern_fig.tight_layout()
        self.pattern_canvas.draw()
    
    def on_closing(self):
        """
        Clean up resources when the application window is closed.
        
        This method ensures the Neo4j database connection is properly
        closed before destroying the window, preventing resource leaks.
        """
        if self.driver:
            self.driver.close()  # Close database connection
        self.root.destroy()  # Destroy the tkinter window


def main():
    """
    Application entry point.
    
    Creates the main window, initializes the application, and starts
    the tkinter event loop which keeps the GUI responsive to user input.
    """
    root = tk.Tk()  # Create the main window
    app = AmazonAnalyticsGUI(root)  # Initialize the application
    # Set up handler for window close button (X button)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()  # Start the event loop - keeps app running


# Standard Python idiom: only run main() if this file is executed directly
# (not when imported as a module)
if __name__ == "__main__":
    main()
