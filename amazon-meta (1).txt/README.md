# CS415 Project - Amazon Stuff with Neo4j

hey prof! this is our project for milestone 2. we used neo4j to store amazon product data and stuff.

## What you need to get this working:

### Neo4j Setup (this was kinda annoying tbh)

- go to neo4j.com and download the desktop thing
- make a new database (we called ours "amazon_db" but whatever works)
- username: neo4j, password: Password (yeah i know its not secure but its just for class)
- make sure its running on port 7687 (should be default)
- if it doesn't work try the community edition instead

### Python stuff

- need python 3.8+ (we used 3.10 on one machine and 3.11 on another, both worked fine)
- pip should work but if not try pip3

## How to run this thing:

### Step 1: Install the packages

```bash
pip install -r requirements.txt
```

_note: if you get errors just try installing them one by one, sometimes pip is weird_

### Step 2: Make sure neo4j is actually running

- check that bolt://localhost:7687 works
- you can test it in the neo4j browser thingy

### Step 3: Run everything

```bash
python run_complete_pipeline.py
```

this script does a bunch of stuff:

- reads the massive amazon file (took forever on my laptop lol)
- makes it smaller so it doesn't crash everything (50k products instead of 500k+)
- cleans up the messy data
- puts it all in neo4j with proper relationships and indexes
- runs some test queries to make sure it worked
- prints out performance stats that hopefully look good for grading

### If something breaks:

````bash
# Data preparation only
python data_preparation.py

# Neo4j ingestion only (requires processed data)
you can also run individual parts but honestly just use the main script unless something's broken:
```bash
# just prep the data
python data_preparation.py

# just do the neo4j stuff
python neo4j_ingestion.py
````

## What files are what:

```
our-project/
├── amazon-meta.txt                 # the huge file prof gave us (977MB oof)
├── data_preparation.py            # makes the data smaller and cleaner
├── neo4j_ingestion.py             # puts everything into neo4j
├── run_complete_pipeline.py       # runs everything (use this one!)
├── requirements.txt               # python packages we need
├── processed_amazon_data.json     # the cleaned up data (gets created)
├── validation_queries.json        # test queries (also gets created)
└── README.md                      # you're reading this lol
```

## What should happen when it works:

- you get a ~150MB json file with 50k products (way more manageable)
- neo4j gets filled up with products, categories, and customer data
- we made relationships for similar products, categories, and reviews
- prints out some timing stats that look impressive

## If stuff breaks (it probably will):

### Neo4j won't connect

- check if neo4j desktop is actually running (we forgot this like 3 times)
- make sure username is "neo4j" and password is "Password"
- sometimes port 7687 gets taken by other stuff, restart neo4j

### Python explodes with memory errors

- go into data_preparation.py and make max_products smaller (like 25000 instead of 50000)
- close chrome and other memory hogs lol
- if your laptop sucks try running it overnight

### Everything is super slow

- change BATCH_SIZE in neo4j_ingestion.py to something smaller (maybe 500 instead of 1000)
- make sure nothing else is using your cpu
- pray to the demo gods

## The database design stuff:

We made 3 types of nodes:

- **Products**: all the amazon stuff with titles, ratings, etc
- **Categories**: like "Books > Fiction > Mystery" but in graph form
- **Customers**: people who wrote reviews

And 3 types of connections:

- **SIMILAR_TO**: amazon's "customers also bought" recommendations
- **BELONGS_TO**: which category products are in
- **REVIEWED**: who reviewed what with what rating

this lets us do cool queries like "find books similar to harry potter that got 4+ stars" and other stuff that should impress prof hopefully
