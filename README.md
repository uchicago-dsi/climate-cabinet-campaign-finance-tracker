# Climate Cabinet Campaign Finance Tracker

## Process

1. Collect: Gather key states' political campaign finance report data which should include recipient information, donor information, and transaction information.
2. Transform: Define database schema for storing transaction and entity information and write code to transform and validate raw data to fit appropriate schema.
3. Clean: Perform record linkage and fix likely data entry errors.
4. Classify: Label all entities as fossil fuel, clean energy, or other
5. Graph: Construct a network graph of campaign finance contributions
6. Analyze: Perform analysis on network data and join with other relevant dataset


## Setup

### Data Collection and Standardization Pipeline
1. Collect the data through **<span style="color: red;">one</span>** of the steps below
    a. Collect state's finance campaign data either from web scraping (AZ, MI, PA) or direct download (MN) OR
    b. Go to the [Project's Google Drive]('https://drive.google.com/file/d/1fazviLqQWOXDVkP8NR80tO522lsIu5-H/view?usp=drive_link') to download each state's data to their local repo following this format: repo_root / "data" / "raw" / state acronym / "file"
2. Run `pip install -r requirements.txt` and `pip install -e .` if not in Docker (not recommended for development)


## Usage

The main components of the package are broken up into subpackages which can be imported and used in external code. To run pipelines directly you can use the scripts in the `scripts` directory. These scripts have been dockerized already and can be run simply using `make` commands.

- `make run-transform-pipeline`: This runs the pipeline to transform raw data from each state into the appropriate schema. 
  - Expects there to be a folder for each state in a `data/raw` folder. Follow setup instructions to get data. 
- `make run-clean-classify-graph-pipeline`: This runs the pipeline to clean, classify, and graph data that is already in the correct schema. 
  - Expects there to be an `inds_mini.csv`, `orgs_mini.csv`, and `trans_mini.csv` in a `data/transformed` directory (should be in git by default) 

For developing, please use either a Docker dev container or slurm computer cluster. See more details in `CONTRIBUTING.md`


## Repository Structure

### utils
Project python code

### notebooks
Contains short, clean notebooks to demonstrate analysis.

### data

Contains details of acquiring all raw data used in repository. If data is small (<50MB) then it is okay to save it to the repo, making sure to clearly document how to the data is obtained.

If the data is larger than 50MB than you should not add it to the repo and instead document how to get the data in the README.md file in the data directory. 

This [README.md file](/data/README.md) should be kept up to date.

### output
This folder is empty by default. The final outputs of make commands will be placed here by default.



transactor-election-year


name
type
address




## Steps

### Step 1: Collect Data
#### Implemented in `collect` 
Retrieve data from state agencies and store in flat files

### Step 2: Normalize Transaction Data
#### Implemented in `normalize`
Convert raw data into a standardized simple schema centered around a `transactions` table.
The `transactions` represents monetary transactions and each row, at minimum, specifies
a donor, recipient, date, and amount. Donor and recipient are foreign keys to a `transactors`
table which is related to `organizations` and `individuals` tables. See schema here TODO. 

The only modifications to source data here are dropping of invalid rows and changing of data types (i.e. 20240627 and June 27, 2024 will both be standardized as datetimes.)

### Step 3: Clean Transaction Data
#### Implemented in `clean`
Modify raw data where appropriate to fix mistakes with high confidence. 

### Step 4: Record Linkage
#### Implemented in `link`
Perform record linkage for individuals and organization. Further normalize the table to include `memberships` and `addresses` tables.

### Step 5: Incorporate Elections
#### Implemented in 


## Team Member

Student Name: Nicolas Posner
Student Email: nrposner@uchicago.edu

Student Name: Alan Kagiri
Student Email: alankagiri@uchicago.edu. 

Student Name: Adil Kassim
Student Email: adilk@uchicago.edu

Student Name: Nayna Pashilkar
Student Email: npashilkar@uchicago.edu

Student Name: Yangge Xu
Student Email: yanggexu@uchicago.edu

Student Name: Bhavya Pandey    
Student Email: bhavyapandey@uchicago.edu

Student Name: Kaya Lee
Student Email: klee2024@uchicago.edu

# Documentation

## Schema

### Database Schema YAML File:

Each top level key is the name of a table that exists in the schema.  
Tables may have the following keys:
- child_types (list of strings where each string is a table name): schema. These are types that inherit this table's properties. If the parent has a matching key, it is extended. If the parent has a matching key that maps to a dictionary and there are matching keys in that dictionary, the values of the child are kept. 
- parent_type (single string that is a table name): A name of another table defined within the schema that the current table inherits from. 
- required_attributes (list of strings): list of necessary attributes for a row of the table this schema block represents to be valid. (For example, without donor, recipient, and amount, a transaction is not useful). If a required attribute is a relation (it ends with id), it will be considered present if it exists as a token (TODO) without the _id suffix.
- attributes (list of strings): all attributes of the table (all columns)
- enum_columns (mapping where keys are attributes and values are lists of strings): has keys that are names of table attributes that maps to a list of valid values for that attribute
- repeating_columns (list of attributes): columns that may have repeated columns in a raw dataset
- forward_relations (mapping where keys are attributes and values are table names): has keys that are names of table attributes with '_id' suffix removed that map to TODO. 
- reverse_relations (mapping where keys are strings and values are table names): has keys that are names of table attributes that map to TODO. These columns do not have  

*Note on inheritence: A given table may have its own attributes, any attributes of any parent types (and parents of parent types, etc), or attributes of children (and children of children, etc.).