# Climate Cabinet Campaign Finance Tracker

## Process

1. Collect: Gather key states' political campaign finance report data which should include recipient information, donor information, and transaction information.
2. Standardize: Define database schema for storing transaction and entity information and standardize column names and values.
3. Normalize: Normalize data into provided schema
4. Classify: Label all entities as fossil fuel, clean energy, or other
5. Graph: Construct a network graph of campaign finance contributions
6. Analyze: Perform analysis on network data and join with other relevant dataset


## Local Development

### Data Collection and Standardization Pipeline
1. Collect the data through **<span style="color: red;">one</span>** of the steps below
    a. Collect state's finance campaign data either from web scraping (AZ, MI, PA) or direct download (MN) OR
    b. Go to the [Project's Google Drive]('https://drive.google.com/file/d/1fazviLqQWOXDVkP8NR80tO522lsIu5-H/view?usp=drive_link') to download each state's data to their local repo following this format: repo_root / "data" / "raw" / state acronym / "file"
2. Run `pip install -r requirements.txt` and `pip install -e .` if not in Docker (not recommended for development)

### Docker Development

The repository provides a Dockerfile and devcontainer configuration. It is recommended to develop in Docker. 


## Usage

The main components of the package are broken up into subpackages which can be imported and used in external code. To run pipelines directly you can use the scripts in the `scripts` directory. These scripts have been dockerized already and can be run simply using `make` commands.

- `make run-standardize-pipeline`: This runs the pipeline to read in raw data and standardize column names and data.
  - Expects there to be a folder for each state in a `data/raw` folder. Follow setup instructions to get data.  Outputs to `output/standardized`
- `make run-normalize-pipeline`: This runs the pipeline to normalize data. 
  - Expects data in `output/standardized`. Outputes to `output/normalized`
- `make run-standardize-normalize-pipeline`. Combines both pipelines.

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


## Past Student Team Members

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
- reverse_relation_names (mapping where keys are strings in reverse_relations and values are strings in the forward_relations of the table this column refers to): every entry in reverse_relations must have an entry here. This is to disambiguate which columns refer to which reverse relations

*Note on inheritence: A given table may have its own attributes, any attributes of any parent types (and parents of parent types, etc), or attributes of children (and children of children, etc.).