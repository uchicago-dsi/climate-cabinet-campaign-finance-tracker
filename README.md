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
