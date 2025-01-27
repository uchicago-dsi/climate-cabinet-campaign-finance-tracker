## General Contribution Guidelines

### Pre-requisites

To contribute directly to the repository, you should:
1. Create a GitHub account
2. Set up an SSH key for your github account
3. Accept your invitation to the repository
4. Clone the repository

### Branching

Changes should be made in a branch off of main. To do this open a terminal and navigate the repository root.
To see what branch you are on (among other things) you can run 
```bash
git status
```
If you are not on main, you can switch by running
```bash
git switch main
```
or (on older versions of git)
```bash
git checkout main
```
This might raise an error: `error: Your local changes to the following files would be overwritten by checkout:` or `error: Your local changes to the following files would be overwritten by checkout:`
This means you have uncommitted changes to files that are also modified by `main`. Your local changes have not been committed so switching to `main` would overwrite them and potentially delete valuable work. If you don't care about your local changes, you can run:
```bash
git restore PATH_TO_FILE
git checkout PATH_TO_FILE # on older versions of git
```
where `PATH_TO_FILE` is a path to the file you wish to remove the unstaged changes of. 

Once on main, make sure you have the most up to date version by pulling
```bash
git pull
```
To create a new branch:
```bash
git branch NEW_BRANCH_NAME
```
Where  `NEW_BRANCH_NAME` should be descriptive of the feature/bug the branch will address. Then you can switch to the new branch with:
```bash
git switch NEW_BRANCH_NAME
git checkout NEW_BRANCH_NAME # on older versions of git
```
Then you can work, make changes and commit them. Once you make your first commit, you should push your work:
```bash
git push --set-upstream origin NEW_BRANCH_NAME
```


### Formatting and Linting

This repository uses `ruff` for both formatting and linting. Ruff implements many standard python linters and formatters in rust (such as flake8, black, isort, bugbear, etc.) Configuration is in `pyproject.toml` and detailed documentation can be found at LINK. The linter statically checks code against a set of rules where each rule is given a code starting with letters to denote which tool it is initially from (ex: `F` is `flake8`, `B` is `bugbear`, etc.) and a number. Ruff uses a base set of rules XXX which can be modified using the `extend-select` and `exclude` statements in `pyproject.toml`. Detailed descriptions of each rule can be found at LINK. To modify the ruleset for this repository, please add or remove any new rules in alphabetical order on their own line followed by a comma and a comment explaining the rule. 

Running a new formatter for the first time on a repository can modify many files and make tracking changes with `git blame` difficult. To prevent this, new formatting changes should be added in a single commit (with no other changes) and that commits full SHA should be added to a `.git-blame-ignore-revs` file at the root of the repository. To make all local tools respect this, run `COMMAND`


## Setting Up a Development Environment

### Using VS Code's Dev Container Extension

#### Setup
1. Open VS Code
2. Click Extensions on the (probably) left sidebar. The icon is 4 squares with the top right one pulled away slightly. 
3. Search for "Dev Containers" and install
4. Open the repository in VS Code (if not already opened)
5. There are two ways to open the repo in the devcontainer:
    a. You may see a dialogue box in the bottom right that says something like "Dev container configuration detected". Click to open the repo in the devcontainer. 
    b. Click the bottom left blue or green rectangle that either shows `><` or `>< WSL: Ubuntu` or something like that. This is the 'backend' that VS code is running on. Select the option "Reopen folder in container"
6. It will likely take a few minutes to build the first time. Subsequent builds should be faster. 
7. You may get permission denied with git if you don't have an ssh agent running. See [Step 2: Create / Manage SSH Keys](https://github.com/dsi-clinic/the-clinic/blob/main/tutorials/slurm.md#step-2-create--manage-ssh-keys)

#### Developing
By default we have a few nice features:
- Our requirements.txt are installed. You may `pip install` as you work, but if you (or someone else) rebuilds the container, those packages will not be installed unless also added to `requirements.txt`
- Our module is installed as an editable package. This means local imports should work as we expect and update as we change our code. 
- VS Code extensions are installed by default. This will allow us to use the Python Debugger, and lint and format documents with ruff automatically.

To run a pipeline using the debugger, we will want to run the relevant file in the `scripts` directory. Open the desired file and click the python debugger icon in the left sidebar (a play button with a bug). Click the play button. Select current python file. 


## Adding a new state

### Scraper

Write a scraper in the scraper package, if it makes sense. If the data is available as only a bulk download, it is not worth it to write a scraper. Document the process for finding the bulk download (TODO: where) and save the bulk download somewhere publicly accessible (like a public google drive) and link it (TODO: where). It often makes sense to email the state organization responsible for placing campaign finance information on their website.

### Standardization

To standardize state data, the finance.source.DataSource class is used. Each unique information source the state provides should be placed in a subclass of DataSource. A unique source of information is any file information is retrieved from with a consistent format. For example, Pennsylvannia provides campaign finance data in 



### Walkthrough: Pennsylvania

Here we go through an example of adding a new state to the campaign finance pipeline.

#### Locating Data

I start with a simple google search "Pennsylvania campaign finance" and find the [PA Department of State's campaign finance page](https://www.pa.gov/en/agencies/dos/programs/voting-and-elections/campaign-finance.html). It's mostly information about filing so I click around the page following links for with titles like "resources" and "data".  I finally find a [campaign finance exports](https://www.pa.gov/en/agencies/dos/resources/voting-and-elections-resources/campaign-finance-data.html) with links to download full exports for each year going back to 2000. 

#### Scraper

Each report is a zip file located at a link that only differs by year, so writing a scraper should be quite simple. At the end of the scraper file it is called as a single function with start and end year as a paramter under an `if __name__ == "__main__":` block for easy usage and testing.

#### Standardization - Understanding the data files

Inspecting the files in each year's zip, there are always 5 files. The naming convention varies but they are always some variation of "contributor", "debt", "expense", "filer", and "reciept" and are always .txt files despite being formatting as csvs. None other than 2023 have any column headers and the headers there are confusing. In order to understand the contents, I send an email to the contact on the website and search for some sort of read me or guide. 

A search of 'readme' in the website search bar returns readmes for campaign finance data from [1987](https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/1987_3/readme.txt) to [2022](https://www.pa.gov/content/dam/copapwp-pagov/en/dos/old-website-documents/readme2022.txt). They look unchanged outside of two new fields noted at the header in 2022. The fields are still not well specified.

The data is very similar from year to year so a subclass of DataSource for each of the 5 files seems like the correct way to start, with a potential need for subclasses of the filetypes to handle minor changes between years (i.e. 2023 having column headers). 

I write a conversion table for the column names in each table, taking guesses where confident and waiting to gather more information. (Ex: EADDRESS1 is very likely representing address line 1 of the donor's employer given context and values present.)

For each row in the conversion table (which represents a column in raw table retrieved from the state), I fill in the raw name of the column, the type (defaulting to 'str'), the standard name based on table.yaml, the [date format according to python's datetime module](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior) if  the column is a date or time, the standard name again in the enum column if there is a set of allowable values, and finally any notes about the column (such as a description if the state provided one). 


#### Standarization - Writing DataSource subclasses

I create a general DataSource subclass `PennsylvaniaForm` that all Pennsylvania form classes will inherit from. This is because there are some similarities with all forms in the state. I write the code for `read_table`. This requires code to generate the default raw data paths (in a nested file structure `data/raw/PA/<year>/filename`), and code to get the column names since the raw files have not column headers. Once this is done, I do some EDA to make sure the right columns have the right data. I explore potential enum columns like office sought and write a mapping of state provided codes to our standard names. 

#### Standardization - Dealing with multiple paths

In the later steps of the pipeline, we normalize the database, splitting information from provided tables to derivative tables when necessary. For example, a transaction table may contain information about a donor, a donor's address, a donor's employer, a recipient's election.

In a fully normalized database, the transaction table would link to a transactor table that contains information about the donor. An address table would link to the donor table with the donor's id as well with information about where the donor lived. A memberhsip table would link to both the employer and employee's ids in transactor tables. As you can see the donor_id column in the transaction table will end up in the transaction, transactor, membership, address, and election result table. To ensure these all end up linking to the same place, we map `donor_id` to the relevant other columns in `id_columns_to_standardize` and set them to `None` if they didn't exist in `_get_additional_columns`. 


#### Rambling about IDs / multiple path (?) fields

One place that requires extra care is IDs.


In PA, for example, several entities appear in the raw dataset that will be replaced with references (IDs) to their details in another table. Sometimes


EYEAR is the election year in the contribution form. This is a dependency of the election which will be linked to the transaction by a foreign key. Additionally, the election will be linked by the donor. 

transaction -> election  <- election_result
     |                          \/
     |------------------->  individual




For something like this, it makes most sense to dictate in the DataSource which fields have multiple paths


reported_election--year
donor--election_result--election--year