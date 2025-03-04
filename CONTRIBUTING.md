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

To standardize state data, the finance.source.DataSourceStandardizationPipeline class is used. This class provides a general algorithm that converts raw tabular data into a format that has standard and consistent names and properties. By default, the class initializes with a state code and a form code which map to yaml configuration files specifying how to standardize the data source's data. The components of the DataSourceStandardizationPipeline can be subclassed for more sophisticated modifications if necessary. This standardization allows the same code to normalize data from all state sources. 

For each state, a configuration in `src/utils/config/finance/states` should be created with the state's lowercase 2 letter abbreviation code as the name and `.yaml` as the extension (ex: `pa.yaml` for Pennsylvania). Within the configuration file, a unique key should be made for each unique form type the state offers. A unique source of information is any file information is retrieved from with a consistent format.


#### Configuration File Schema

There are two types of first level keys of the configuration files:
- Names of forms/data sources. These are used in the DataSourceStandardizationPipeline to lookup the data source's configuration.
- Abstract bases. These are used to prevent repeating keys that are shared accross multiple data sources in the same state. Another key can 'inherit' the properties of a base key by including an 'inherits' key with the name of the base. If both configurations share a key, the child one will take precedence. For example:

```yaml
base:
  foo: x
  bar: y

transactors:
  inherits: base
  foo: z
```
'transactors' will be read as having foo = z and bar = y. 

Both first level keys have the same set of subkeys:
- state_code: two letter state code
- read_csv_params: keyword arguments to be passed to pandas [read_csv](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html) method.
- include_column_order: boolean. If True, 'names' is passed as a read_csv param with the column_order. If False, column order will be inferred from header row (either provided in `read_csv_params` or 0)
- column_details: list of column properties where each may have the following keys:
    - raw_name: the name of the column as it appears in the raw data
    - type: Pandas dtype of the column
    - standard_name: (optional) If the column is used, [standard name](#TODO)
    - date_format: (optional) Format of dates in the provided data according to [datetime strftime](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior).
- column_order: list of columns in the order they appear in the data format. If not provided, will default to the order in column_properties.
- duplicate_columns:
    - map of standard column names to list of additional columns that should be copies of them
- state_code_columns:
    - list of columns to add to table with each row containing the state code. If this data is mixed with other states, we'll want to be able to know that the data pertains to this state.
- enum_mapper:
    - keys are names of standardized column names and map to mappings of raw values to standard values for a given enum.
    - if additional keys generated that are enums, their names should be listed here as well. 
- table_type: type of table represented. transaction, transactor, election, election_result, address, membership.
- path_pattern: regex describing the default location of default raw files of this type. Relative to the `data/raw/${state_code}` directory. 

### Walkthrough: Pennsylvania

Here we go through an example of adding a new state to the campaign finance pipeline.

#### Locating Data

I start with a simple google search "Pennsylvania campaign finance" and find the [PA Department of State's campaign finance page](https://www.pa.gov/en/agencies/dos/programs/voting-and-elections/campaign-finance.html). It's mostly information about filing so I click around the page following links for with titles like "resources" and "data".  I finally find a [campaign finance exports](https://www.pa.gov/en/agencies/dos/resources/voting-and-elections-resources/campaign-finance-data.html) with links to download full exports for each year going back to 2000. 

#### Scraper

Each report is a zip file located at a link that only differs by year, so writing a scraper should be quite simple. At the end of the scraper file it is called as a single function with start and end year as a paramter under an `if __name__ == "__main__":` block for easy usage and testing.

