## General Contribution Guidelines

Thanks for your interest in contributing to the campaign finance tracker! If you would like to contribute or see an issue, please follow these steps:

1. Search through the [issues](https://github.com/uchicago-dsi/climate-cabinet-campaign-finance-tracker/issues) to see if someone else has identified this previously.
2. If your issue/feature has already been identified you can comment on it to see how you can contribute. Otherwise, create a new issue with a description of your bug or feature request. If it is a bug, please explain how someone can reproduce the unwanted behavoir in detail.
3. If you want to work on the bug or feature request, fork the repository and open a pull request. Name the Pull Request with '[WIP]' to indicate it is a work in progress until you are done and ready for review. 
4. Before getting merged into the main repository, your code must be reviewed, tested, and pass linting.

If you are interested in adding a new state to the finance source standardizer logic, please see [below](#adding-a-new-state)

### Formatting and Linting

This repository uses `ruff` for both formatting and linting. Ruff implements many standard python linters and formatters in rust (such as flake8, black, isort, bugbear, etc.) Configuration is in `pyproject.toml` and detailed documentation can be found at LINK. The linter statically checks code against a set of rules where each rule is given a code starting with letters to denote which tool it is initially from (ex: `F` is `flake8`, `B` is `bugbear`, etc.) and a number. Ruff uses a base set of rules XXX which can be modified using the `extend-select` and `exclude` statements in `pyproject.toml`. Detailed descriptions of each rule can be found at LINK. To modify the ruleset for this repository, please add or remove any new rules in alphabetical order on their own line followed by a comma and a comment explaining the rule. 


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
    - standard_name: (optional) If the column is used, [standard name](#TODO). Even if the standard name is the same as the raw name, this must be included. 
    - date_format: (optional) Format of dates in the provided data according to [datetime strftime](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior).
- column_order: list of columns in the order they appear in the data format. If not provided, will default to the order in column_properties.
- duplicate_columns:
    - map of standard column names to list of additional columns that should be copies of them
- state_code_columns:
    - list of columns to add to table with each row containing the state code. If this data is mixed with other states, we'll want to be able to know that the data pertains to this state.
- enum_mapper:
    - keys are names of standardized column names and map to mappings of raw values to standard values for a given enum.
    - if additional keys generated that are enums, their names should be listed here as well. 
- table_name: type of table represented. transaction, transactor, election, election_result, address, membership.
- path_pattern: regex describing the default location of default raw files of this type. Relative to the `data/raw/${state_code}` directory. 

### Walkthrough: Pennsylvania

Here we go through an example of adding a new state to the campaign finance pipeline.

#### Locating Data

I start with a simple google search "Pennsylvania campaign finance" and find the [PA Department of State's campaign finance page](https://www.pa.gov/en/agencies/dos/programs/voting-and-elections/campaign-finance.html). It's mostly information about filing so I click around the page following links for with titles like "resources" and "data".  I finally find a [campaign finance exports](https://www.pa.gov/en/agencies/dos/resources/voting-and-elections-resources/campaign-finance-data.html) with links to download full exports for each year going back to 2000. 

#### Scraper

Each report is a zip file located at a link that only differs by year, so writing a scraper should be quite simple. At the end of the scraper file it is called as a single function with start and end year as a paramter under an `if __name__ == "__main__":` block for easy usage and testing.

