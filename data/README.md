### Data

This directory contains information for use in this project. 

Please make sure to document each source file here.

**Arizona Campaign Finance Documentation:**

**Data Accessibility**

Data is accessibly via seethemoney.az.gov, supported by the Arizona Secretary of State’s office. When the AZ SoS changes, not inconcievable that the web portal and its organization may also change

**Data Format**

The data can be downloaded in the form of utf-161e encoded CSVs. There are 8 different main pages, structured as follows:

Candidates: contains Name*, Candidate Committee, Office (that the candidate is running for), Party, Income*, Expense*, Cash Balance, IE (independent expenditures) for*, IE against*
Political Action Committee: contains Name*, Income*, Expense*, Cash Balance, IE for*, IE against*, BME (ballot measure expenditure) for*, BME against*
Political Parties: Name*, Income*, Expense*, Cash Balance
Organizations: Name*, IE for*, IE against*, BME for*, BME against*
Independent Expenditures (includes expenditures for campaign advertising not directly connected to the campaign or political party): Affected Candidate Committee*, IE for*, IE against*
Ballot Measure: Prop or Identifier, Ballot Name, Amount for*, Amount against*
Individual Contributors: Name, Amount*
Vendors: Vendor Name, Amount*

The information in these tables is aggregated over the course of the entire time range, such that re-downloading a table for detailed information on a certain race or time period may be necessary. Under the Candidates page, it is further possible to search by individual race, and it is possible to specify a year range for all pages. 

An asterisk indicates that the entries in that column are links to more detailed pages. Those linked under 'Name' columns lead to summary pages from which the original finance reports may be downloaded as PDFs. Others lead to portals from which detailed, transaction-by-transaction information can be downloaded in a CSV format. However, downloading this data would require manually processing over a million links individually, and thus us not feasible without a web crawler. They do no follow a common schema. 

While the portal's date ranges ostensibly run from 2002 to 2023, attemptiing to access data from 2002 or 2003 often led to retrieval errors. Sticking to the 2004-2023 range is recommended. Note also that, by default when accessing some pages, the upper limit for the year range is set to 2025, which appears to result in similar errors. 

Eight CSV files, one for each page, containing data from 2004-2023 are uploaded in the data folder. Code to access those CSVs is likewise included in az_campaign_data_access. 

**Web-scraping feasibility**

There do not appear to be any anti-web-scraping defenses on this website, and detailed portal access can be specified using the url alone. Making a web-scraper for this purpose is likely feasible and desirable. 


**Arizona Campaign Finance Requirements**

**Races included**

This dataset includes state-level races: state representative, state senate, governor, corporate commissioner, state treasurer, state mine inspector, state Supreme Court, state Court of Appeals, superintendent of public instruction, and attorney general. It does not include lower level races such as county or mayoral races. 

**Time span**

As mentioned previously, the dataset notionally runs from 2002 to 2023, but accessing certain tables, notably the Individual Contributions table, during the years 2002 and 2003 appears to lead to retrieval errors. Sticking to the provided 2004-2023 range is recommended. 

**Reporting Requirements**

Expenditures must be listed if they come from individuals, PACs, political parties, vendors, and other organizations such as nonprofits and corporations. 
Ballot measure expenditures are also listed separately. 

**Limitations**

By Arizona state law, individual contributions from within the state of $100 or less must be reported, but are not required to record the contributor's name, address, occupation and employer. Thus, many individual contributions are listed as coming from 'unknown', ',ultiple contributors', or other non-names and amount to millions of dollars in the aggregate, though they originate as small-money donations. 

(Note: negative expenditures, marked with either () or -, are listed, and appear to list the times when candidates for office make certain payments)


