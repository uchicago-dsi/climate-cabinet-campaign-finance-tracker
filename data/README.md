### Data

This directory contains information for use in this project. 

Please make sure to document each source file here.

About the Data - Pennsylvania

Accessibility
- The data comes from the Pennsylvania Government Website’s Full Finance Campaign Report section. To see the actual forms and reports those can be found here: https://www.dos.pa.gov/VotingElections/CandidatesCommittees/FormsReports/Pages/default.aspx

Format
- The data consists of csv files organized according to year, with each annual file having 5 files/categories: contributions, debt, expenditures, basic filer info, and other receipts. While there is a readme file, it is largely ineffectual since it merely describes some of the data types. A more useful description can be found on the https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Pages/Technical-Specifications.aspx page, which redirects to a page that better details what the data is about. Even then one must consult the filing documentations like the report cover sheet, its various Schedules.
- In 2022 there were changes to the format of the data, including concatenating the name fields (no more separate fields for first, middle and last name), the treasurer’s address fields were removed in the filer document, party codes were updated to be more intuitive, and some more minor adjustments. 

Defenses
None

*************************************************************************

Races included (i.e state legislator, mayoral, city council, etc)
- Seems to be filed by any candidates running for any level of government, notably statewide, legislative, and judicial offices (general and municipal elections)

Years that the Dataset covers
1987-2023

Who is required to report their contributions
- Candidates, political committees, and contributing lobbyists are required to disclose expenditures and contributions through the Campaign Finance Report document. However they can fill out a statement in lieu of a full report when the amount of contributions (including in-kind contributions) received, the amount of money expended, and the liabilities incurred each did not exceed $250.00 during the reporting period. This means that the cumulative contributions received, the money expended, and liabilities incurred in the entire reporting period (campaign cycle), each did not exceed $250.00  

unless the incurred contributions and expenditures do not individually exceed $250. If they don’t exceed then they can file a statement instead of a report. I am unsure where this leaves individuals, but I am still scouring for this information. 


Limits to the dataset
- Although there is a README file detailing what the dataset columns are, the column don’t match up with the format of some of the documents, so one has to cross match manually which can be tricky, especially with columns that have many NaN values. 

- The Finance Report states that a record must be kept for any contribution over $10, but  “Contributions and receipts of $50.00 or less per contributor, during the reporting period, need not be itemized on the report” … this might mean that if 1,000 people for instance donate $50 or less, there could be potentially thousands/tens of thousands of $$ not shown on the data.

Any other relevant information
Filer Document:
- Contains info about each filer. Filers range from interest groups, individuals, committees (including PACs like VisionPAC, Build PA PAC...etc), private organizations, 

- Debts that are forgiven are considered contributions, but double counting is prevented as the data is reviewed and updated months after the last filing period, allowing for data that was classified as debt to be itemized as a contribution. Corporations or unincorporated associations are prohibited from forgiving debts and thus contributing in this manner.

Transparency USA has aggregated data on the contributions of individuals and committees. This could be a helpful source to cross-check the data and potentially help alleviate the debt-contribution issue. 
