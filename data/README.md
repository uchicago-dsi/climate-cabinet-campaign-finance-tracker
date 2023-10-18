### Data

This directory contains information for use in this project. 

Please make sure to document each source file here.

#### Pennsylvania Campaign Finance Data
##### Summary
The Pennsylvania Campaign Finance Data comes from the Pennsylvania Government Website’s Full Finance Campaign Report section. To see the actual forms and reports those can be found here: https://www.dos.pa.gov/VotingElections/CandidatesCommittees/FormsReports/Pages/default.aspx. No defenses or anti-captcha mechanisms exist to monitor access to the data. The data is stored in the form of csvs, but is named with a .txt and .zip tag depending on the year. This is because while the data spans from 1987-2022, there are incongruences in the formatting. The pre-2000 data have their sub-categories listed as separate links in the form of .txt, while the post-2000 years have each year as a .zip nested file that contains the sub-categories. Additionally, 2022 has 2 additional fields (Timestamp & Reported ID) in the filer report, making it have more columns than previous years.

##### Format
- The data consists of csv files organized according to year, with each annual file having 5 files/categories: contributions, debt, expenditures, basic filer info, and other receipts. While there is a readme file, it is largely ineffectual since it merely describes some of the data types. A more useful description can be found on the https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Pages/Technical-Specifications.aspx page, which redirects to a page that better details what the data is about. Even then one must consult the filing documentations like the report cover sheet, and its various Schedules, to better understand how some of the values (like filerType) can be intepreted.
- As mentioned, in 2022 there were changes to the format of the data, including concatenating the name fields (no more separate fields for first, middle and last name), the treasurer’s address fields were removed in the filer document, party codes were updated to be more intuitive, and some more minor adjustments. 

##### Features
- The following statewide offices, which can be grouped into administrative, judicial, and legislative appointments, are required to file out the finance reports: 

- Administrative:
1. GOV: Governor
2. LTG: Liutenant Gov
3. ATT: Attorney General
4. AUD: Auditor General 
5. TRE: State Treasurer

- Judicial:
6. SPM: Justice of the Supreme Court
7. SPR: Judge of the Superior Court
8. CCJ: Judge of the CommonWealth Court
9. CPJ: Judge of the Court of Common Pleas 
10. MCJ: Judge of the Municipal Court 
11. TCJ: Judge of the Traffic Crt

- Legislative:
12. STS: Senator (General Assembly)
13. STH: Representative (General Assembly)

-Other:
14. OTH: Other candidates for local offices

##### Limits to the dataset
- Although there is a README file detailing what the dataset columns are, the column don’t match up with the format of some of the documents, so one has to cross match manually which can be tricky, especially with columns that have many NaN values. 
- The Finance Report states that a record must be kept for any contribution over $10, but  “Contributions and receipts of $50.00 or less per contributor, during the reporting period, need not be itemized on the report” … this might mean that if 1,000 people for instance donate $50 or less, there could be potentially thousands/tens of thousands of $$ not shown on the data.

##### Other Relevant Information
1. Candidates, political committees, and contributing lobbyists are required to disclose expenditures and contributions through the Campaign Finance Report document. However they can fill out a statement in lieu of a full report when the amount of contributions (including in-kind contributions) received, the amount of money expended, and the liabilities incurred each did not exceed $250.00 during the reporting period. This means that the cumulative contributions received, the money expended, and liabilities incurred in the entire reporting period (campaign cycle), each did not exceed $250.00. 

2. Debts that are forgiven are considered contributions, but double counting is prevented as the data is reviewed and updated months after the last filing period, allowing for data that was classified as debt to be itemized as a contribution. Corporations or unincorporated associations are prohibited from forgiving debts and thus contributing in this manner.   

3. The Finance Report states that a record must be kept for any contribution over $10, but “Contributions and receipts of $50.00 or less per contributor, during the reporting period, need not be itemized on the report” … this might mean that if 1,000 people for instance donate $50 or less, there could be potentially thousands/tens of thousands of $$ not shown on the data, even though this information is recorded. This means that the total contributions that filers itemize does not necessarily reflect the total contributions they received. 

4. Transparency USA has aggregated data on the contributions of individuals and committees. This could be a helpful source to cross-check the data and potentially help alleviate the debt-contribution issue.    