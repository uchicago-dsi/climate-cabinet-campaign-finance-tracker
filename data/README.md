# Data

This directory contains information for use in this project. 

## Arizona Campaign Finance Data

### Summary
- The Arizona Campaign Finance Data are publicly available at (https://seethemoney.az.gov/Reporting/) as a collection of tables which can be downloaded as aggregated CSVs. It has no overt webscraping defenses, but scraping the bulk transactions is non-trivial. This site is supported by the Arizona Secretary of State’s office. 


-  The dataset comprises records of contributions, expenditures, vendor payments and operating expenses for political entities within the State of Arizona. Individual contributions under $100 from within the state need not be identified by name, employer, or other identifying data, and are collected under pseudonyms such as 'Multiple Donors.' More specifically, the dataset includes
    - individual donations, with those over $100 or from out of state always accompanied by identifying information
    - Independent expenditures and ballot measure expenditures by PACs, political parties, and other organizations
    - Income and Expenses for candidate campaigns, political parties, PACs, and other organizations
    - Payments to and refunds from vendors with regard to the aforementioned entities
      
    -  This project will focus on individual, corporate, and PAC spendin.

### Features
 - This dataset includes comprehensive records on races for the office of Governor, Attorney General, Corporation Commissioner, Secretary of State, State Senator, State Mine Inspector, State Representative, State Treasurer, and Superintendent of Public Education. Other races, such as mayoral or federal Congressional races, appear incomprehensively within the dataset, and will not be studied.
   
 - The dataset covers the years 2002 - present

 - Data is divided into 8 sections: Candidates, PACs, Political Parties, Organizations, Indepndent Expenditures, Ballot Measures, Individual Contributions, and Vendors. 

 - Transactions required to report and itemize:
    1. Contributions in excess of $100 or from out of state
    2. Expenses in excess of $250
    3. Independent Expenditures and Ballot Measure Expenditures

- Limitation:
    1. Small-money contributions under $100 from within the state need not be itemized (though they frequently still are).
    2. The easily available CSVs are heavily aggregated, and do not list both payer and payee. Detailed transaction data with dates, precise amounts, and both payer and payee are available, but they must be accessed individually, and over a million such records exist.
    3. Lobbyist spending is not distinguished, and spending by political organizations is often obfuscated by listing them as vendors.

- Additional information:
    1. Negative expenditures in the dataset are enclosed within parentheses. These expenditures indicate refunds to donors, loans paid, and similar transaction. 

## Michigan Campaign Finance Data

### Summary
The Michigan Campaign Finance data are publicly available on the 
[Michigan Department of State Website](https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/)
in txt format and has a captcha button functions as an anti-web scraping defense; however, the yearly campaign finance records are directly available at the link above. 

The developers of this project have stored the 1995-2023 Michigan campaign finance 
contribution data and READMEs in a Google Drive for the duration of this project.

### Features
- This dataset covers 1998 to 2023
- The contributions data are stored in tab separated text files with naming conventions {year}_mi_cfr_contribution_00.txt, in which only the 00 file contains a header. The data includes RUNTIME in the header which indicates the time these transactions were exported from the Bureau of Elections database. RUNTIME is only indluded in the header.

- Transactions Required to Report
    1. Beginning in January 2014, committees spending or receiving $5,000 or more in a calendar year were required to file electronically.
    2. The Michigan Secretary of State website notes that, "when filing electronically, all of the information submitted by the committee is made available and information is not changed or manipulated by the Bureau of Elections".
    3. Independent Expenditures from an individual of over $100.01 in a calendar year must be reported with the individuals employer information and occupation. The committee reveiving this information must report it on campaign statements. More iformations on Individuals and the Michigan Campaign Finance Act (MCFA) is available [here](https://mertsplus.com/mertsuserguide/index.php?n=MANUALS.AppendixQ).

- Additional information: 
    1. The MI Campaign Finance Data includes contributions/expenditures for federal and state legislators and representatives, as well as local elections.
    2. Contribution Type Accronyms 
        - DIS: District Party
        - STA: State Party
        - BAL: Ballot Question
        - COU: County Party
        - POL: Political Party
        - GUB: Gubernatorial
        - CAN: Candidate
        - IND: Independent PAC.   


## Minnesota Campaign Finance Data

### Summary
- The Minnesota Campaign Finance data are available in this shared
[Google Drive](https://drive.google.com/drive/u/2/folders/1uA70woWDhTf3_0F8AbadDa_XIKraCeoc) in zip format and has no anti-webscraping defenses. Please first unzip it and store 12 csv files (10 candidate-recipient contribution dataset, 1 noncandidate-recipient dataset, and 1 expenditure dataset) to local repo in this format: repo root / "data" / "file name"

- The above dataset is provided by the Minnesota Campaign Finance website developer. This dataset includes 10 separate CSV files, each documenting contributions made to a specific recipient type from 1998 to 2023. This dataset also includes a non-candidate contribution dataset dating back to 1998 and an independent expenditure dataset dating back to 2015.

- MN dataset comprises itemized records of contributions and expenditures exceeding $200, which aligns with the reporting threshold set at $200 in Minnesota campaign finance regulations. 

- For the purpose of our project I will focus on contribution and independent expenditure from 2018 to 2023.

### Features
- Races / Office Sought:
    - AG: Attorney General
    - AP: State Appeals Court Judge
    - DC: State District Court Judge
    - GC: Governor
    - House: State Representative
    - SA: State Auditor
    - SC: State Supreme Court Justice
    - Senate: State Senator
    - SS: Secretary of State
    - ST: State Treasurer (this office was abolished in 2003 and no longer exists)

- Donor Types:
    - I: Individual 
    - L: Lobbyist  
    - C: Candidate Committee 
    - F: Political Committee/Fund  
    - S: Supporting Association
    - P: Party Unit
    - B: Businness
    - H: Hennepin County Local Candidate Committee
    - U: Association Not Registered in Board
    - O: Other 
    - PTU: Political Party Unit
    - PCF: Political Committee and Fund

- Trasactions required to report and itemize: Contributions received from any particular source in excess of $200 within a calendar year

- Limitation: Only covers contributions over 200$ by MN campaign finance regulation


- Additional information: 
    1. in-kind: Donations of things other than money are in-kind contributions to the receiving entity
    2. For the purpose of our project, I created a separate column of total donation by summing both monetary donation and in-kind donation
    3. Contributors whose total contributions exceed $200 are individually itemized in separate rows. Contributions from donors who each give $200 or less are reported as aggregate totals and are not included in this dataset by definition.
    4. The dataset has 467 missing rows, of which belong to "Registration fee for Netroots event" and have no recipient, donor, or total donation amount.


## Pennsylvania Campaign Finance Data
### Summary
- The Pennsylvania Campaign Finance Data comes from the [Pennsylvania Department of State Website’s Full Campaign Finance Export section](https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Pages/FullCampaignFinanceExport.aspx). Although only some years are visible on this page, the url for each year follows the same format. To see the actual forms and reports those can be found here: https://www.dos.pa.gov/VotingElections/CandidatesCommittees/FormsReports/Pages/default.aspx. No defenses or anti-captcha mechanisms exist to monitor access to the data. The data is stored in the form of csvs, but is named with a .txt and .zip tag depending on the year. This is because while the data spans from 1987-2022, there are incongruences in the formatting. The pre-2000 data have their sub-categories listed as separate links in the form of .txt, while the post-2000 years have each year as a .zip nested file that contains the sub-categories. Additionally, 2022 has 2 additional fields (Timestamp & Reported ID) in the filer report, making it have more columns than previous years.

### Format
- The data consists of csv files organized according to year, with each year having 5 files representing 5 categories. Although the exact filenames are not consistent, they always start with the same string (at least since 2010): `contrib` (contributions), `debt` (debt), `expense` (expenditures), `filer` (basic filer info), and `receipt` (other receipts). While there is a readme file, it is largely ineffectual since it merely describes some of the data types. A more useful description can be found on the https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Pages/Technical-Specifications.aspx page, which redirects to a page that better details what the data is about. Even then one must consult the filing documentations like the report cover sheet, and its various Schedules, to better understand how some of the values (like filerType) can be intepreted.
- As mentioned, in 2022 there were changes to the format of the data, including concatenating the name fields (no more separate fields for first, middle and last name), the treasurer’s address fields were removed in the filer document, party codes were updated to be more intuitive, and some more minor adjustments. 

### Features
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

### Other Relevant Information
1. Candidates, political committees, and contributing lobbyists are required to disclose expenditures and contributions through the Campaign Finance Report document. However they can fill out a statement in lieu of a full report when the amount of contributions (including in-kind contributions) received, the amount of money expended, and the liabilities incurred each did not exceed $250.00 during the reporting period. This means that the cumulative contributions received, the money expended, and liabilities incurred in the entire reporting period (campaign cycle), each did not exceed \$250.00. 

2. Debts that are forgiven are considered contributions, but double counting is prevented as the data is reviewed and updated months after the last filing period, allowing for data that was classified as debt to be itemized as a contribution. Corporations or unincorporated associations are prohibited from forgiving debts and thus contributing in this manner.   

3. The Finance Report states that a record must be kept for any contribution over \$10.00, but “Contributions and receipts of \$50.00 or less per contributor, during the reporting period, need not be itemized on the report” … this might mean that if 1,000 people for instance donate \$50 or less, there could be potentially thousands/tens of thousands of \$ not shown on the data, even though this information is recorded. This means that the total contributions that filers itemize does not necessarily reflect the total contributions they received. 

4. Transparency USA has aggregated data on the contributions of individuals and committees. This could be a helpful source to cross-check the data and potentially help alleviate the debt-contribution issue. Pennsylvania' Dept. of State also offers a detailed website that shows all the aggregated contributions made and received, expenditures made, debts, and receipts. The catch is one must know which candidate they are looking for as it's a searchable database, but it can be very helpful for cross-matching and verification. Here's the link :https://www.campaignfinanceonline.pa.gov/Pages/CFReportSearch.aspx 

## Texas

Texas data is retrieved from 'Campaign Finance CSV Database' from the [Texas Ethics Commission](https://www.ethics.state.tx.us/search/cf/)