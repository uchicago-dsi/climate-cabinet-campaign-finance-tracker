### Data

This directory contains information for use in this project. 

Please make sure to document each source file here.

#### Michigan Campaign Finance Data

##### Summary
The Michigan Campaign Finance data are publicly available on the 
[Michigan Department of State Website](https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/).
The records include Committee information, Contributions and Expenditures, and Late Contribution Reports.

The developers of this project have stored the 1995-2023 Michigan campaign finance 
expenditures and contribution data and READMEs in a Google Drive for the duration of this project.

##### Access Limitations
A captcha button functions as an anti-web scraping defense; however, the yearly campaign finance records 
are directly available at the link above. Please note that the records are reuploaded nightly to the Secretary of State website.


##### Features
The Michigan campaign contributions and expenditures dataset covers races from 
1995 to 2023 (Note: the 1995 data only has expenditures and 1996 is not included).
The contributions data are stored in tab separated text files with naming conventions {year}_mi_cfr_contribution_00.txt, in which only the 00 file contains a header. 
The expenditure data follows a {year}_mi_cfr_expenditures.txt format, and only had one file per year. Both the expenditures and contributions data includes RUNTIME in the header which indicates the time these transactions were exported from the Bureau of Elections database. RUNTIME is only indluded in the header.

- What races are included in the dataset (ie state legislator, mayoral, city council, etc)?
The MI Campaign Finance Data includes contributions/expenditures for federal and state legislators and representatives, as well as local elections.

- Who is required to report their contributions (ie individuals, candidate committees, PACs, etc)?
Beginning in January 2014, committees spending or receiving $5,000 or more in a calendar year were required to file electronically. 
The Michigan Secretary of State website notes that, "when filing electronically, all of the information submitted by the committee is made 
available and information is not changed or manipulated by the Bureau of Elections".

Independent Expenditures from an individual of over $100.01 in a calendar year must be reported with the individuals employer information and occupation. The committee reveiving this information must report it on campaign statements. More iformations on Individuals and the Michigan Campaign Finance Act (MCFA) is available [here](https://mertsplus.com/mertsuserguide/index.php?n=MANUALS.AppendixQ).


#### Minnesota Campaign Finance Data

##### Summary
- The Minnesota Campaign Finance data are publicly available on the 
[Minnesota Campaign Finance and Public Disclosure Board](https://cfb.mn.gov/reports-and-data/self-help/data-downloads/campaign-finance/) in csv format and has no anti-webscraping defenses. 

- However, there is an glitch in the data available through the Data Downloads page above: this dataset does not include contributions reported by the committees of candidates for State Court of Appeals Judge. Consequently, I have utilized an alternative dataset provided by the Minnesota Campaign Finance website developer. This dataset comprises 10 separate CSV files, each documenting contributions made to a specific recipient type from 1998 to 2023. I have consolidated these files into a single dataset to ensure comprehensive coverage.

- The old dataset comprises itemized records of contributions and expenditures made since 2015, specifically including transactions exceeding $200, which aligns with the reporting threshold set at $200 in Minnesota campaign finance regulations. The new dataset itemizes all contributions to candidates from 1998 to 2023.

- For the purpose of our project I will focus on contribution, not expenditure.

##### Features
- Races / Office Sought:
    1. Governor (GC) 
    2. Attorney General(AG)
    3. Secretary of State(SS)
    4. State Auditor(SA)
    5. State Treasurer (ST, this office was abolished in 2003 and no longer exists)
    6. State Senator(Senate)
    7. State Representative(House)
    8. State Supreme Court Justice(SC)
    9. State Appeals Court Judge(AP) 
    10. State District Court Judge(DC)

- This dataset covers 1998 to present

- Trasactions required to report and itemize: Contributions received from any particular source in excess of $200 within a calendar year

- Limitation:
    1. This new dataset only covers contributions made to candidates, i.e., all recipients are candidates
    2. Only covers contributions over 200$ by MN campaign finance regulation
    3. This dataset only dates back to 1998. Pre-1998 is not digitized so access to that data is limited to paper reports.

- Additional information: 
    1. in-kind: Donations of things other than money are in-kind contributions to the receiving entity
    2. For the purpose of our project, I created a separate column of total donation by summing both monetary donation and in-kind donation
    3. Type and Subtype Acronym:
        - PCC: Political Contribution Committee
        - PTU: Political Party Unit
        - PCF - Political Committee Fund
        - PF: Political Fund
        - PC: Political Committee
        - PCN: Positive Community Norms
        - PFN: Professional Fundraising Network
        - IEF: Independent Expenditure Fund
        - IEC: Independent Expenditure Committee
        - BC: Ballot Committee
    4. Recipient Type and Subtype: 
        - Candidates: Recipient Type PCC 
        - Party Units: Recipient Type PTU
        - State Party Units: Recipient Type PTU, Recipient Subtype SPU
        - Party Unit Caucus Committees: Recipient Type PTU, Recipient subtype CAU
        - Local Party Units: Recipient Type PTU
        - Committees and Funds: Recipient Type PCF, Recipient Subtype PF, PC, PCN, PFN, IEF, IEC, BC
        - Independent Expenditure Committees and Funds: Recipient Type PCF, Recipient Subtype IEF, IE
    5. Contributors whose total contributions exceed $200 are individually itemized in separate rows. Contributions from donors who each give $200 or less are reported as aggregate totals and are not included in this dataset by definition.
    6. Contributor/donor Types:
        - C: Candidate Committee 
        - I: Individual 
        - L: Lobbyist  
        - F: Political Committee/Fund  
        - S: Self 
        - P: Party Unit 
        - H: Registered with Hennepin County 
        - O: Other 
    7. The new dataset has 467 missing rows, of which belong to "Registration fee for Netroots event" and have no recipient, donor, or total donation amount.

#### Pennsylvania Campaign Finance Data
##### Accessibility
- The data comes from the Pennsylvania Government Website’s Full Finance Campaign Report section. To see the actual forms and reports those can be found here: https://www.dos.pa.gov/VotingElections/CandidatesCommittees/FormsReports/Pages/default.aspx

##### Format
- The data consists of csv files organized according to year, with each annual file having 5 files/categories: contributions, debt, expenditures, basic filer info, and other receipts. While there is a readme file, it is largely ineffectual since it merely describes some of the data types. A more useful description can be found on the https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Pages/Technical-Specifications.aspx page, which redirects to a page that better details what the data is about. Even then one must consult the filing documentations like the report cover sheet, its various Schedules.
- In 2022 there were changes to the format of the data, including concatenating the name fields (no more separate fields for first, middle and last name), the treasurer’s address fields were removed in the filer document, party codes were updated to be more intuitive, and some more minor adjustments. 

###### Defenses
None

##### Races
Races included (i.e state legislator, mayoral, city council, etc)
- Seems to be filed by any candidates running for any level of government, notably statewide, legislative, and judicial offices (general and municipal elections)

##### Years that the Dataset covers: 1987-2023

##### Reporting:
Who is required to report their contributions:
- Candidates, political committees, and contributing lobbyists are required to disclose expenditures and contributions through the Campaign Finance Report document. However they can fill out a statement in lieu of a full report when the amount of contributions (including in-kind contributions) received, the amount of money expended, and the liabilities incurred each did not exceed $250.00 during the reporting period. This means that the cumulative contributions received, the money expended, and liabilities incurred in the entire reporting period (campaign cycle), each did not exceed $250.00  

- Unless the incurred contributions and expenditures do not individually exceed $250. If they don’t exceed then they can file a statement instead of a report. I am unsure where this leaves individuals, but I am still scouring for this information. 


##### Limits to the dataset
- Although there is a README file detailing what the dataset columns are, the column don’t match up with the format of some of the documents, so one has to cross match manually which can be tricky, especially with columns that have many NaN values. 

- The Finance Report states that a record must be kept for any contribution over $10, but  “Contributions and receipts of $50.00 or less per contributor, during the reporting period, need not be itemized on the report” … this might mean that if 1,000 people for instance donate $50 or less, there could be potentially thousands/tens of thousands of $$ not shown on the data.

##### Other Relevant Information
1. Filer Document: Contains info about each filer. Filers range from interest groups, individuals, committees (including PACs like VisionPAC, Build PA PAC...etc), and private organizations. The filer document is needed to trace the contributions dataset to the individuals who filed them.

2. Debts that are forgiven are considered contributions, but double counting is prevented as the data is reviewed and updated months after the last filing period, allowing for data that was classified as debt to be itemized as a contribution. Corporations or unincorporated associations are prohibited from forgiving debts and thus contributing in this manner.

3. Transparency USA has aggregated data on the contributions of individuals and committees. This could be a helpful source to cross-check the data and potentially help alleviate the debt-contribution issue.

