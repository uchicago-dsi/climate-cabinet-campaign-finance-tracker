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


=======
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


=======

#### Minnesota Campaign Finance Data

##### Summary
- The Minnesota Campaign Finance data are publicly available on the 
[Minnesota Campaign Finance and Public Disclosure Board](https://cfb.mn.gov/reports-and-data/self-help/data-downloads/campaign-finance/) in csv format and has no anti-webscraping defenses. 

- This dataset comprises itemized records of contributions and expenditures made since 2015, specifically including transactions exceeding $200, which aligns with the reporting threshold set at $200 in Minnesota campaign finance regulations. Specifically, the dataset includes:
    - *itemized contributions received of over $200*, 
    - *itemized general expenditures and contributions made of over $200*, 
    - *itemized independent expenditures of over $200*

- For the purpose of our project we will focus on *Itemized contributions received of over $200*.

##### Features
- According to [Transparency USA](https://www.transparencyusa.org/mn/races), MN races includes Attorney General, Governor, Lieutenant Governor, District Court, Court of Appeals, House of Representatives, Secretary of State, State Auditor, State Senate, Supreme Court. I have verified the completeness of this list through *All registered candidatecommittees* on the Minnesota Campaign Finance and Public Disclosure Board and the Executive Director of Minnesota Campaign Finance and Public Disclosure Board.

- This dataset covers 2015 to present

- Trasactions required to report and itemize:
    1. Contributions received from any particular source in excess of $200 within a calendar year
    2. Expenditures made to any particular individual or association in excess of $200 within a calendar year
    3. Contributions made to any principal campaign committee, local candidate, political committee or fund, or political party unit in excess of $200 within a calendar year
    4. Contributions and expenditures made by Ballot question political committees and funds that exceed $500

- Limitation:
    1. This dataset only covers contributions and expenditures over 200$ by MN campaign finance regulation
    2. This dataset only dates back to 2015. Highly detailed summaries of campaign finance data reported to the Board for each two-year period from 2005 through 2015 are available on [the Board’s website](https://cfb.mn.gov/publications/programs/reports/campaign-finance_summaries/) in pdf format. Pre-2005 data is not publicly available and can contact the board developer for such information.  Pre-1998 is not digitized so access to that data is limited to paper reports.
    3. Both party units and local party units have recipient type PCC and no distinguishable subtype

- Additional information: 
    1. in-kind: Donations of things other than money are in-kind contributions to the receiving entity
    2. Type and Subtype Acronym:
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
    3. Recipient Type and Subtype: 
        - Candidates: Recipient Type PCC 
        - Party Units: Recipient Type PTU
        - State Party Units: Recipient Type PTU, Recipient Subtype SPU
        - Party Unit Caucus Committees: Recipient Type PTU, Recipient subtype CAU
        - Local Party Units: Recipient Type PTU
        - Committees and Funds: Recipient Type PCF, Recipient Subtype PF, PC, PCN, PFN, IEF, IEC, BC
        - Independent Expenditure Committees and Funds: Recipient Type PCF, Recipient Subtype IEF, IE
    4. Contributors whose total contributions exceed $200 are individually itemized in separate rows. Contributions from donors who each give $200 or less are reported as aggregate totals and are not included in this dataset by definition.
    5. Contributors are categorozed as Candidates, Party units, State party units,Party unit caucu committees, Local party units, Committees and funds, and Independent expenditure committees and funds

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

