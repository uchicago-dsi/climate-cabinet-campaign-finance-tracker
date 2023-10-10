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
