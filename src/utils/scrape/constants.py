"""Constants related to scraping"""

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    "(KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
MAX_TIMEOUT = 10

AZ_pages_dict = {
    "Candidate": 1,
    "PAC": 2,
    "Political Party": 3,
    "Organizations": 4,
    "Independent Expenditures": 5,
    "Ballot Measures": 6,
    "Individual Contributors": 7,
    "Vendors": 8,
    "Name": 11,
    "Candidate/Income": 20,
    "Candidate/Expense": 21,
    "Candidate/IEFor": 22,
    "Candidate/IEAgainst": 23,
    "Candidate/All Transactions": 24,
    "PAC/Income": 30,
    "PAC/Expense": 31,
    "PAC/IEFor": 32,
    "PAC/IEAgainst": 33,
    "PAC/BMEFor": 34,
    "PAC/BMEAgainst": 35,
    "PAC/All Transactions": 36,
    "Political Party/Income": 40,
    "Political Party/Expense": 41,
    "Political Party/All Transactions": 42,
    "Organizations/IEFor": 50,
    "Organizations/IEAgainst": 51,
    "Organizations/BMEFor": 52,
    "Organizations/BME Against": 53,
    "Organizations/All Transactions": 54,
    "Independent Expenditures/IEFor": 60,
    "Independent Expenditures/IEAgainst": 61,
    "Independent Expenditures/All Transactions": 62,
    "Ballot Measures/Amount For": 70,
    "Ballot Measures/Amount Against": 71,
    "Ballot Measures/All Transactions": 72,
    "Individuals/All Transactions": 80,
    "Vendors/All Transactions": 90,
}
