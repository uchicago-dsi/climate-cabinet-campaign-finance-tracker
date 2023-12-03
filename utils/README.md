**Guide on running the Arizona state cleaner**

**The Files**

The state cleaner is located in utils/arizona_cleaner.py. 

It is further supported by the following files:

utils/clean.py contains the base abstract class which the Arizona cleaner is built upon. 

utils/cleaner_utils.py contains utility functions necessary to the cleaner's functioning. 

utils/constants.py contains constants necessary to the cleaner's functioning, including the filepaths for the files to be used in the demo. 

utils/az_curl_crawler.py is the web crawler/scraper which gathered the data which the state cleaner processes. It is not necessary for the purposes of this demo. 

**Information**

The state cleaner takes three files as input. They are in the google drive as az_transactions_demo.csv, az_individuals_demo.csv, and az_orgs_demo.csv. The filepaths leading to them (assuming one is working in the google drive) are in constants.py and at the bottom of arizona_cleaner.py. 

Each file contains a subset of arizona electoral finance data of each of the six categories we examine: those being candidates, individual contributors, political action committees (PACs), poltical parties, vendors, and organizations (which the Arizona database uses as a catch-all term). 

az_transactions_demo.csv contains transaction-level data: each row is a transaction, with a little over 50 columns of information, most of which gets filtered out. 

az_individuals_demo.csv contains information on the individuals (for us, that means individual contributors and candidates) involved in transactions in the dataset. 

az_orgs_demo.csv contains information on the organizations (those being the PACs, parties, 'organizations' and vendors) involved in transactions in the dataset. 

**Running the State Cleaner**

The arizona_cleaner.py file has an if __name__ == "__main__" clause at the bottom which will run the cleaner on the demo data. The data must be available via the specified filepaths (laid out in utils/constants). 

Otherwise, one may call ArizonaCleaner.clean_state([]) with the relevant filepaths inside the list as the only argument. Noe that the files must go in the order: transactions, individuals, organizations. 



Note that since this is a subset of the data, many of the recipients/contributors listed in the final transactions table are not present in the individuals or organizations tables. For example, the original id '-1' refers to anonymous individual contributors within the state of arizona who made donations under $100. We have chosen not to include them in this demo subset because they massively inflate the number of transactions but give us almost no information. However, in all cases, at least one of either the donors or recipients is listed in either the final individuals or organizations tables. 

