### Notebook directory

This should contain information about what is done in each notebook

* `Test.ipynb` : This is a test notebook to demonstrate how to use this repository.

MN EDA
File Name: 
- MN_EDA: The clear jupyter notebook for MN EDA
- MN_util: the util file that stores necessary preprocessing functions in MN_EDA

1. 10 datasets on candidate-recipient contributions and 1 dataset on non-candidate-recipient contributions. They are seperate and not relational.
2. Clean dataset column: OfficeSought (object), RegNumb (float64), CandFirstName (object), CandLastName (object), Committee (object), DonationDate (datetime64), DonorType (object), DonorName (object), DonationAmount (float64), InKindDonAmount, InKindDescriptionText (object), RecipientType (object), DonationYear (float64), TotalAmount (float64)
3. Top 10 contributors in 2023: IBEW Local 292, SEIU local 26, Zarth John, Zarth Kelly, Krech Kathy, Restemayer Douglas, Carlson Jessica, Collins Greg, Collins Jane, and Doherty John. Top 10 recipients in 2023: Warren Limmer, Mary Murphy, Gregory Davids, Carla Nelson, Kim Crockett, Torrey Westrom, Scott Jensen, Leslie	Lienemann, James Schultz, and Beth Beebe.
4. Yearly Trends:
- Individuals, excluding lobbyists, constitute the largest share of contributions in the MN dataset.
- The second most substantial contributor category is General Purpose Political Committee or Fund, followed by lobbyists.
- Contributions from other donor types are notably lower throughout the years.
- Analyzing a sample from 2018 to 2022, we observe a cyclical pattern with a major increase in contributions, followed by three years of reduced contribution totals. This cycle aligns with the four-year election cycle.
- From 1998 to 2023, there are several years with significantly lower contribution amount: 1999, 2001, 2003, 2007, 2011.	

- Candidates, as the recipients, make up the overwhelming majority of contributions.
- Examining the period from 1998 to 2023, a distinct cyclical pattern emerges, characterized by alternating years of increased and decreased contributions, which may correspond to congressional elections or MN state house representatives elections which take place every two years.
- Starting in 2012, recipient types "Political Committee or Fund" and "Political Party Unit" began receiving a larger share of contributions compared to prior years.
