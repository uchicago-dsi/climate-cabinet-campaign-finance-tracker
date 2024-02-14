import pandas as pd

from utils.linkage import calculate_string_similarity

#we want to run down a list of people and, hopefully, their adresses, plus a list of 
#corporations, groups, etc, and classify them, basically just looking for matches

#do we want to just input all the names/people (there's not many, less than 200 for sure), 
#give a string similarity match score, and extract the top ten for manual review?
#thsi should give us a feeling for how to set our threhsold
#we might also, once we have all the data, buckle down and just classify some of them manually

inds_list = []

#a list of individual names


def similarity_calculator(df: pd.DataFrame, suspect):
    """Run through a pandas dataframe column and compare elements to a constant
    
    """
    # this needs to output somehting useful

    similarities = df['column1'].apply(lambda x: calculate_string_similarity(x, suspect))
    return similarities


#crawl through list automatically once a threshold has been set
def

for i in inds_list:
    similarities = similarity_calculator(data, i)

    similarities
    #




    df.apply(calculate_string_similarity, inds_list) #very psuedocode
    #get top n, maybe just ten, and output

    #we can use the indices and/or select manually, just add a new column to the subjects table
    #that marks fossil fuels, green energy, or neither



