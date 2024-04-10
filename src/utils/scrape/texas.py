import pandas as pd
import os

contrib_files = [file for file in os.listdir('/Users/yuexu/Downloads/TEC_CF_CSV') if file.startswith('contribs_') and file.endswith('.csv')]
expend_files = [file for file in os.listdir('/Users/yuexu/Downloads/TEC_CF_CSV') if file.startswith('expend_') and file.endswith('.csv')]

def mergeFiles(files,mergedFileName):
    merged_df = pd.DataFrame()

    for file in files:
        print(f"Processing File {file}...")
        try:
            df = pd.read_csv(os.path.join('/Users/yuexu/Downloads/TEC_CF_CSV', file), low_memory=False)  # Read CSV file into DataFrame
    #         if mergedFileName =="merged_trimmed_contribs":
    #             df = df[['reportInfoIdent',
    #    'receivedDt',  'filerIdent', 'filerTypeCd', 'filerName',
    #    'contributionInfoId', 'contributionDt', 'contributionAmount',
    #    'contributionDescr', 'contributorNameOrganization',
    #    'contributorNameLast', 'contributorNameSuffixCd',
    #    'contributorNameFirst', 'contributorNamePrefixCd',
    #    'contributorNameShort',  'contributorEmployer',
    #    'contributorOccupation', 'contributorJobTitle']]
            df= df.sample(50)
            merged_df = pd.concat([merged_df, df], ignore_index=True)  # Concatenate with merged DataFrame
        except FileNotFoundError:
            print(f"File {file} not found. Skipping...")
            continue

    merged_df.to_csv(f'/Users/yuexu/Desktop/practicum project/climate-cabinet-campaign-finance-tracker/data/raw/TX/sample/{mergedFileName}.csv', index=False)





mergeFiles(contrib_files,"contribs")
# mergeFiles(expend_files,"merged_expend")


