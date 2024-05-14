import pandas as pd

individuals_df = pd.read_csv('src/score/individuals_table_TX.csv')
score_df = pd.read_csv('src/score/score_TX.csv')

individuals_df['FIRST_NAME'] = individuals_df['FIRST_NAME'].str.title()
individuals_df['LAST_NAME'] = individuals_df['LAST_NAME'].str.title()
score_df['First Name'] = score_df['First Name'].str.title()
score_df['Last Name'] = score_df['Last Name'].str.title()

merged_df = pd.merge(individuals_df, score_df, how='inner', left_on=['FIRST_NAME', 'LAST_NAME'], right_on=['First Name', 'Last Name'])

output_columns = ['ID', 'ENTITY_TYPE_SPECIFIC', 'ENTITY_TYPE_GENERAL', 'FULL_NAME', 'LAST_NAME', 'FIRST_NAME', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2', 'CITY', 'STATE', 'ZIP_CODE', 'PHONE_NUMBER', 'OFFICE_SOUGHT', 'DISTRICT', 'ENTITY_TYPE', 'ORIGINAL_ID', 'EMPLOYER', 'OCCUPATION', 'Chamber', 'Party']
result_df = merged_df[output_columns]

result_df.to_csv('src/score/score_match.csv', index=False)

print("Matched records have been saved to 'score_match.csv'.")
