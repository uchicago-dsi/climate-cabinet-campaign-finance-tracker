"""EDA on company data to improve their classification"""

import pandas as pd

organizations_df = pd.read_csv(transformed_data / "organizations_table.csv")
print(organizations_df.head())
organizations_df = organizations_df.dropna()

# instances of ff keywords such as "oil", "gas", or "coal"
ff_keywords = ["oil", "gas", "coal"]

ff_keywords_dict = {}

for keyword in ff_keywords:
    ff_keywords_dict[keyword] = organizations_df[
        organizations_df["name"].str.contains(keyword)
    ]

# no organizations with specified ff keywords
print(ff_keywords_dict)

# instances of clean energy keywords
clean_keywords = ["clean", "renewable", "energy", "wind", "solar"]

clean_keywords_dict = {}

for keyword in clean_keywords:
    clean_keywords_dict[keyword] = organizations_df[
        organizations_df["name"].str.contains(keyword)
    ]

# no organizations with specified clean keywords
print(clean_keywords_dict)
