import pandas as pd

input_file = "orcidxofs_2.csv"
output_file = "osfNameDataset.csv"

df = pd.read_csv(input_file)

df_cleaned = df.drop_duplicates(subset='orcid', keep='first')
df_cleaned.to_csv(output_file, index=False)

print("Duplicate entries removed and cleaned data saved to cleaned_userinfo.csv")
