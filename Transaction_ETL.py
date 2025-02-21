import pandas as pd

df = pd.read_csv("transactional_data.csv", sep=";")

#print(df.head())
#print(df.columns)


sub_set = df[["Customer_ID", "Date","Currency", "Exchange_Rate", "Country","Amount"]]

#print(sub_set.head())

sub_set.to_csv("filtered_data.csv", index=False)