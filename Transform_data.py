import pandas as pd

df = pd.read_csv("filtered_data.csv", sep=",")

#creating new field for EUR equivelient for currency amount
df["Amount_in_EUR"] = df["Amount"] / df["Exchange_Rate"]
df["Amount_in_EUR"] = round(df["Amount_in_EUR"], 2)

#Date to period format
df["Date"] = pd.to_datetime(df["Date"])
df["Date"] = df["Date"].dt.to_period("M")

#renaming fields
df_rename = df.rename(columns={"Date": "period", "Customer_ID": "customer_id", "Currency": "currency", "Exchange_Rate": "rate",
                               "Country": "country", "Amount": "amount", "Amount_in_EUR": "amount_eur"})

#print(df_rename)


#matching currency with country
df_rename["country"] = df_rename["currency"].map({
    "USD": "USA",
    "EUR": "Germany",
    "GBP": "UK",
    "JPY": "Japan",
    "CAD": "Canada"
})


#ordering columns for better reading
df_rename = df_rename.sort_values(by=["customer_id", "period"])

#Windows functions
df_rename["amount_m"] = df_rename.groupby(["customer_id", "period"])["amount"].transform("sum")
df_rename["amount_m_eur"] = df_rename.groupby(["customer_id", "period"])["amount_eur"].transform("sum")

df_rename["amount_m_avg"] = df_rename.groupby(["customer_id", "period"])["amount"].transform("mean")
df_rename["amount_m_eur_avg"] = df_rename.groupby(["customer_id", "period"])["amount_eur"].transform("mean")

df_rename["amount_l3m"] = (
    df_rename.sort_values("period")
    .groupby("customer_id")["amount"]
    .rolling(window=3, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)
df_rename["amount_eur_l3m"] = (
    df_rename.sort_values("period")
    .groupby("customer_id")["amount_eur"]
    .rolling(window=3, min_periods=1)
    .sum()
    .reset_index(level=0, drop=True)
)



#df_rename["period"] = df_rename["period"].dt.strftime("%m%Y")

#Final check that the data is your liking
sub_set = df_rename[["customer_id", "period", "amount", "amount_m", "country", "currency","rate", "amount_eur", "amount_m_eur", "amount_m_avg", "amount_m_eur_avg", "amount_l3m"]]

#print(sub_set)

#to parquet and csv file
sub_set.to_parquet("final_data.parquet", engine="pyarrow")
sub_set.to_csv("final_data.csv")

final = pd.read_parquet("final_data.parquet", engine="pyarrow")
print(final)


