import pandas as pd


def clean_main_talent():
    df = pd.read_csv("main_talent.csv")
    df["name"] = df["name"].str.lower()
    df = df.drop_duplicates(subset="name", keep='first', inplace=False)
    df.to_csv("clean_main_talent.csv", index=False)


clean_main_talent()
