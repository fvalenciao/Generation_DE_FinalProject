import pandas as pd


def create_dataframe(filepath,columns):
    df = pd.read_csv(filepath, header=None)
    df.columns = columns
    return df
