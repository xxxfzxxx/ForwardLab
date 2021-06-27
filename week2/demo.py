import pandas as pd
import numpy as np


def load_dataset(path):
    df = pd.read_csv(path, index_col=0)
    df.fillna('unknown author', inplace=True)
    return df

