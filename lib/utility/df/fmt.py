import pandas as pd

def fmt_head(df: pd.DataFrame) -> str:
    with pd.option_context('display.max_columns', None):
        return str(df.head())
