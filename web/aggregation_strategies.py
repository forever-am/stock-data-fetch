import pandas as pd

def average_strategy(dfs):
    """
    Calculate the average data frame of a list of dataframes of the same
    instrument. Basically taken from different sources.
    The dataframes don't have to contain the same range of market data.
    If several data frames contain a value for a couple (date, column)
    the result for this (date, column) will be the average of these values
    If just one dataframe contains a value for a couple (date, column)
    the result for this (date, column) will be this value
    If none/NaN value is provided, the result will be NaN
    :param dfs: the list of data frames to aggregate
    :return: a ref dataframe
    """
    if len(dfs) == 0:
        return None
    # numerator dataframe
    df_num = pd.DataFrame(index=dfs[0].index)
    # denominator dataframe
    df_denom = pd.DataFrame(index=dfs[0].index)
    for df in dfs:
        # mask to be added to the denominator. 0 if NaN, 1 if not
        df_mask = df.isnull().transform(
            lambda col: col.apply(lambda x: 0 if x else 1))

        # the numerator is the sum of the dataframes - filling in the NaN with 0
        df_num = df_num.append(df.fillna(0))

        # the denominator should be the number of non-NaN values for a couple
        # (date, column)
        df_denom = df_denom.append(df_mask)

    # the calculte the average
    # Note that a NaN could appear here if we get 0/0.
    return df_num.groupby("Date").sum()/df_denom.groupby("Date").sum()