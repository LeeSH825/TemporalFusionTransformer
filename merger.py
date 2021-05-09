import sys
import pandas as pd
import datetime

args = sys.argv


def plus_forecast(df):
    """
        set actual forecast time
    """
    append = pd.to_datetime(df['Forecast time']) + pd.to_timedelta(df['forecast'], 'h')
    output = df.copy()
    output['Forecast time'] = append
    return(output)


if len(args) == 1:
    exit()
else:
    df1 = pd.read_csv(args[1])
    df1 = plus_forecast(df1)
    df1 = df1.rename({'Forecast time': 'time'}, axis='columns')

    df2 = pd.read_csv(args[2])
    df2 = df2.rename({'일시': 'time'}, axis='columns')
    df2['time'] = pd.to_datetime(df2['time'])

    print(args[1], ":\n", )
    print(df1)
    print("col:\n", df1.columns)
    print("\n\n")
    print(args[2], ":\n", )
    print(df2)
    print("col:\n", df2.columns)
    print("\n\n")

    df3 = pd.merge(df1, df2, how='outer', on='time')
    print('merge:\n')
    print(df3)
    df3.to_csv('new_merge' + '.csv', index=False)
    # df1 = df['time']
    # print(df1.head())

