import sys
import pandas as pd
import datetime

args = sys.argv

def plus_forecast(df):
    append = pd.to_datetime(df['Forecast time']) + pd.to_timedelta(df['forecast'], 'h')
    output = df.copy()
    output['Forecast time'] = append
    # output = df.assign(NEW=append)
    return(output)

first_ignore = 1
if len(args) == 1:
    exit()
else:
    for x in args:
        if first_ignore == 1:
            first_ignore = 0
        else:
            df = pd.read_csv(x)
            output = plus_forecast(df)
            print(x, ":\n", )
            print(output.head())
            print("col:\n", output.columns)
            print("\n\n")
            output.to_csv('new_' + 'csv', index=False)
            # df1 = df['time']
            # print(df1.head())

