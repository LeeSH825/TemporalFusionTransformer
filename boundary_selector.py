"""
    Usage:
        python boundary_selector.py training_boundary validation_boundary test_boundary
        (with no arguments, boundary set to defaults: (60:20:20))
    Needs:
        ./csv_data/final.csv

    Output:
        ./csv_data/*final_slected.csv

    TODO:
        remove NaN of fcst & compare [valid: NaN] ratio
"""


import pandas as pd
import numpy as np
import sys

args = sys.argv


def debug(df, path):
    """
        print file path, dataframe, column info
    """
    print(path + ":", end='\n')
    print(df)
    print("col:\n", df.columns)
    print("\n")


def boundary_slice(df, col_name, boundary):
    """
        Args:
            df: dataframe to slice
            col_name: dataframe's column to slice
            boundary: boundary between [training + validation | test]
        Return:
            output = [training_set + validation_set, test_set](axis=column)
    """
    temp1 = df['obs' + '_' + col_name].iloc[0:round(boundary)].values # returns ndarray
    temp1 = pd.DataFrame(temp1, columns=[col_name])
    temp2 = df['fcst' + '_' + col_name].iloc[round(boundary):len(df)].values
    temp2 = pd.DataFrame(temp2, columns=[col_name])
    output = pd.concat([temp1, temp2], ignore_index=True)
    return(output)


"""
    DataFrame Analysis

    # of Regions: 2
    # of Dates: 1068
    Time format : 00 ~ 23
    # of total Datasets: 1068 * 24 * 2 = 51254
    actual # of total Datasets = 51258
"""
if __name__ == '__main__':
    data_path = './' + 'csv_data' + '/'

    unprocessed_df = pd.read_csv(data_path + 'final' + '.csv')

    total_num = len(unprocessed_df)
    train_rate = 60
    valid_rate = 20
    test_rate = 20

    # parsing arguments
    if len(args) == 1:
        pass
    else:
        train_rate = int(args[1])
        valid_rate = int(args[2])
        test_rate = int(args[3])

    boundary_rate = (train_rate + valid_rate) / (train_rate + valid_rate + test_rate)
    processed_df_set = []
    print('valid region: ( ', end='')
    for name, group in unprocessed_df.groupby('ID'):
        # common data type classifier
        df = pd.DataFrame()
        print(name, end='')
        print('(#=' + str(len(group)) + ')', end=' ')
        group.reset_index(inplace=True)
        df['id'] = group['ID'] # ID: plant
        df['region'] = group['region']
        df['date'] = pd.to_datetime(group['time'])
        df['month'] = df['date'].dt.month
        df['week_of_year'] = df['date'].dt.isocalendar().week
        df['day_of_month'] = df['date'].dt.day
        df['days_from_start'] = (df['date'] - pd.to_datetime('2018-03-01 00:00')).dt.days

        # boundary data type classifier
        boundary = boundary_rate * len(group)
        df['temperature'] = boundary_slice(group, 'temp', boundary)
        df['wind_speed'] = boundary_slice(group, 'windSpd', boundary)
        df['wind_direction'] = boundary_slice(group, 'windDir', boundary)
        df['humidity'] = boundary_slice(group, 'humid', boundary)
        df['cloud'] = boundary_slice(group, 'cloud', boundary)

        # copy energy values
        df['energy'] = group['energy']

        # debug
        # debug(df, name)
        processed_df_set.append(df)
    print(')')
    print('Select with rate( test : validation : test ) = (', train_rate, ':', valid_rate, ':', test_rate, ')')
    processed_df = pd.concat(processed_df_set, ignore_index=True)

    # debug
    # debug(processed_df, 'processed')

    print('Selection Done.')
    processed_df.to_csv(data_path + 'processed' + '.csv', index=False)
    print('Saved to: ', data_path + 'processed' + '.csv')
