import sys
import pandas as pd
import datetime
import email.utils as eutils
import time
args = sys.argv
"""
    Needs:
    ./csv_data/energy.csv
    ./csv_data/regionX_fcst_data.csv
    ./csv_data/regionX_obs_data.csv
    
    Usage:
    python merger.py region
    python merger.py region1 region2 ...

    Output:
    ./csv_data/final.csv
"""

def debug(df, path):
    """
        print file path, dataframe, column info
    """
    print(path + ":\n")
    print(df)
    print("col:\n", df.columns)
    print("\n\n")


def tag_region(df, region):
    """
        tag each region to distinguish
    """
    for col_name in df.columns.values:
        if col_name == 'time':
            continue
        elif col_name == 'region':
            continue
        else:
            df.rename(columns={col_name: region + '_' + col_name}, inplace=True)


def plus_forecast(df):
    """
        set actual forecast time
    """
    append = pd.to_datetime(df['Forecast time']) + pd.to_timedelta(df['forecast'], 'h')
    output = df.copy()
    output.insert(2, 'NEW', append)
    return(output)


def  fcst_process(df, region):
    """
        fcst data pre-processing
        column sorting, forecast date adjusting, column renameing
    """
    output = df.copy()
    output = output[[   'Forecast time', \
                        'forecast', \
                        'Temperature', \
                        'WindSpeed', \
                        'WindDirection', \
                        'Humidity', \
                        'Cloud']]
    output = plus_forecast(output)
    output = output.rename({'NEW': 'time'}, axis='columns')
    output = output.rename({'Forecast time': 'fcst_fcst', \
                            'Temperature': 'fcst_temp', \
                            'WindSpeed': 'fcst_windSpd', \
                            'WindDirection': 'fcst_windDir', \
                            'Humidity': 'fcst_humid', \
                            'Cloud': 'fcst_cloud'}, \
                            axis='columns')
    tag_region(output, region)
    return(output)


def obs_process(df, region):
    """
        obs data preprocessing
        column renaming
    """
    output = df.copy()
    output = output.rename({'일시': 'time'}, axis='columns')
    output['time'] = pd.to_datetime(output['time'])
    output = output.rename({'지점': 'region', \
                            '기온(°C)': 'obs_temp', \
                            '풍속(m/s)': 'obs_windSpd', \
                            '풍향(16방위)': 'obs_windDir', \
                            '습도(%)': 'obs_humid', \
                            '전운량(10분위)': 'obs_cloud'}, \
                            axis='columns')
    output.drop(columns=["지점명"], inplace=True)
    tag_region(output, region)
    return(output)


def energy_process(df):
    """
        energy data processing
        converts 24:00:00 into normal form
    """
    output = df.copy()
    output['date'] = output['time'].apply(lambda x: x.split()[0])
    output['time'] = output['time'].apply(lambda x: x.split()[1])
    output['time'] = output['time'].str.rjust(8, '0')  # 한자릿수 시간 앞에 0 추가 ex) 3시 -> 03시
    # 23시를 00시로 바꿔주기
    output.loc[output['time'] == '24:00:00', 'time'] = '00:00:00'
    output['time'] = output['date'] + ' ' + output['time']
    output['time'] = pd.to_datetime(output['time'])
    output.loc[output['time'].dt.hour == 0, 'time'] += datetime.timedelta(days=1)
    return(output)


if __name__ == '__main__':
    """
    arguments:
        region: array for region data ([0]: first arg, [1]: second arg)
        merged_df: array for process multiple regions
        data_path: file folde to load and save .csv files(set by arguments?)
    """
    region = []
    merged_df = []
    if len(args) == 1:
        exit()
    else:
        first_pass = 1  # ignore program's first argument
        data_path = './' + 'csv_data' + '/'
        energy = pd.read_csv(data_path + 'energy.csv')
        energy = energy_process(energy)
        for x in args:
            if first_pass == 1:
                first_pass = 0
            else:
                obs_csv_path = data_path + x + '_obs_data.csv'
                fcst_csv_path = data_path + x + '_fcst_data.csv'
                df_fcst = pd.read_csv(fcst_csv_path)
                df_fcst = fcst_process(df_fcst, x)
                df_obs = pd.read_csv(obs_csv_path)
                df_obs = obs_process(df_obs, x)
                region.append(x)
                merge_temp = pd.merge(df_obs, df_fcst, how='outer', on='time')
                merge_temp = pd.merge(merge_temp, energy, how='outer', on='time')
                merge_temp_reindex = merge_temp.sort_values(by=['time', x + '_' + 'fcst_fcst'], axis=0)
                merge_temp_reindex.drop(columns=["date", x + '_' + "forecast"], inplace=True)
                merge_temp_duplicated = merge_temp_reindex.drop_duplicates(subset='time', keep='last', inplace=False)
                merge_temp_duplicated.drop(columns=[x + '_' + "fcst_fcst"], inplace=True)
                merged_df.append(merge_temp_duplicated)
        df_combined = pd.concat(merged_df, ignore_index=True)
        df_combined.dropna(subset=['region'], how='any', axis=0, inplace=True)
        debug(df_combined, 'combined')
        df_combined.to_csv(data_path + 'final' + '.csv', index=False)
