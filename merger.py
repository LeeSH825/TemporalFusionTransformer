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
import sys
import pandas as pd
import datetime
import email.utils as eutils
import time
args = sys.argv

def debug(df, path):
    """
        print file path, dataframe, column info
    """
    print(path + ":", end='\n')
    print(df)
    print("col:\n", df.columns)
    print("\n")


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
    return(output)


def obs_process(df, region):
    """
        obs data preprocessing
        column renaming
    """
    output = df.copy()
    output = output.rename({'일시': 'time'}, axis='columns')
    output['time'] = pd.to_datetime(output['time'])
    output = output.rename({'지점명': 'region', \
                            '기온(°C)': 'obs_temp', \
                            '풍속(m/s)': 'obs_windSpd', \
                            '풍향(16방위)': 'obs_windDir', \
                            '습도(%)': 'obs_humid', \
                            '전운량(10분위)': 'obs_cloud'}, \
                            axis='columns')
    output.drop(columns=["지점"], inplace=True)
    return(output)


def energy_process(df):
    """
        energy data processing
        converts 24:00:00 into normal form
    """
    output = df.copy()
    output['date'] = output['time'].apply(lambda x: x.split()[0])
    output['time'] = output['time'].apply(lambda x: x.split()[1])
    output['time'] = output['time'].str.rjust(8, '0')
    output.loc[output['time'] == '24:00:00', 'time'] = '00:00:00'
    output['time'] = output['date'] + ' ' + output['time']
    output['time'] = pd.to_datetime(output['time'])
    output.loc[output['time'].dt.hour == 0, 'time'] += datetime.timedelta(days=1)
    return(output)


if __name__ == '__main__':
    """
    Args:
        region: array for region data ([0]: first arg, [1]: second arg)
        region_df_set: array for processed multiple regions' dataframe
        data_path: file folder to load and save .csv files(TODO: set by arguments?)
    """
    region = []
    region_df_set = []
    if len(args) == 1:
        exit()
    else:
        first_pass = 1  # ignore program's first argument
        data_path = './' + 'csv_data' + '/'
        energy = pd.read_csv(data_path + 'energy.csv')
        energy = energy_process(energy)

        # process each region's data file
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
                region.append(x + '_')

                # merging obs + fcst + energy = region_df
                temp = pd.merge(df_obs, df_fcst, how='outer', on='time')
                temp_reindex = temp.sort_values(by=['time', 'fcst_fcst'], axis=0)
                temp_reindex_dropped = temp_reindex.drop(columns=["forecast"], inplace=False)
                temp_duplicated = temp_reindex_dropped.drop_duplicates(subset='time', keep='last', inplace=False)
                temp_duplicated_dropped = temp_duplicated.drop(columns=["fcst_fcst"], inplace=False)
                temp_duplicated_dropped.dropna(subset=['region'], how='any', axis=0, inplace=True)
                for col_mem in energy.columns:
                    if x in col_mem:
                        temp1 = temp_duplicated_dropped.copy()
                        temp1.reset_index(inplace=True)
                        temp1['energy'] = energy[col_mem]
                        # debug_e = pd.DataFrame(energy[col_mem], columns=[col_mem])
                        # debug(debug_e, col_mem + 'one')
                        # debug(temp1, col_mem + 'temp')
                        temp1.insert(1, 'ID', col_mem)
                        region_df_set.append(temp1)
                    else:
                        continue
                print('Processing Done. (region: ' + x + ')')

        # combining region dataframes
        df_combined = pd.concat(region_df_set, ignore_index=True)
        # df_combined.dropna(subset=['region'], how='any', axis=0, inplace=True)

        # debug
        # debug(df_combined, 'combined')

        # save to file
        """
        region_str = ''
        for x in region:
            region_str = region_str + x
        df_combined.to_csv(data_path + region_str + 'final' + '.csv', index=False)
        print('saved to: ', data_path + region_str + 'final' + '.csv\n')
        """
        print('Combining Done.')
        df_combined.to_csv(data_path + 'final' + '.csv', index=False)
        print('Saved to: ', data_path + 'final' + '.csv')
