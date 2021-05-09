import sys
import pandas as pd
import datetime
import email.utils as eutils
import time

args = sys.argv


def plus_forecast(df):
    """
        set actual forecast time
    """
    append = pd.to_datetime(df['Forecast time']) + pd.to_timedelta(df['forecast'], 'h')
    output = df.copy()
    # DataFrame.insert(location, column_name, value, allow_duplicates=False)
    output.insert(2, 'NEW', append)
    # output['Forecast time'] = append
    return(output)

"""
def reformat_datetime(df):
     
        Re-format datetime "xxxx-xx-xx 24:00:00" to "xxxx-xx-xx 23:00:00"
     
    output = df.copy()
    for x in output['time']:
        print(x)
        ntuple = eutils.parsedate(x)
        print(ntuple)
        timestamp = time.mktime(ntuple)
        date = datetime.datetime.fromtimestamp(timestamp)
        x = date
    return output
"""

if __name__ == '__main__':
    """
    arguments:
        args[1]: Region data to merge
        args[2]: Energy data to merge
    """
    if len(args) == 1:
        exit()
    else:
        obs_csv_path = args[1] + '_obs_data.csv'
        fcst_csv_path = args[1] + '_fcst_data.csv'
        df_fcst = pd.read_csv(fcst_csv_path)
        df_fcst = plus_forecast(df_fcst)
        df_fcst = df_fcst.rename({'NEW': 'time'}, axis='columns')

        df_obs = pd.read_csv(obs_csv_path)
        df_obs = df_obs.rename({'일시': 'time'}, axis='columns')
        df_obs['time'] = pd.to_datetime(df_obs['time'])

        df_merge = pd.merge(df_fcst, df_obs, how='outer', on='time')
        energy = pd.read_csv('energy.csv')
        #########
        # df_energy['minus'] = 1
        # df_energy = df_energy.assign(minus=1)
        # minus = pd.to_datetime(pd.to_datetime(df_energy['time']) - pd.to_timedelta(df_energy['minus'], 'h'))
        # df_energy['time'] = minus
        ########

        ########
        # df_energy = reformat_datetime(df_energy)
        # df_energy['time'] = pd.to_datetime(df_energy['time'])
        ########

        ###########: xxxx-xx-xx 24:00:00 == invalid form
        # df_energy['time'] = pd.to_datetime(df_energy['time'] + pd.to_timedelta(days=1))

        energy['date'] = energy['time'].apply(lambda x: x.split()[0])
        energy['time'] = energy['time'].apply(lambda x: x.split()[1])
        energy['time'] = energy['time'].str.rjust(8,'0') # 한자릿수 시간 앞에 0 추가 ex) 3시 -> 03시

        # 24시를 00시로 바꿔주기
        energy.loc[energy['time']=='24:00:00','time'] = '00:00:00'
        energy['time'] = energy['date'] + ' ' + energy['time']
        energy['time'] = pd.to_datetime(energy['time'])
        energy.loc[energy['time'].dt.hour==0,'time'] += datetime.timedelta(days=1)

        df_merge = pd.merge(df_merge, energy, how='outer', on='time')


        df_reindex = df_merge.sort_values(by=['time'], axis=0)

        # debug
        print(fcst_csv_path + ":\n")
        print(df_fcst)
        print("col:\n", df_fcst.columns)
        print("\n\n")
        print(obs_csv_path, ":\n")
        print(df_obs)
        print("col:\n", df_obs.columns)
        print("\n\n")
        print("energy:\n")
        print(energy)
        print("col:\n", energy.columns)
        print("\n\n")
        print("merge:\n")
        print(df_merge)
        print("\n\n")
        print("reindex:\n")
        print(df_reindex)

        df_reindex.to_csv('total' + '.csv', index=False)
