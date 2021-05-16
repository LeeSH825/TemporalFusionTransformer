# coding=utf-8
# Copyright 2021 The Google Research Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Lint as: python3
"""Script to download  data for a default experiment.

Only downloads data if the csv files are present, unless the "force_download"
argument is supplied. For new datasets, the download_and_unzip(.) can be reused
to pull csv files from an online repository, but may require subsequent
dataset-specific processing.

Usage:
  python3 script_download_data {EXPT_NAME} {OUTPUT_FOLDER} {FORCE_DOWNLOAD}

Command line args:
  EXPT_NAME: Name of experiment to download data for  {e.g. volatility}
  OUTPUT_FOLDER: Path to folder in which
  FORCE_DOWNLOAD: Whether to force data download from scratch.



"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import gc
import glob
import os
import shutil
import sys

from expt_settings.configs import ExperimentConfig
import numpy as np
import pandas as pd
import datetime
import pyunpack
import wget


# Default Params
data_path = './' + 'csv_data' + '/'
region_default = {
    'ulsan',
    'dangjin'
}
train_rate = 60
valid_rate = 20
test_rate = 20


# General functions for data downloading & aggregation.
def recreate_folder(path):
  """Deletes and recreates folder."""

  shutil.rmtree(path)
  os.makedirs(path)


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
    output = output[[
        'Forecast time', \
        'forecast', \
        'Temperature', \
        'WindSpeed', \
        'WindDirection', \
        'Humidity', \
        'Cloud']]
    output = plus_forecast(output)
    output = output.rename({'NEW': 'time'}, axis='columns')
    output = output.rename({
        'Forecast time': 'fcst_fcst', \
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


def merger():
    """
    Args:
        region: array for region info
        region_df_set: array for multiple region dataframe
    """
    region = []
    region_df_set = []
    energy = pd.read_csv(data_path + 'energy.csv')
    energy = energy_process(energy)

    # process each region's data file
    for x in region_default:
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

        # Add each energy value
        for col_mem in energy.columns:
            if x in col_mem:
                temp1 = temp_duplicated_dropped.copy()
                temp1.reset_index(inplace=True)
                temp1['energy'] = energy[col_mem]
                temp1.insert(1, 'ID', col_mem)
                region_df_set.append(temp1)
            else:
                continue
        print('Processing done. (region: ' + x + ')')

    # combining region dataframes
    df_combined = pd.concat(region_df_set, ignore_index=True)
    ############## TODO: fill NaN within func
    df_output = df_combined.fillna(0)
    print('Merging done.')
    return(df_output)


def boundary_slice(df, col_name, boundary):
    """
        Args:
            df: dataframe to slice
            col_name: dataframe's column to slice
            boundary: boundary between [training + validation | test]
        Return:
            output = [training_set + validation_set, test_set](axis=column)
    """
    temp1 = df['obs' + '_' + col_name].iloc[0:boundary].values # returns ndarray
    temp1 = pd.DataFrame(temp1, columns=[col_name])
    temp2 = df['fcst' + '_' + col_name].iloc[boundary:len(df)].values
    temp2 = pd.DataFrame(temp2, columns=[col_name])
    output = pd.concat([temp1, temp2], ignore_index=True)
    return(output)


def boundary_selector(unselected_df):
    """
        DataFrame Analysis

        # of Regions: 4
        # of Dates: 1068
        Time format : 00 ~ 23
        # of total Datasets: 1068 * 24 * 4 = 102528
        actual # of total Datasets = 102510
    """
    total_num = len(unselected_df)
    train_boundary_rate = (train_rate) / (train_rate + valid_rate + test_rate)
    valid_boundary_rate = (train_rate + valid_rate) / (train_rate + valid_rate + test_rate)
    processed_df_set = []
    print('valid region: ( ', end='')
    for name, group in unselected_df.groupby('ID'):
        print(name, end='')
        print('(#=' + str(len(group)) + ')', end=' ')
    print(')')
    print('Select with rate ( test : validation : test ) = (', train_rate, ':', valid_rate, ':', test_rate, ')')
    for name, group in unselected_df.groupby('ID'):
        # common data type classifier
        df = pd.DataFrame()
        group.reset_index(inplace=True)
        group_len = len(group)
        df['id'] = group['ID']  # ID: plant
        df['region'] = group['region']
        df['date'] = pd.to_datetime(group['time'])
        df['month'] = df['date'].dt.month
        df['week_of_year'] = df['date'].dt.isocalendar().week
        df['day_of_month'] = df['date'].dt.day
        df['days_from_start'] = (df['date'] - pd.to_datetime('2018-03-01 00:00')).dt.days

        # boundary data type classifier
        train_boundary = round(train_boundary_rate * group_len)
        valid_boundary = round(valid_boundary_rate * group_len)
        print(name + ':')
        print('train boundary:', group.loc[[train_boundary], :]['time'].values, '(#=', str(train_boundary) + ')')
        print('valid boundary:', group.loc[[valid_boundary], :]['time'].values, '(#=', str(valid_boundary - train_boundary) + ')')
        print('test boundary:', group.loc[[group_len - 1], :]['time'].values, '(#=', str(group_len - valid_boundary) + ')')
        df['temperature'] = boundary_slice(group, 'temp', valid_boundary)
        df['wind_speed'] = boundary_slice(group, 'windSpd', valid_boundary)
        df['wind_direction'] = boundary_slice(group, 'windDir', valid_boundary)
        df['humidity'] = boundary_slice(group, 'humid', valid_boundary)
        df['cloud'] = boundary_slice(group, 'cloud', valid_boundary)

        # copy energy values
        df['energy'] = group['energy']

        processed_df_set.append(df)
    processed_df = pd.concat(processed_df_set, ignore_index=True)
    print('Selection done.')
    return(processed_df)


# Dataset specific download routines.
def process_dacon(config):
    """
        Processing dacon data files
    """
    data_folder = config.data_folder

    print("Merging into one...")
    df_merged = merger()
    print("Selecting boundaries")
    df_selected = boundary_selector(df_merged)
    print("Adding more informations...")

    # make new DataFrame for model input
    df = pd.DataFrame()
    df['ID'] = df_selected['id']
    df['date'] = pd.to_datetime(df_selected['date'])
    df['month'] = df['date'].dt.month
    df['week_of_year'] = df['date'].dt.isocalendar().week
    df['day_of_month'] = df['date'].dt.day
    df['temperature'] = df_selected['temperature']
    df['wind_speed'] = df_selected['wind_speed']
    df['wind_direction'] = df_selected['wind_direction']
    df['humidity'] = df_selected['humidity']
    df['cloud'] = df_selected['cloud']
    df['energy'] = df_selected['energy']
    df['Region'] = df_selected['region']
    df['days_from_start'] = df_selected['days_from_start']

    output_file = config.data_csv_path
    print('Completed formatting, saving to {}'.format(output_file))
    df.to_csv(output_file)

    print('Done.')


# Core routine.
def main(expt_name, force_download, output_folder):
  """Runs main download routine.

  Args:
    expt_name: Name of experiment
    force_download: Whether to force data download from scratch
    output_folder: Folder path for storing data
  """

  print('#### Running download script ####')

  expt_config = ExperimentConfig(expt_name, output_folder)

  if os.path.exists(expt_config.data_csv_path) and not force_download:
    print('Data has been processed for {}. Skipping download...'.format(
        expt_name))
    sys.exit(0)
  else:
    print('Resetting data folder...')
    recreate_folder(expt_config.data_folder)

  # Default download functions
  download_functions = {
      'dacon': process_dacon,
      'ulsan': process_dacon
  }

  if expt_name not in download_functions:
    raise ValueError('Unrecongised experiment! name={}'.format(expt_name))

  download_function = download_functions[expt_name]

  # Run data download
  print('Getting {} data...'.format(expt_name))
  download_function(expt_config)

  print('Download completed.')


if __name__ == '__main__':

  def get_args():
    """Returns settings from command line."""

    experiment_names = ExperimentConfig.default_experiments

    parser = argparse.ArgumentParser(description='Data download configs')
    parser.add_argument(
        'expt_name',
        metavar='e',
        type=str,
        nargs='?',
        choices=experiment_names,
        help='Experiment Name. Default={}'.format(','.join(experiment_names)))
    parser.add_argument(
        'output_folder',
        metavar='f',
        type=str,
        nargs='?',
        default='.',
        help='Path to folder for data download')
    parser.add_argument(
        'force_download',
        metavar='r',
        type=str,
        nargs='?',
        choices=['yes', 'no'],
        default='no',
        help='Whether to re-run data download')

    args = parser.parse_known_args()[0]

    root_folder = None if args.output_folder == '.' else args.output_folder

    return args.expt_name, args.force_download == 'yes', root_folder

  name, force, folder = get_args()
  main(expt_name=name, force_download=force, output_folder=folder)
