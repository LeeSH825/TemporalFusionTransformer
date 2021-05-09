import pandas as pd
import numpy as np
import os


def opener(data_folder, filename):
    """Open files"""
    csv_path = os.path.join(data_folder, filename, 'csv')

    df = pd.read_csv(csv_path, index_col=0)
    return(df)


if __name__ == '__main__':

    # args parsing needed
    output_folder = './output'

    opener(output_folder, 'ulsan')
