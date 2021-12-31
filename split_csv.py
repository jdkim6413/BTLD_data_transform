import pandas as pd
import os

f_name = 'YH06_BandData_Minute.csv'

# number_lines = 48000

number_lines = sum(1 for row in (open(f_name, encoding='UTF8')))

rowsize = 801216

for i in range(0, number_lines, rowsize):
    df = pd.read_csv(f_name, header=None, nrows=rowsize, skiprows=i, usecols=[0, 1, 5, 9, 13, 17])  # Band
    # df = pd.read_csv(f_name, header=None, nrows=rowsize, skiprows=i, usecols=[0, 1, 5])  # Trend
    out_csv = 'band_split_' + str(i) + '.csv'
    df.to_csv(out_csv, index=False, header=False, mode='a', chunksize=rowsize)
