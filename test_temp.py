import pandas as pd
import time
from multiprocessing import Pool
import os


dir_name = "D:\\03_Python_project\\rectuson_btld_converting\\temp_files"
os.chdir(dir_name)
file_list = os.listdir(dir_name)

merged_df = pd.read_csv(file_list[0])

for i, name in enumerate(file_list):
    if i != 0:
        temp_df = pd.read_csv(name, encoding='cp949')
        temp_df = temp_df.iloc[:, 2:]
        merged_df = pd.concat([merged_df, temp_df], axis=1)

merged_df.to_csv('new_merged.csv', index=False)
