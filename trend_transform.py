import pandas as pd
import time
from multiprocessing import Pool
import os


def read_rows(inp_tpl):
    (i, filename) = inp_tpl
    df = pd.read_csv(filename, header=None, nrows=52, skiprows=i, usecols=[0, 2], dtype={2: "float16"})

    if len(df.iloc[:, 0].value_counts()) != 1:
        print(i)

    cur_date = df.iloc[0, 0]
    new_data = df.iloc[:, 1].tolist()
    new_data.insert(0, cur_date)

    return new_data


if __name__ == "__main__":
    start = time.time()  # 시작 시간 저장

    dir_name = "D:\\03_Python_project\\rectuson_btld_converting\\trend_split"
    os.chdir(dir_name)
    file_list = os.listdir(dir_name)

    new_file_list = []

    for name in file_list:
        filename = name

        # number_lines = 1602432
        number_lines = sum(1 for row in (open(filename, encoding='UTF8')))
        rowsize = 52

        temp_df = pd.read_csv(filename, header=None, nrows=rowsize, usecols=[1])
        header_list = temp_df.iloc[:, 0].tolist()
        header_list.insert(0, 'Time')

        num_cores = 8
        pool = Pool(num_cores)

        lst1 = list(range(0, number_lines, rowsize))
        lst2 = [filename] * len(lst1)
        lst_tuple = list(zip(lst1, lst2))

        result = pool.map(read_rows, lst_tuple)
        pool.close()
        pool.join()

        new_df = pd.DataFrame(result, columns=header_list)

        new_name = "new_" + name
        new_file_list.append(new_name)

        new_df.to_csv(new_name, index=False)

    new_file_list.sort()

    with open('new_merged.csv', 'w') as outfile:
        for i, fname in enumerate(new_file_list):
            with open(fname) as infile:
                for j, line in enumerate(infile):
                    if (i != 0) and (j == 0):
                        pass
                    else:
                        outfile.write(line)

    my_df = pd.read_csv('new_merged.csv')
    my_df['Time'] = pd.to_datetime(my_df.Time)
    my_df.sort_values(by='Time', axis=0, inplace=True)

    my_df.to_csv('new_merged_sorted.csv', index=False)

    print("time :", time.time() - start)  # 현재시각 - 시작시간 = 실행 시간
