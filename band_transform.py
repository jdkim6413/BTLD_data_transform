import pandas as pd
import time
from multiprocessing import Pool
import os


def read_rows(inp_tpl):
    (i, filename) = inp_tpl
    df = pd.read_csv(filename, header=None, nrows=48, skiprows=i, usecols=[0, 2, 3, 4, 5])

    if len(df.iloc[:, 0].value_counts()) != 1:
        print(i)

    cur_date = df.iloc[0, 0]
    band_a = df.iloc[:, 1].tolist()
    band_b = df.iloc[:, 2].tolist()
    band_c = df.iloc[:, 3].tolist()
    band_d = df.iloc[:, 4].tolist()
    new_data = band_a + band_b + band_c + band_d
    new_data.insert(0, cur_date)

    return new_data


if __name__ == "__main__":
    start = time.time()  # 시작 시간 저장

    dir_name = "D:\\03_Python_project\\rectuson_btld_converting\\band_split"
    os.chdir(dir_name)
    file_list = os.listdir(dir_name)

    new_file_list = []

    for name in file_list:
        filename = name

        # number_lines = 1602432
        number_lines = sum(1 for row in (open(filename, encoding='UTF8')))
        rowsize = 48

        temp_df = pd.read_csv(filename, header=None, nrows=rowsize, usecols=[1])
        header_list = temp_df.iloc[:, 0].tolist()

        header_list_a = [None] * rowsize
        header_list_b = [None] * rowsize
        header_list_c = [None] * rowsize
        header_list_d = [None] * rowsize

        for idx, item in enumerate(header_list):
            name_a = item + "_A"
            name_b = item + "_B"
            name_c = item + "_C"
            name_d = item + "_D"

            header_list_a[idx] = name_a
            header_list_b[idx] = name_b
            header_list_c[idx] = name_c
            header_list_d[idx] = name_d

        band_header = header_list_a + header_list_b + header_list_c + header_list_d
        band_header.insert(0, 'Time')

        num_cores = 8
        pool = Pool(num_cores)

        lst1 = list(range(0, number_lines, rowsize))
        lst2 = [filename] * len(lst1)
        lst_tuple = list(zip(lst1, lst2))

        result = pool.map(read_rows, lst_tuple)
        pool.close()
        pool.join()

        new_df = pd.DataFrame(result, columns=band_header)

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
