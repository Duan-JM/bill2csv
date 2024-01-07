import os

import pandas as pd


file_list = os.listdir("./")
file_list = [x for x in file_list if x not in ["merge.py", ".DS_Store", ".gitignore"]]

key_set = set()
for f in file_list:
    if key_set:
        set_check_cnt = len(set(pd.read_csv(f).keys().tolist()) - key_set)
        assert set_check_cnt >= 0, f"{key_set} - {set(pd.read_csv(f).keys().tolist())}"
    else:
        key_set = set(pd.read_csv(f).keys().tolist())

pd_data_list = list()
for f in file_list:
    print(f)
    pd_data_list.append(pd.read_csv(f)[list(key_set)])

pd.concat(pd_data_list).to_csv("./明细-2023.csv", index=False)
