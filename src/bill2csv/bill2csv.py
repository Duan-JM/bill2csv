import argparse
from functools import partial
import os
from typing import Dict

import pandas as pd

from constants import (
    FIRST_LABEL_NAME,
    REFERENCE_TABLE_FOLDER_PATH,
    SECOND_LABEL_NAME,
    TRADE_DEALER,
)
from utils import get_current_date, prefill_label
from loaders import read_cmbc_bill_pdf, read_alipay_bill_csv


def prefilled_map() -> Dict:
    """Formulte prefilled_map"""
    file_paths = os.listdir(REFERENCE_TABLE_FOLDER_PATH)
    file_paths = [x for x in file_paths if x not in [".DS_Store"]]
    data_list = [
        pd.DataFrame(pd.read_csv(f"{REFERENCE_TABLE_FOLDER_PATH}/{x}"))
        for x in file_paths
    ]
    total_data = pd.concat(data_list, ignore_index=True)
    output_dict = dict()
    for _s in total_data.saler.unique():
        select_table = pd.DataFrame(total_data[total_data.saler == _s])
        if len(pd.unique(select_table[SECOND_LABEL_NAME])) == 1:
            first_prefilled_label = pd.unique(select_table[FIRST_LABEL_NAME])
            second_prefilled_label = pd.unique(select_table[SECOND_LABEL_NAME])
            output_dict[_s] = [
                str(first_prefilled_label[0] if first_prefilled_label else ""),
                str(second_prefilled_label[0] if second_prefilled_label else ""),
            ]
    return output_dict


def comment_with_alipay_bill(bill_date, bill_amount, alipay_bill_pd):
    matched_pd = alipay_bill_pd[
        alipay_bill_pd["交易时间"].apply(lambda x: x[:10] == bill_date)
    ]
    matched_pd = matched_pd[matched_pd["金额"] == abs(bill_amount)]
    if len(matched_pd) == 1:
        return matched_pd.iloc[0]["交易对方"] + matched_pd.iloc[0]["商品说明"]


def add_comment(bill_pd, alipay_bill_pd) -> pd.DataFrame:
    alipay_comment_fill_func = partial(
        comment_with_alipay_bill, alipay_bill_pd=alipay_bill_pd
    )
    bill_pd["comment"] = bill_pd.apply(
        lambda x: alipay_comment_fill_func(
            bill_date=x["date_time"], bill_amount=x["amount"]
        ),
        axis=1,
    )
    return bill_pd


def prefill_bill(bill_pd: pd.DataFrame, prefilled_map: Dict) -> pd.DataFrame:
    """Use prefilled_map to prefill bill_pd"""
    bill_pd[FIRST_LABEL_NAME] = pd.DataFrame(
        bill_pd[TRADE_DEALER].apply(
            partial(prefill_label, prefilled_map=prefilled_map, label_type="first")
        )
    )
    bill_pd[SECOND_LABEL_NAME] = pd.DataFrame(
        bill_pd[TRADE_DEALER].apply(
            partial(prefill_label, prefilled_map=prefilled_map, label_type="second")
        )
    )
    return bill_pd


def read_bill_from_pdf(bill_file_path, bank_type: str = "CMBC") -> pd.DataFrame:
    if bank_type == "CMBC":
        bill_pd = read_cmbc_bill_pdf(bill_file_path)
        return bill_pd
    else:
        raise NotImplementedError(f"Not found {bank_type} implement.")


def main(
    file_path: str,
    alipay_bill_path: str,
    output_path=f"./outputs/output_{get_current_date()}.csv",
):
    prelabeled_map = prefilled_map()
    bill_pd = read_bill_from_pdf(file_path)
    alipay_bill_pd = read_alipay_bill_csv(alipay_bill_path)
    prefilled_pd = prefill_bill(bill_pd=bill_pd, prefilled_map=prelabeled_map)
    prefilled_pd = add_comment(bill_pd=prefilled_pd, alipay_bill_pd=alipay_bill_pd)
    prefilled_pd.to_csv(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", help="file_path")
    parser.add_argument("--alipay_bill_path", help="alipay_bill_path")
    args = parser.parse_args()
    file_path = args.file_path
    main(file_path=args.file_path, alipay_bill_path=args.alipay_bill_path)
