import argparse
from functools import partial
import os
from typing import Dict, Optional

import pandas as pd

from constants import (
    FIRST_LABEL_NAME,
    REFERENCE_TABLE_FOLDER_PATH,
    SECOND_LABEL_NAME,
    TRADE_DEALER,
)
from loaders import read_alipay_bill_csv, read_cmbc_bill_pdf, read_wechat_bill_csv
from utils import get_current_date, prefill_label


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
        return matched_pd.iloc[0]["交易对方"] + " - " + matched_pd.iloc[0]["商品说明"]


def comment_with_wechat_bill(bill_date, bill_amount, wechat_bill_pd):
    matched_pd = wechat_bill_pd[
        wechat_bill_pd["交易时间"].apply(lambda x: x[:10] == bill_date)
    ]
    matched_pd["金额"] = matched_pd["金额(元)"].apply(lambda x: float(x[1:]))
    matched_pd = matched_pd[matched_pd["金额"] == abs(bill_amount)]
    if len(matched_pd) == 1:
        return matched_pd.iloc[0]["交易对方"] + " - " + matched_pd.iloc[0]["商品"]


def add_comment(
    bill_pd: pd.DataFrame,
    alipay_bill_pd: Optional[pd.DataFrame],
    wechat_bill_pd: Optional[pd.DataFrame],
) -> pd.DataFrame:
    if alipay_bill_pd is not None:
        alipay_comment_fill_func = partial(
            comment_with_alipay_bill, alipay_bill_pd=alipay_bill_pd
        )
        bill_pd["comment"] = bill_pd.apply(
            lambda x: alipay_comment_fill_func(
                bill_date=x["date_time"], bill_amount=x["amount"]
            ),
            axis=1,
        )
    if wechat_bill_pd is not None:
        wechat_comment_fill_func = partial(
            comment_with_wechat_bill, wechat_bill_pd=wechat_bill_pd
        )
        bill_pd["wechat_comment"] = bill_pd.apply(
            lambda x: wechat_comment_fill_func(
                bill_date=x["date_time"], bill_amount=x["amount"]
            ),
            axis=1,
        )
        # merge comment
        bill_pd["comment"] = bill_pd.apply(
            lambda x: x["wechat_comment"] if x["wechat_comment"] else x["comment"],
            axis=1,
        )
        bill_pd = bill_pd.drop("wechat_comment", axis=1)

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


def fetch_bills_from_folder(folder_path: str) -> Dict[str, Optional[str]]:
    file_names = os.listdir(folder_path)
    cmbc_bill_path = None
    alipay_bill_path = None
    wechat_bill_path = None
    for f_name in file_names:
        if "alipay" in f_name:
            alipay_bill_path = f"{folder_path}/{f_name}"
        elif "微信" in f_name:
            wechat_bill_path = f"{folder_path}/{f_name}"
        elif "招商银行":
            cmbc_bill_path = f"{folder_path}/{f_name}"
    return {
        "alipay_bill_path": alipay_bill_path,
        "wechat_bill_path": wechat_bill_path,
        "cmbc_bill_path": cmbc_bill_path,
    }


def main(
    cmbc_bill_path: str,
    alipay_bill_path: Optional[str],
    wechat_bill_path: Optional[str],
    output_path=f"./outputs/output_{get_current_date()}.csv",
):
    prelabeled_map = prefilled_map()
    bill_pd = read_bill_from_pdf(cmbc_bill_path)
    prefilled_pd = prefill_bill(bill_pd=bill_pd, prefilled_map=prelabeled_map)

    alipay_bill_pd = None
    wechat_bill_pd = None
    if alipay_bill_path:
        alipay_bill_pd = read_alipay_bill_csv(alipay_bill_path)

    if wechat_bill_path:
        wechat_bill_pd = read_wechat_bill_csv(wechat_bill_path)

    prefilled_pd = add_comment(
        bill_pd=prefilled_pd,
        alipay_bill_pd=alipay_bill_pd,
        wechat_bill_pd=wechat_bill_pd,
    )

    prefilled_pd.to_csv(output_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bill_folder", help="file_path")
    args = parser.parse_args()
    bill_path_collection = fetch_bills_from_folder(args.bill_folder)
    if bill_path_collection["cmbc_bill_path"]:
        main(
            cmbc_bill_path=bill_path_collection["cmbc_bill_path"],
            alipay_bill_path=bill_path_collection["alipay_bill_path"],
            wechat_bill_path=bill_path_collection["wechat_bill_path"],
        )
    else:
        raise RuntimeError(f"cmbc_bill_path not found in {args.bill_folder}.")
