import PyPDF2 as pdf2
from loguru import logger
import pandas as pd

from constants import (
    ALIPAY_AVALIABLE_BILL_KEYS,
    ALIPAY_DELETE_LINE_TAG,
    CARD_REMAIN_AMOUNT_COLUMN_NAME,
    DATE_COLUMN_NAME,
    TRADE_AMOUNT_COLUMN_NAME,
    TRADE_DEALER,
    WECHAT_AVALIABLE_BILL_KEYS,
    WECHAT_DELETE_LINE_TAG,
)


def delete_titles(file_path: str, delete_line_tag: str):
    try:
        # alipay is encoding with cp1252, however reformat and wechat is
        # encoding normally
        with open(file_path, "r", encoding="cp1252") as f:
            raw_lines = f.readlines()
        raw_lines = [bytes(x.encode("cp1252")).decode("gbk") for x in raw_lines]
    except:
        with open(file_path, "r") as f:
            raw_lines = f.readlines()
        raw_lines = [x for x in raw_lines]

    target_pos = 0
    for idx, row_content in enumerate(raw_lines):
        if delete_line_tag in row_content:
            target_pos = idx

    if target_pos == 0:
        logger.warning(
            f"Definitely target pos not larger than 0, nothing deleted from {file_path}"
        )
    else:
        with open(file_path, "w") as f:
            f.writelines(raw_lines[target_pos + 1 :])


def read_alipay_bill_csv(file_path: str):
    delete_titles(file_path=file_path, delete_line_tag=ALIPAY_DELETE_LINE_TAG)
    alipay_bill_pd = pd.DataFrame(pd.read_csv(file_path))
    return alipay_bill_pd[ALIPAY_AVALIABLE_BILL_KEYS]


def read_wechat_bill_csv(file_path: str):
    delete_titles(file_path=file_path, delete_line_tag=WECHAT_DELETE_LINE_TAG)
    wechat_bill_pd = pd.DataFrame(pd.read_csv(file_path))
    return wechat_bill_pd[WECHAT_AVALIABLE_BILL_KEYS]


def read_cmbc_bill_pdf(bill_file_path) -> pd.DataFrame:
    date_time_list = list()
    amount_list = list()
    remain_amount_list = list()
    saler_list = list()
    with open(bill_file_path, "rb") as f:
        pdf = pdf2.PdfReader(f)
        pdf_num_cnt = len(pdf.pages)
        for cnt in range(pdf_num_cnt):
            page_one = pdf.pages[cnt]
            text_line = page_one.extract_text()
            text_line = text_line.split("\n")
            for single_line in text_line:
                if "CNY" in single_line:
                    extract_info = single_line.split(" ")
                    date_time_list.append(extract_info[0])
                    amount_list.append(extract_info[2])
                    remain_amount_list.append(extract_info[3])
                    saler_list.append("".join(extract_info[4:]))

    bill_pd = pd.DataFrame.from_dict(
        {
            DATE_COLUMN_NAME: date_time_list,
            TRADE_AMOUNT_COLUMN_NAME: amount_list,
            CARD_REMAIN_AMOUNT_COLUMN_NAME: remain_amount_list,
            TRADE_DEALER: saler_list,
        }
    )
    bill_pd[TRADE_AMOUNT_COLUMN_NAME] = bill_pd[TRADE_AMOUNT_COLUMN_NAME].apply(
        lambda x: float(x.replace(",", ""))
    )
    return bill_pd
