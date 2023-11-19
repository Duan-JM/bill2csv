import pandas as pd
import PyPDF2 as pdf2

from constants import (
    CARD_REMAIN_AMOUNT_COLUMN_NAME,
    DATE_COLUMN_NAME,
    TRADE_AMOUNT_COLUMN_NAME,
    TRADE_DEALER,
    ALIPAY_DELETE_LINE_TAG,
    ALIPAY_AVALIABLE_BILL_KEYS,
)


def delete_titles(file_path: str):
    with open(file_path, "r", encoding="cp1252") as f:
        raw_lines = f.readlines()
    raw_lines = [bytes(x.encode("cp1252")).decode("gbk") for x in raw_lines]

    target_pos = 0
    for idx, row_content in enumerate(raw_lines):
        if ALIPAY_DELETE_LINE_TAG in row_content:
            target_pos = idx
    assert target_pos != 0, "Definitely target pos should larger than 0"

    with open(file_path, "w") as f:
        f.writelines(raw_lines[target_pos + 1 :])


def read_alipay_bill_csv(file_path: str):
    delete_titles(file_path=file_path)
    alipay_bill_pd = pd.DataFrame(pd.read_csv(file_path))
    return alipay_bill_pd[ALIPAY_AVALIABLE_BILL_KEYS]


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
