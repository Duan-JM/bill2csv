from loguru import logger

from .bill2csv import create_args, fetch_bills_from_folder, main


def run_entry():
    args = create_args()
    bill_path_collection = fetch_bills_from_folder(args.bill_folder)
    if bill_path_collection["cmbc_bill_path"]:
        main(
            cmbc_bill_path=bill_path_collection["cmbc_bill_path"],
            alipay_bill_path=bill_path_collection["alipay_bill_path"],
            wechat_bill_path=bill_path_collection["wechat_bill_path"],
            ref_table_folder=args.ref_table_folder,
            output_path=args.output_path,
        )
        logger.success(f"Export results to {args.output_path}")
    else:

        raise RuntimeError(f"cmbc_bill_path not found in {args.bill_folder}.")
