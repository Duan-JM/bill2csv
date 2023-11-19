from typing import Dict, Literal
import datetime


def get_current_date():
    now = datetime.datetime.now()
    return f"{now.year}-{now.month}-{now.day}"


def prefill_label(
    saler: str, prefilled_map: Dict, label_type: Literal["first", "second"]
) -> str:
    if prefilled_map.get(saler, None):
        prefilled_labels = prefilled_map[saler]
        if label_type == "first":
            return prefilled_labels[0] if prefilled_labels[0] else ""
        elif label_type == "second":
            return prefilled_labels[1] if prefilled_labels[1] else ""
    return ""
