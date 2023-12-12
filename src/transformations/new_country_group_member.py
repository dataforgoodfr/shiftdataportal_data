from typing import Union, List

import pandas as pd


def add_new_members_to_group(
        df: pd.DataFrame,
        new_members: Union[str, List[str]],
        group_name: str,
        is_country_group: bool,
) -> pd.DataFrame:
    """
    TODO.
    """
    if isinstance(new_members, str):
        new_members = [new_members]

    rows = []
    for new_member in new_members:
        if is_country_group:
            row = {
                "group_type": "group",
                "group_name": group_name,
                "country": new_member,
            }
        else:
            row = {
                "group": group_name,
                "country": new_member,
            }
        rows.append(row)
    new_members_to_add = pd.DataFrame(data=rows)
    return pd.concat([df, new_members_to_add], ignore_index=True)
