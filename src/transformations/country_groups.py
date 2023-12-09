from typing import Union, List

import pandas as pd


def _add_new_members_to_group(
        country_groups: pd.DataFrame,
        new_members: Union[str, List[str]],
        group_name: str,
) -> pd.DataFrame:
    """
    TODO.
    """
    if isinstance(new_members, str):
        new_members = [new_members]

    rows = []
    for new_member in new_members:
        row = {
            "group_type": "group",
            "group_name": group_name,
            "country": new_member,
        }
        rows.append(row)
    new_members_to_add = pd.DataFrame(data=rows)
    return pd.concat([country_groups, new_members_to_add], ignore_index=True)


def update_country_groups(raw_country_groups: pd.DataFrame) -> pd.DataFrame:
    """
    TODO.
    :param raw_country_groups: DataFrame that contains the raw data of the multi-selection country groups.
    :return: DataFrame with the final data of the multi-selection country groups.
    """
    country_groups = raw_country_groups.copy()

    # Removal of UK from EU
    country_groups = country_groups[
        ~(
                (country_groups.group_name == "EU28")
                & (country_groups.country == "United Kingdom")
        )
    ]

    # Replace EU28 with EU27
    country_groups.loc[country_groups.group_name == "EU28", "group_name"] = "EU27"

    # Add Colombia, Costa Rica, Estonia, Israel, Latvia, Lithuania, Slovenia to OECD members
    new_oecd_members = [
        "Colombia",
        "Costa Rica",
        "Estonia",
        "Israel",
        "Latvia",
        "Lithuania",
        "Slovenia",
    ]
    country_groups = _add_new_members_to_group(country_groups, new_members=new_oecd_members, group_name="OECD")

    # Add Congo, Equatorial Guinea, Gabon, Libya to OPEC members
    new_opec_members = [
        "Congo",
        "Equatorial Guinea",
        "Gabon",
        "Libya",
    ]
    country_groups = _add_new_members_to_group(country_groups, new_members=new_opec_members, group_name="OPEC")

    # Removal of Ecuador and Qatar from OPEC
    country_groups = country_groups[
        ~(
                (country_groups.group_name == "OPEC")
                & (country_groups.country.isin(["Ecuador", "Qatar"]))
        )
    ]
    return country_groups
