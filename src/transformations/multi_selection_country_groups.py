import pandas as pd


def _get_country_only(row: pd.Series) -> bool:
    """
    Returns True if the group or organisation of countries contains only country, False otherwise.
    :param row: Series representing one group or organisation of countries as a row.
    :return: True or False.
    """
    group = row["group"]
    return False if group.startswith("Top GHG Emitters") or group == "Continents" else True


def _update_multi_selection_country_groups(raw_multi_selection_country_groups: pd.DataFrame) -> pd.DataFrame:
    multi_selection_country_groups = raw_multi_selection_country_groups.copy()

    # Removal of UK from EU
    multi_selection_country_groups = multi_selection_country_groups[
        ~(
                (multi_selection_country_groups.group == "EU28")
                & (multi_selection_country_groups.country == "United Kingdom")
        )
    ]

    # Replace EU28 with EU27
    multi_selection_country_groups.loc[multi_selection_country_groups.group == "EU28", "group"] = "EU27"

    # Add Indonesia to Top GHG Emitters
    new_row = {
        "group": "Top GHG Emitters in 2022",
        "country": "Indonesia",
    }
    new_emitter_to_add = pd.DataFrame(data=new_row, index=[0])
    multi_selection_country_groups = pd.concat([multi_selection_country_groups, new_emitter_to_add], ignore_index=True)

    # Update the year for Top GHG Emitters
    multi_selection_country_groups.loc[
        multi_selection_country_groups.group == "Top GHG Emitters in 2018",
        "group"
    ] = "Top GHG Emitters in 2022"

    return multi_selection_country_groups


def process_multi_selection_country_groups(raw_multi_selection_country_groups: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the final processed dataset containing the multi-selection country groups.
    :param raw_multi_selection_country_groups: DataFrame that contains the raw data of the multi-selection country groups.
    :return: DataFrame with the final data of the multi-selection country groups.
    """
    multi_selection_country_groups = _update_multi_selection_country_groups(raw_multi_selection_country_groups)
    multi_selection_country_groups["country_only"] = multi_selection_country_groups.apply(
        _get_country_only,
        axis="columns",
    )
    multi_selection_country_groups["group"] = "Quickselect " + multi_selection_country_groups["group"]
    return multi_selection_country_groups
