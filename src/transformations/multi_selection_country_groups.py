import pandas as pd


def _get_country_only(row: pd.Series) -> bool:
    """
    Returns True if the group or organisation of countries contains only country, False otherwise.
    :param row: Series representing one group or organisation of countries as a row.
    :return: True or False.
    """
    group = row["group"]
    return False if group.startswith("Top GHG Emitters") or group == "Continents" else True


def process_multi_selection_country_groups(raw_multi_selection_country_groups: pd.DataFrame) -> pd.DataFrame:
    """
    Returns the final processed dataset containing the multi-selection country groups.
    :param raw_multi_selection_country_groups: DataFrame that contains the raw data of the multi-selection country groups.
    :return: DataFrame with the final data of the multi-selection country groups.
    """
    multi_selection_country_groups = raw_multi_selection_country_groups.copy()
    multi_selection_country_groups["country_only"] = multi_selection_country_groups.apply(
        _get_country_only,
        axis="columns",
    )
    multi_selection_country_groups["group"] = "Quickselect " + multi_selection_country_groups["group"]
    return multi_selection_country_groups
