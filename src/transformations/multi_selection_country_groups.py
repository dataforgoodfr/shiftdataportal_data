import pandas as pd


def _get_country_only(row: pd.Series) -> bool:
    group = row["group"]
    return False if group.startswith("Top GHG Emitters") or group == "Continents" else True


def process_multi_selection_country_groups(raw_multi_selection_country_groups: pd.DataFrame) -> pd.DataFrame:
    multi_selection_country_groups = raw_multi_selection_country_groups.copy()
    multi_selection_country_groups["country_only"] = multi_selection_country_groups.apply(
        _get_country_only,
        axis="columns",
    )
    multi_selection_country_groups["group"] = "Quickselect " + multi_selection_country_groups["group"]
    return multi_selection_country_groups
