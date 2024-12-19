from functools import partial
import pandas as pd
from typing import Dict


def rename_columns(df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=column_mapping)


rename_secondary_df_columns = partial(
    rename_columns,
    column_mapping={
        'City': 'city',
        'Country': 'country_txt',
        'Injuries': 'nwound',
        'Fatalities': 'nkill'
    }
)

rename_event_record_columns = partial(
    rename_columns,
    column_mapping={
        "date": "event_date",
        "country_txt": "country",
        "city": "city",
        "region_txt": "region",
        "provstate": "province_or_state",
        "latitude": "latitude",
        "longitude": "longitude",
        "nkill": "num_killed",
        "nkillter": "num_terrorist_killed",
        "nwound": "num_wounded",
        "nwoundte": "num_terrorist_wounded",
        "total_casualties": "total_casualties",
        "nperps": "num_perpetrators",
        "nperpcap": "num_perpetrators_captured",
        "attacktype1_txt": "attack_type_1",
        "attacktype2_txt": "attack_type_2",
        "attacktype3_txt": "attack_type_3",
        "targtype1_txt": "target_type_1",
        "targsubtype1_txt": "target_subtype_1",
        "targtype2_txt": "target_type_2",
        "targsubtype2_txt": "target_subtype_2",
        "targtype3_txt": "target_type_3",
        "targsubtype3_txt": "target_subtype_3",
        "gname": "terror_group_name",
        "gsubname": "terror_group_subname",
        "gname2": "secondary_terror_group_name",
        "gsubname2": "secondary_terror_group_subname",
        "gname3": "tertiary_terror_group_name",
        "gsubname3": "tertiary_terror_group_subname",
        "Description": "description",
        "data_source": "data_source"
    }
)
