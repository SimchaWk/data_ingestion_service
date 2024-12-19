from functools import partial
from typing import List, Optional, Any
import pandas as pd

from app.config.local_files_config.local_files import GLOBAL_TERRORISM_CSV, SECONDARY_TERROR_CSV

primary_df_columns: List[str] = [
    'iyear', 'imonth', 'iday', 'country_txt', 'region_txt',
    'provstate', 'city', 'latitude', 'longitude',
    'attacktype1_txt', 'attacktype2_txt', 'attacktype3_txt',
    'targtype1_txt', 'targsubtype1_txt', 'targtype2_txt',
    'targsubtype2_txt', 'targtype3_txt', 'targsubtype3_txt',
    'gname', 'gsubname', 'gname2', 'gsubname2', 'gname3', 'gsubname3',
    'nkill', 'nkillter', 'nwound', 'nwoundte', 'nperps', 'nperpcap'
]

secondary_df_columns: List[str] = ['Date', 'City', 'Country', 'Injuries', 'Fatalities', 'Description']


def load_csv_dataframe(
        file_path: str,
        columns: Optional[List[str]] = None,
        low_memory: bool = False
) -> pd.DataFrame:
    if columns:
        return pd.read_csv(file_path, encoding='latin1', usecols=columns, low_memory=low_memory)
    else:
        return pd.read_csv(file_path, encoding='latin1', low_memory=low_memory)


def save_dataframe_to_csv(
        df: pd.DataFrame,
        filename: str,
        encoding_method: str = 'utf-8-sig',
        **kwargs: Any
) -> None:
    try:
        default_params = {
            'index': False,
            'sep': ',',
            'decimal': '.',
            'na_rep': 'NA'
        }

        default_params.update(kwargs)

        df.to_csv(
            filename,
            encoding=encoding_method,
            **default_params
        )

        print(f'File {filename} was successfully saved with encoding {encoding_method}')

    except Exception as e:
        print(f'Error saving file: {str(e)}')


load_primary_csv: partial[pd.DataFrame] = partial(
    load_csv_dataframe,
    file_path=GLOBAL_TERRORISM_CSV,
    columns=primary_df_columns
)

load_secondary_csv: partial[pd.DataFrame] = partial(
    load_csv_dataframe,
    file_path=SECONDARY_TERROR_CSV,
    columns=secondary_df_columns
)
