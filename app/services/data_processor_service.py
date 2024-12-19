from typing import List, Tuple, Callable, Any, Optional, Union, Dict, Generator
import numpy as np
import pandas as pd
from functools import reduce

from app.models.pydantic_model.event_record import EventRecord
from app.repositories.local_files_repository import load_primary_csv, load_secondary_csv, save_dataframe_to_csv
from app.services.rename_columns_service import rename_secondary_df_columns, rename_event_record_columns

ESSENTIAL_COLUMNS: List[str] = [
    'date',
    'country_txt', 'city', 'region_txt', 'provstate',
    'latitude', 'longitude',
    'nkill', 'nkillter', 'nwound', 'nwoundte', 'total_casualties', 'nperps', 'nperpcap',
    'attacktype1_txt', 'attacktype2_txt', 'attacktype3_txt',
    'targtype1_txt', 'targsubtype1_txt', 'targtype2_txt',
    'targsubtype2_txt', 'targtype3_txt', 'targsubtype3_txt',
    'gname', 'gsubname', 'gname2', 'gsubname2', 'gname3', 'gsubname3',
    'Description', 'data_source'
]


def convert_dates_primary_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.insert(0, 'date', pd.NaT)

    valid_dates = df['iyear'].notna()
    valid_dates &= df['imonth'].notna()
    valid_dates &= df['iday'].notna()
    valid_dates &= df['imonth'] != 0
    valid_dates &= df['iday'] != 0

    df.loc[valid_dates, 'date'] = pd.to_datetime(dict(
        year=df.loc[valid_dates, 'iyear'],
        month=df.loc[valid_dates, 'imonth'],
        day=df.loc[valid_dates, 'iday']
    ))

    return df


def convert_dates_secondary_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.insert(0, 'date', pd.to_datetime(df['Date'], format='%d-%b-%y'))
    return df


def prepare_primary_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['total_casualties'] = (df['nkill'].fillna(0) + df['nwound'].fillna(0)).astype(int)
    return df


def prepare_secondary_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = rename_secondary_df_columns(df)
    df['total_casualties'] = (df['nkill'].fillna(0) + df['nwound'].fillna(0)).astype(int)
    return df


def perform_initial_merge(primary_df: pd.DataFrame, secondary_df: pd.DataFrame) -> pd.DataFrame:
    MERGE_KEYS: List[str] = ['date', 'country_txt', 'city']

    merged = pd.merge(
        primary_df,
        secondary_df,
        on=MERGE_KEYS,
        how='outer',
        indicator=True,
        suffixes=('_primary', '_secondary')
    )

    merged['data_source'] = merged['_merge'].map({
        'left_only': 'only_in_primary',
        'right_only': 'only_in_secondary',
        'both': 'matched'
    })

    return merged


def process_matched_records(df: pd.DataFrame) -> pd.DataFrame:
    matched_mask = df['_merge'] == 'both'

    if not matched_mask.any():
        return df

    matched_rows = df[matched_mask].groupby(
        ['date', 'country_txt', 'city'],
        as_index=False
    ).first()

    for col in ['nkill', 'nwound', 'total_casualties', 'Description']:
        col_primary = f"{col}_primary"
        col_secondary = f"{col}_secondary"
        if col_primary in matched_rows.columns and col_secondary in matched_rows.columns:
            matched_rows[col] = matched_rows[col_primary].fillna(matched_rows[col_secondary])

    duplicate_cols: List[str] = [
        col for col in matched_rows.columns
        if col.endswith(('_primary', '_secondary'))
    ]
    matched_rows = matched_rows.drop(columns=duplicate_cols, errors='ignore')

    unmatched_rows = df[~matched_mask]
    return pd.concat([matched_rows, unmatched_rows], ignore_index=True)


def cleanup_final_dataframe(df: pd.DataFrame, essential_columns: Optional[List[str]] = None) -> pd.DataFrame:
    if essential_columns is None:
        essential_columns = ESSENTIAL_COLUMNS
    df = df.drop(columns=['_merge'], errors='ignore')
    final_columns = [col for col in essential_columns if col in df.columns]
    return df[final_columns].sort_values('date', na_position='last')


def merge_and_enrich_dataframes(primary_df: pd.DataFrame, secondary_df: pd.DataFrame) -> pd.DataFrame:
    PipelineStep = Callable[[Union[Tuple[pd.DataFrame, pd.DataFrame], pd.DataFrame]],
    Union[Tuple[pd.DataFrame, pd.DataFrame], pd.DataFrame]]

    pipeline: List[PipelineStep] = [
        lambda dfs: (
            prepare_primary_dataframe(dfs[0]),
            prepare_secondary_dataframe(dfs[1])
        ),
        lambda dfs: perform_initial_merge(*dfs),
        process_matched_records,
        cleanup_final_dataframe
    ]

    result = reduce(
        lambda data, step: step(data),
        pipeline,
        (primary_df, secondary_df)
    )

    return result


def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.dropna(subset=['event_date', 'country', 'city'])

    df['latitude'] = df['latitude'].fillna(np.nan)
    df['longitude'] = df['longitude'].fillna(np.nan)

    df['num_perpetrators_captured'] = df['num_perpetrators_captured'].fillna(0).astype(int)

    numeric_columns = [
        'num_killed', 'num_terrorist_killed', 'num_wounded',
        'num_terrorist_wounded', 'total_casualties',
        'num_perpetrators', 'num_perpetrators_captured'
    ]
    for col in numeric_columns:
        df[col] = df[col].apply(lambda x: np.nan if x is None or x < 0 else x)

    string_columns = ['country', 'city', 'region', 'province_or_state']
    for col in string_columns:
        df[col] = df[col].apply(lambda x: x.strip().title() if isinstance(x, str) else x)

    target_columns = ['target_type_1', 'target_type_2', 'target_type_3']
    for col in target_columns:
        df[col] = df[col].fillna(np.nan)

    return df


def create_data_processing_pipeline() -> pd.DataFrame:
    PipelineStep = Callable[[Any], Any]

    def load_dataframes(_: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return load_primary_csv(), load_secondary_csv()

    def convert_dates(dfs: Tuple[pd.DataFrame, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        primary_df, secondary_df = dfs
        return (
            convert_dates_primary_df(primary_df),
            convert_dates_secondary_df(secondary_df)
        )

    pipeline: List[PipelineStep] = [
        load_dataframes,
        convert_dates,
        lambda dfs: merge_and_enrich_dataframes(*dfs),
    ]

    result = reduce(
        lambda data, step: step(data),
        pipeline,
        None
    )
    result = rename_event_record_columns(result)

    return normalize_data(result)


def create_event_batches(events: List[EventRecord], batch_size: int = 10) -> Generator[List[Dict], None, None]:
    event_dicts = [event.model_dump() for event in events]
    for i in range(0, len(event_dicts), batch_size):
        yield event_dicts[i:i + batch_size]
