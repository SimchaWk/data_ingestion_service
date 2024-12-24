import json
from typing import List
from datetime import datetime
import pandas as pd

from app.models.terror_event import TerrorEvent


def dataframe_to_pydantic_models(df: pd.DataFrame) -> List[TerrorEvent]:
    attack_type_cols = ["attack_type_1", "attack_type_2", "attack_type_3"]
    target_cols = [
        "target_type_1", "target_subtype_1",
        "target_type_2", "target_subtype_2",
        "target_type_3", "target_subtype_3"
    ]
    terror_group_cols = [
        "terror_group_name", "terror_group_subname",
        "secondary_terror_group_name", "secondary_terror_group_subname",
        "tertiary_terror_group_name", "tertiary_terror_group_subname"
    ]

    return [
        TerrorEvent(
            **{
                key: row_dict[key]
                for key in TerrorEvent.__annotations__
                if key in (row_dict := row.dropna().to_dict())
            },
            attack_types=[
                row[col] for col in attack_type_cols
                if pd.notna(row.get(col))
            ],
            target_details=[
                row[col] for col in target_cols
                if pd.notna(row.get(col))
            ],
            terror_groups=[
                row[col] for col in terror_group_cols
                if pd.notna(row.get(col))
            ]
        )
        for _, row in df.iterrows()
    ]


def prepare_models_for_kafka(models: List[TerrorEvent]) -> List[str]:
    def convert_dates(data: dict) -> dict:
        return {
            key: parse_date(value).isoformat()
            if isinstance(value, (datetime, str)) and is_valid_date(value)
            else value
            for key, value in data.items()
        }

    return [
        json.dumps(
            convert_dates(
                model.model_dump(
                    exclude_unset=True,
                    exclude_none=True,
                    exclude_defaults=True
                )
            )
        )
        for model in models
    ]


def is_valid_date(value: str | datetime) -> bool:
    try:
        parse_date(value)
        return True
    except ValueError:
        return False


def parse_date(date_str: str | datetime) -> datetime:
    return (
        datetime.strptime(date_str, '%Y-%m-%d')
        if isinstance(date_str, str)
        else date_str
    )
