from typing import List, Dict
import pandas as pd
from datetime import datetime
from pydantic import ValidationError
from app.models.pydantic_model.event_record import EventRecord


def convert_row_to_dict(row: pd.Series) -> Dict:
    return row.where(pd.notna(row), None).to_dict()


def parse_date(date_str: str) -> datetime:
    return (
        datetime.strptime(date_str, '%Y-%m-%d')
        if isinstance(date_str, str)
        else date_str
    )


def validate_dataframe_to_events(df: pd.DataFrame) -> List[EventRecord]:
    try:
        validated_records = [
            EventRecord(
                **{
                    **convert_row_to_dict(row),
                    'event_date': parse_date(row.event_date)
                }
            )
            for _, row in df.iterrows()
            if pd.notna(row.event_date)  # Skip rows without date
        ]

        print(f"\nValidation Summary:")
        print(f"Total records processed: {len(df)}")
        print(f"Successfully validated: {len(validated_records)}")
        print(f"Failed/Skipped: {len(df) - len(validated_records)}")

        return validated_records

    except Exception as e:
        print(f"Error during validation: {str(e)}")
        return []

# Example usage:
# validated_events = validate_dataframe_to_events(processed_df)