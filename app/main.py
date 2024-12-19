import os

from app.config.local_files_config.local_files import MERGED_FILES
from app.repositories.local_files_repository import save_dataframe_to_csv
from app.services.data_processor_service import create_data_processing_pipeline
from app.services.data_validator_service import validate_dataframe_to_events
from app.services.kafka_publisher_service import publish_events_in_batches


def main():
    # קריאה ועיבוד ראשוני - קיים
    merged_df = create_data_processing_pipeline()
    # save_dataframe_to_csv(merged_df, MERGED_FILES)

    # ולידציה והמרה למודלים
    validated_events = validate_dataframe_to_events(merged_df)

    # חילוץ ישויות ייחודיות
    # unique_entities = extract_unique_entities(df)

    # Publish in batches
    publish_events_in_batches(
        events=validated_events,
        topic=os.environ['TERROR_EVENTS'],
        batch_size=100
    )


if __name__ == '__main__':
    main()
