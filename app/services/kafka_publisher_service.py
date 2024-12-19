from typing import List
from app.models.pydantic_model.event_record import EventRecord
from app.config.kafka_config.producer import produce
from app.services.data_processor_service import create_event_batches


def publish_events_in_batches(
        events: List[EventRecord],
        topic: str,
        batch_size: int = 100
) -> None:

    total_events = len(events)
    published = 0
    failed = 0

    for batch_number, batch in enumerate(create_event_batches(events, batch_size), 1):
        try:

            produce(
                topic=topic,
                key=f"batch_{batch_number}",
                value=batch
            )

            published += len(batch)

            print(f"Published batch {batch_number}: {len(batch)} events")

        except Exception as e:
            failed += len(batch)
            print(f"Failed to publish batch {batch_number}: {str(e)}")

    print(f"\nPublishing Summary:")
    print(f"Total events: {total_events}")
    print(f"Successfully published: {published}")
    print(f"Failed to publish: {failed}")
