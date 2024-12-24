import os

from app.config.kafka_config.producer import produce_batch
from app.config.local_files_config.local_files import MERGED_FILES, NEO4J_QUERIES
from app.repositories.local_files_repository import save_dataframe_to_csv
from app.services.data_processor_service import (
    create_data_processing_pipeline, add_event_id, prepare_data_for_neo4j, generate_neo4j_cypher_script
)
from app.services.data_validator_service import dataframe_to_pydantic_models, prepare_models_for_kafka
from app.services.neo4j__structure_service import create_neo4j_queries, Neo4jProcessor


def main():
    merged_df = create_data_processing_pipeline()
    merged_df = add_event_id(merged_df)
    save_dataframe_to_csv(merged_df, MERGED_FILES)

    validated_events = dataframe_to_pydantic_models(merged_df)

    kafka_messages = prepare_models_for_kafka(validated_events)

    produce_batch(
        topic=os.environ['TERROR_EVENTS'],
        messages=kafka_messages,
        batch_size=100
    )

    # save_neo4j_queries(validated_events, NEO4J_QUERIES)
    neo4j_messages = create_neo4j_queries(validated_events)

    produce_batch(
        topic=os.environ['NEO4J_ENTITIES'],
        messages=neo4j_messages,
        batch_size=100
    )


if __name__ == '__main__':
    main()
