from typing import List

from app.models.terror_event import TerrorEvent
from app.services.neo4j__structure_service import Neo4jProcessor


def save_neo4j_queries(events: List[TerrorEvent], output_path: str):
    processor = Neo4jProcessor()
    queries = processor.process_events(events)

    with open(output_path, 'w', encoding='utf-8') as f:
        for i, query in enumerate(queries, 1):
            f.write(f"{query};\n")