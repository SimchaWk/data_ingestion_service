from typing import List, Set

from app.models.terror_event import TerrorEvent


class Neo4jProcessor:
    def __init__(self):
        self.locations: Set[tuple] = set()
        self.terror_groups: Set[str] = set()
        self.attack_types: Set[str] = set()
        self.targets: Set[str] = set()
        self.cypher_queries: List[str] = []

    def _clean_string(self, text: str) -> str:
        if text is None:
            return ""
        return text.replace("'", "\\'")

    def _is_valid_string(self, value: str) -> bool:
        return value and value.strip() and value.lower() != "unknown"

    def _extract_unique_entities(self, events: List[TerrorEvent]):
        for event in events:
            if self._is_valid_string(event.country) and self._is_valid_string(event.city):
                location = (
                    self._clean_string(event.country),
                    self._clean_string(event.city),
                    self._clean_string(event.region) if self._is_valid_string(event.region) else None,
                    self._clean_string(event.province_or_state) if self._is_valid_string(
                        event.province_or_state) else None,
                    event.latitude if event.latitude is not None else None,
                    event.longitude if event.longitude is not None else None
                )
                self.locations.add(location)

            if event.terror_groups:
                self.terror_groups.update(
                    group for group in event.terror_groups
                    if self._is_valid_string(group)
                )
            if event.attack_types:
                self.attack_types.update(
                    attack_type for attack_type in event.attack_types
                    if self._is_valid_string(attack_type)
                )
            if event.target_details:
                self.targets.update(
                    target for target in event.target_details
                    if self._is_valid_string(target)
                )

    def _generate_entity_queries(self):
        for loc in self.locations:
            country, city, region, province, lat, lon = loc
            props = {
                'country': country,
                'city': city,
                'region': region,
                'province': province,
                'latitude': lat,
                'longitude': lon
            }
            props_str = ', '.join(f"{k}: '{v}'" for k, v in props.items() if v is not None)
            self.cypher_queries.append(f"MERGE (l:Location {{{props_str}}})")

        for group in self.terror_groups:
            if group and group.lower() != "unknown":
                self.cypher_queries.append(f"MERGE (g:TerrorGroup {{name: '{self._clean_string(group)}'}})")

        for attack_type in self.attack_types:
            self.cypher_queries.append(f"MERGE (at:AttackType {{type: '{self._clean_string(attack_type)}'}})")

        for target in self.targets:
            self.cypher_queries.append(f"MERGE (t:Target {{type: '{self._clean_string(target)}'}})")

    def _generate_attack_queries(self, events: List[TerrorEvent]):
        for event in events:
            if not self._is_valid_string(event.event_id):
                continue

            attack_props = {
                'id': event.event_id,
                'date': event.event_date.isoformat(),
                'data_source': event.data_source
            }

            numeric_props = {
                'num_killed': event.num_killed,
                'num_wounded': event.num_wounded,
                'total_casualties': event.total_casualties,
                'num_perpetrators': event.num_perpetrators,
                'num_perpetrators_captured': event.num_perpetrators_captured
            }
            attack_props.update({k: v for k, v in numeric_props.items() if v is not None and v >= 0})

            text_props = {
                'summary': event.summary,
                'description': event.description
            }
            attack_props.update({k: v for k, v in text_props.items()
                                 if self._is_valid_string(v)})

            props_str = ', '.join(f"{k}: {repr(v)}" for k, v in attack_props.items())
            self.cypher_queries.append(f"CREATE (a:Attack {{{props_str}}})")

            if self._is_valid_string(event.country) and self._is_valid_string(event.city):
                self._create_relationships(event)

    def _create_relationships(self, event: TerrorEvent):
        if self._is_valid_string(event.country) and self._is_valid_string(event.city):
            self.cypher_queries.append(
                f"MATCH (a:Attack {{id: '{event.event_id}'}}), "
                f"(l:Location {{country: '{self._clean_string(event.country)}', "
                f"city: '{self._clean_string(event.city)}'}}) "
                f"CREATE (a)-[:OCCURRED_AT]->(l)"
            )

        if event.terror_groups:
            for group in event.terror_groups:
                if self._is_valid_string(group):
                    self.cypher_queries.append(
                        f"MATCH (a:Attack {{id: '{event.event_id}'}}), "
                        f"(g:TerrorGroup {{name: '{self._clean_string(group)}'}}) "
                        f"CREATE (a)-[:CONDUCTED_BY]->(g)"
                    )

        if event.attack_types:
            for attack_type in event.attack_types:
                if self._is_valid_string(attack_type):
                    self.cypher_queries.append(
                        f"MATCH (a:Attack {{id: '{event.event_id}'}}), "
                        f"(at:AttackType {{type: '{self._clean_string(attack_type)}'}}) "
                        f"CREATE (a)-[:TYPE_OF]->(at)"
                    )

        if event.target_details:
            for target in event.target_details:
                if self._is_valid_string(target):
                    self.cypher_queries.append(
                        f"MATCH (a:Attack {{id: '{event.event_id}'}}), "
                        f"(t:Target {{type: '{self._clean_string(target)}'}}) "
                        f"CREATE (a)-[:TARGETED]->(t)"
                    )

    def process_events(self, events: List[TerrorEvent]) -> List[str]:
        self._extract_unique_entities(events)
        self._generate_entity_queries()
        self._generate_attack_queries(events)
        return self.cypher_queries


def create_neo4j_queries(events: List[TerrorEvent]) -> List[str]:
    processor = Neo4jProcessor()
    return processor.process_events(events)
