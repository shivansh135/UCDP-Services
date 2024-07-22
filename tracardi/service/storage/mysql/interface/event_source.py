from typing import List, Tuple

from tracardi.domain.event_source import EventSource
from tracardi.service.storage.mysql.mapping.event_source_mapping import map_to_event_source
from tracardi.service.storage.mysql.service.event_source_service import EventSourceService

ess = EventSourceService()


def _records(records) -> Tuple[List[EventSource], int]:
    if not records.exists():
        return [], 0

    return list(records.map_to_objects(map_to_event_source)), records.count()


async def load_all_event_sources(query, limit) -> Tuple[List[EventSource], int]:
    records = await ess.load_all_in_deployment_mode(query, limit=limit)
    return _records(records)


async def delete_event_source(source_id: str):
    await ess.delete_by_id_in_deployment_mode(source_id)


async def insert_event_source(event_source: EventSource):
    return await ess.save(event_source)
