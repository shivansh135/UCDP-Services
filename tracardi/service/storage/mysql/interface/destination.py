from typing import List, Tuple, Optional

from tracardi.domain.destination import Destination
from tracardi.service.storage.mysql.mapping.destination_mapping import map_to_destination
from tracardi.service.storage.mysql.service.destination_service import DestinationService
from tracardi.service.storage.preconfig.preconfigured_metadata import pc_destinations

ds = DestinationService()


def _append_pre_config_records(records) -> Tuple[List[Destination], int]:
    pre_config_destinations = [Destination(**item) for item in pc_destinations.values()] \
        if pc_destinations else []

    if not records.exists():
        return pre_config_destinations, len(pre_config_destinations)

    destinations = pre_config_destinations
    for destination in records.map_to_objects(map_to_destination):
        destinations.append(destination)

    return destinations, records.count() + len(pc_destinations)


async def load_destination_by_id(id: str) -> Optional[Destination]:
    pre_config_destinations = pc_destinations if pc_destinations else {}

    if id in pre_config_destinations:
        return Destination(**pc_destinations[id])

    record = await ds.load_by_id(id)

    if not record.exists():
        return None

    return record.map_to_object(map_to_destination)


async def load_all_destinations(query, start, limit) -> Tuple[List[Destination], int]:
    records = await ds.load_all(query, start, limit)
    return _append_pre_config_records(records)


async def load_destinations_for_event_type(event_type: str, source_id: str) -> Tuple[List[Destination], int]:
    records = await ds.load_event_destinations(event_type, source_id)
    return _append_pre_config_records(records)


async def load_destinations_for_profile():
    records = await ds.load_profile_destinations()
    return _append_pre_config_records(records)


async def insert_destination(destination: Destination):
    return await ds.insert(destination)


async def delete_destination(id: str):
    await ds.delete_by_id(id)
