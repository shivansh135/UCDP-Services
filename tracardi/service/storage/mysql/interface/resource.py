from typing import Tuple, List, Optional

from tracardi.domain.resource import Resource
from tracardi.service.storage.mysql.map_to_named_entity import map_to_named_entity
from tracardi.service.storage.mysql.mapping.resource_mapping import map_to_resource
from tracardi.service.storage.mysql.service.resource_service import ResourceService

rs = ResourceService()


def _records(records, mapper) -> Tuple[List[Resource], int]:
    if not records.exists():
        return [], 0

    return list(records.map_to_objects(mapper)), records.count()


async def load_resource_by_id(id: str) -> Optional[Resource]:
    record = await rs.load_by_id(id)
    return record.map_to_object(map_to_resource)


async def load_all_resources(search: str = None, limit: int = None, offset: int = None) -> Tuple[List[Resource], int]:
    records = await rs.load_all(search, limit, offset)
    return _records(records, map_to_resource)


async def load_all_resource_entities(search: str = None, limit: int = None, offset: int = None) -> Tuple[
    List[Resource], int]:
    records = await rs.load_all(search, limit, offset)
    return _records(records, map_to_named_entity)
