from typing import List

from tracardi.config import memory_cache
from tracardi.domain.destination import Destination
from tracardi.service.storage.mysql.interface.destination import load_destinations_for_event_type, \
    load_destinations_for_profile
from tracardi.service.decorators.function_memory_cache import async_cache_for


@async_cache_for(memory_cache.event_destination_cache_ttl)
async def load_event_destinations(event_type, source_id) -> List[Destination]:
    destination, total = await load_destinations_for_event_type(event_type, source_id)
    return destination


@async_cache_for(memory_cache.profile_destination_cache_ttl)
async def load_profile_destinations() -> List[Destination]:
    destination, total = await load_destinations_for_profile()
    return destination
