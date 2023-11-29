from typing import List, Optional, Tuple, Dict

import logging

from tracardi.domain.entity import Entity
from tracardi.domain.event import Event
from tracardi.domain.rule import Rule
from tracardi.exceptions.log_handler import log_handler
from tracardi.service.storage.mysql.mapping.workflow_trigger_mapping import map_to_workflow_trigger_table, \
    map_to_workflow_trigger_rule
from tracardi.service.storage.mysql.schema.table import WorkflowTriggerTable
from tracardi.service.storage.mysql.utils.select_result import SelectResult
from tracardi.service.storage.mysql.service.table_service import TableService, where_tenant_context
from tracardi.event_server.utils.memory_cache import MemoryCache, CacheItem
from tracardi.config import tracardi, memory_cache as memory_cache_config

logger = logging.getLogger(__name__)
logger.setLevel(tracardi.logging_level)
logger.addHandler(log_handler)
memory_cache = MemoryCache("rules")

class WorkflowTriggerService(TableService):

    async def load_all(self, search: str = None, limit: int = None, offset: int = None) -> SelectResult:
        where = None
        if search:
            where = where_tenant_context(
                WorkflowTriggerTable,
                WorkflowTriggerTable.name.like(f'%{search}%')
            )

        return await self._select_query(WorkflowTriggerTable,
                                        where=where,
                                        order_by=WorkflowTriggerTable.name,
                                        limit=limit,
                                        offset=offset)

    async def load_by_id(self, trigger_id: str) -> SelectResult:
        return await self._load_by_id(WorkflowTriggerTable, primary_id=trigger_id)

    async def delete_by_id(self, trigger_id: str) -> str:
        return await self._delete_by_id(WorkflowTriggerTable, primary_id=trigger_id)

    async def insert(self, workflow_trigger: Rule):
        return await self._replace(WorkflowTriggerTable, map_to_workflow_trigger_table(workflow_trigger))

    # Custom

    async def load_by_workflow(self,
                               workflow_id: str,
                               limit: int = None,
                               offset: int = None, ) -> SelectResult:
        where = where_tenant_context(
            WorkflowTriggerTable,
            WorkflowTriggerTable.flow_id == workflow_id
        )

        return await self._select_query(WorkflowTriggerTable,
                                        where=where,
                                        limit=limit,
                                        offset=offset)

    async def _load_rule(self, event_type_id, source_id):
        where = where_tenant_context(
            WorkflowTriggerTable,
            WorkflowTriggerTable.event_type_id == event_type_id,
            WorkflowTriggerTable.source_id == source_id,
            WorkflowTriggerTable.enabled == True
        )

        return await self._select_query(
            WorkflowTriggerTable,
            where=where
        )

    @staticmethod
    def _get_cache_key(source_id, event_type):
        return f"rules-{source_id}-{event_type}"

    async def _get_rules_for_source_and_event_type(self, source: Entity, events: List[Event]) -> Tuple[
        Dict[str, List[Rule]], bool]:

        # Get event types for valid events

        event_types = {event.type for event in events if event.metadata.valid}

        # Cache rules per event types

        event_type_rules = {}
        has_routes = False
        for event_type in event_types:

            cache_key = self._get_cache_key(source.id, event_type)
            if cache_key not in memory_cache:
                logger.debug("Loading routing rules for cache key {}".format(cache_key))
                records = await self._load_rule(event_type, source.id)
                rules: List[Rule] = list(records.map_to_objects(map_to_workflow_trigger_rule))

                memory_cache[cache_key] = CacheItem(data=rules,
                                                    ttl=memory_cache_config.trigger_rule_cache_ttl)

            routes = list(memory_cache[cache_key].data)
            if not has_routes and routes:
                has_routes = True

            event_type_rules[event_type] = routes

        return event_type_rules, has_routes

    @staticmethod
    def _read_rule(event_type_id: str, rules: Dict[str, List[Rule]]) -> List[Rule]:
        if event_type_id not in rules:
            return []

        return rules[event_type_id]

    async def load_by_source_and_events(self, source: Entity, events: List[Event]) -> Optional[List[Tuple[List[Rule], Event]]]:
        rules, has_routing_rules = await self._get_rules_for_source_and_event_type(source, events)

        if not has_routing_rules:
            return None

        return [(self._read_rule(event.type, rules), event) for event in events]

    async def load_by_event_type(self, event_type_id: str, limit: int = 100) -> SelectResult:
        where = where_tenant_context(
            WorkflowTriggerTable,
            WorkflowTriggerTable.event_type_id == event_type_id
        )

        return await self._select_query(WorkflowTriggerTable,
                                        where=where,
                                        limit=limit)

    async def load_by_segment(self, segment_id: str, limit: int = 100) -> SelectResult:
        where = where_tenant_context(
            WorkflowTriggerTable,
            WorkflowTriggerTable.segment_id == segment_id
        )

        return await self._select_query(WorkflowTriggerTable,
                                        where=where,
                                        limit=limit)
