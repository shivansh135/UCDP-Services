from typing import Optional, List

from tracardi.domain.entity import Entity


class SystemEntityProperty(Entity):
    entity: str
    path: Optional[str] = None
    property: str
    type: str
    default: Optional[str] = None
    optional: bool = False
    converter: Optional[str] = None
    merge_strategies: List[str]
    nested: Optional[bool] = False
