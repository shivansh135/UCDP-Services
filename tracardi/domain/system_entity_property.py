from typing import Optional, List, Set

from tracardi.domain.entity import Entity


def get_wildcard(path):
    return f"{path}.*"


class SystemEntityProperty(Entity):
    entity: str
    path: Optional[str] = None
    property: str
    type: str
    default: Optional[str] = None
    optional: bool = False
    converter: Optional[str] = None
    merge_strategies: Optional[List[str]]
    nested: Optional[bool] = False
    undefined: Optional[bool] = False

    def wildcard_property(self) -> bool:
        return self.property == get_wildcard(self.path)

    def property_in(self, properties) -> bool:
        return self.property in properties

    def is_property_of(self, fields, in_path) -> bool:
        return (self.property_in(fields) or self.wildcard_property()) and self.path == in_path

    def __hash__(self):
        return hash(f"{self.path}-{self.property}")


class SystemEntityPropertySet(Set[SystemEntityProperty]):

    def __str__(self):
        return f"SystemEntityPropertySet({[item.property for item in self]})"