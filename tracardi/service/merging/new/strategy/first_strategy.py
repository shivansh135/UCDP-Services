from dotty_dict import Dotty
from typing import List, Optional

from tracardi.service.merging.new.value_timestamp import ValueTimestamp


class FirstStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata):
        self.profiles = profiles
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        return len(self.field_metadata.values)>=1

    def merge(self) -> Optional[ValueTimestamp]:
        return ValueTimestamp(value=self.field_metadata.values[0])
