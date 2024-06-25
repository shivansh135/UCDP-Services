from dotty_dict import Dotty
from typing import Optional, List

from datetime import datetime

from tracardi.service.merging.engine.utils.converter import _convert
from tracardi.service.merging.engine.value_timestamp import ValueTimestamp


class ValueUpdateStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata):
        self.profiles = profiles
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        # Checks if the prerequisite of all items having timestamp is met. Cant select last update if there is no timestamps on all fields.
        for value_meta in self.field_metadata.values:
            if not value_meta.timestamp:
                return False
        return True

class LastUpdateStrategy(ValueUpdateStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Filter out tuples with None as the second element and convert
        filtered_data = [ValueTimestamp(
            value=value_meta.value,
            timestamp=value_meta.timestamp.timestamp() if isinstance(value_meta.timestamp, datetime) else value_meta.timestamp)
            for value_meta in self.field_metadata.values if value_meta.timestamp is not None and not value_meta.is_empty_value()]

        # Sort the filtered data based on the second element
        sorted_data = max(filtered_data, key=lambda value_meta: value_meta.timestamp)

        # Return the first tuple in the sorted list
        return _convert(sorted_data)


class FirstUpdateStrategy(ValueUpdateStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Filter out tuples with None as the second element and convert
        filtered_data = [ValueTimestamp(
            value=value_meta.value,
            timestamp=value_meta.timestamp.timestamp() if isinstance(value_meta.timestamp, datetime) else value_meta.timestamp)
            for value_meta in self.field_metadata.values if value_meta.timestamp is not None]

        # Sort the filtered data based on the second element
        sorted_data = min(filtered_data, key=lambda value_meta: value_meta.timestamp)

        # Return the first tuple in the sorted list
        return _convert(sorted_data)