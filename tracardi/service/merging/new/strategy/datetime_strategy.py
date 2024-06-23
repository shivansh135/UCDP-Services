from dotty_dict import Dotty
from typing import Optional, List

from dateparser import parse
from datetime import datetime

from tracardi.service.merging.new.utils.converter import _convert
from tracardi.service.merging.new.value_timestamp import ValueTimestamp


def convert_time(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value

    if isinstance(value, float):
        return datetime.fromtimestamp(value)

    if isinstance(value, str):
        return parse(value)

    return None

class DateTimeStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata):
        self.profiles = profiles
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        # Checks if the prerequisite of all items being numbers. Cant select min value if there is numbers.
        for value_meta in self.field_metadata.values:
            if not isinstance(value_meta.value, (datetime, float, str)):
                return False
        return True

class MinDateTimeStrategy(DateTimeStrategy):


    def merge(self):
        # Convert all to datetime
        data = [ValueTimestamp(value=convert_time(value_meta.value), timestamp=value_meta.timestamp) for value_meta in self.field_metadata.values]

        # Filter out tuples with None as the fist (value) element
        filtered_data = [value_meta for value_meta in data if not value_meta.is_empty_value()]

        # Sort the filtered data based on the second element
        sorted_data = min(filtered_data, key=lambda value_meta: value_meta.value)

        # Return the first tuple in the sorted list
        return _convert(sorted_data)


class MaxDateTimeStrategy(DateTimeStrategy):


    def merge(self):
        # Convert all to datetime
        data = [ValueTimestamp(value=convert_time(value_meta.value), timestamp=value_meta.timestamp) for
                value_meta in self.field_metadata.values]

        # Filter out tuples with None as the fist (value) element
        filtered_data = [value_meta for value_meta in data if not value_meta.is_empty_value()]

        # Sort the filtered data based on the second element
        sorted_data = max(filtered_data, key=lambda value_meta: value_meta.value)

        # Return the first tuple in the sorted list
        return _convert(sorted_data)

