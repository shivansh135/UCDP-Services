from dateparser import parse
from datetime import datetime

from dotty_dict import Dotty
from typing import Optional, List

from tracardi.service.merging.engine.utils.converter import _convert
from tracardi.service.merging.engine.value_timestamp import ValueTimestamp


def _parse(value):
    if isinstance(value, datetime):
        return value
    return parse(value)

class LastProfileInsertTimeStrategy:

    def __init__(self, profile: Dotty, field_metadata):
        self.profile = profile
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        for value_meta in self.field_metadata.values:  # List[ValueTimestamp]
            if value_meta.profile_insert is None:
                return False
            date = _parse(value_meta.profile_insert)
            if not isinstance(date, datetime):
                return False
        return True

    def merge(self) -> Optional[ValueTimestamp]:
        # Fiter values that are empty
        values = [value for value in self.field_metadata.values if not value.is_empty_value()]

        if not values:
            # If all values are empty return None Value
            return ValueTimestamp(value=None)

        sorted_data = max(values,
                          key=lambda value_meta: _parse(value_meta.profile_insert))

        # Return the first tuple in the sorted list
        return _convert(sorted_data)

class FirstProfileInsertTimeStrategy:

    def __init__(self, profile: Dotty, field_metadata):
        self.profile = profile
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        for value_meta in self.field_metadata.values:  # List[ValueTimestamp]
            if value_meta.profile_insert is None:
                return False
            date = _parse(value_meta.profile_insert)
            if not isinstance(date, datetime):
                return False
        return True

    def merge(self) -> Optional[ValueTimestamp]:

        # Fiter values that are empty
        values = [value for value in self.field_metadata.values if not value.is_empty_value()]

        if not values:
            # If all values are empty return None Value
            return ValueTimestamp(value=None)

        sorted_data = min(values,
                          key=lambda value_meta: _parse(value_meta.profile_insert))

        # Return the first tuple in the sorted list
        return _convert(sorted_data)


class LastProfileUpdateTimeStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata):
        self.profiles = profiles
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        for value_meta in self.field_metadata.values:  # List[ValueTimestamp]
            if value_meta.profile_update is None:
                return False

            if isinstance(value_meta.profile_update, datetime):
                return True

            date = _parse(value_meta.profile_update)

            if not isinstance(date, datetime):
                return False
        return True

    def merge(self) -> Optional[ValueTimestamp]:

        # Fiter values that are empty
        values = [value for value in self.field_metadata.values if not value.is_empty_value()]

        if not values:
            # If all values are empty return None Value
            return ValueTimestamp(value=None)

        sorted_data = max(values, key=lambda value_meta: _parse(value_meta.profile_update))

        # Return the first tuple in the sorted list
        return _convert(sorted_data)
