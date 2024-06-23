from dotty_dict import Dotty
from itertools import chain

from typing import Optional, List

from tracardi.service.merging.new.value_timestamp import ValueTimestamp


def convert_list(value) -> Optional[list]:
    if isinstance(value, list):
        return value

    if isinstance(value, set):
        return list(value)

    return None


class ListStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata):
        self.profiles = profiles
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        for value_meta in self.field_metadata.values:
            if value_meta.value is None:
                continue
            if not isinstance(value_meta.value, (list, set)):
                return False
        return True


class ConCatStrategy(ListStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Convert all to datetime
        data = [convert_list(value_meta.value) for value_meta in self.field_metadata.values]

        # Filter out tuples with None as the fist (value) element
        filtered_data = [value for value in data if value is not None]

        return ValueTimestamp(value=list(chain.from_iterable(filtered_data)))


class ConCatDistinctStrategy(ListStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Convert all to datetime
        data = [convert_list(value_meta.value) for value_meta in self.field_metadata.values]

        # Filter out tuples with None as the fist (value) element
        filtered_data = [value for value in data if value is not None]

        return ValueTimestamp(value=list(set((chain.from_iterable(filtered_data)))))
