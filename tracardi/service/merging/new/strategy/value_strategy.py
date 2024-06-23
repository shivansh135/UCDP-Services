from dotty_dict import Dotty
from typing import Optional, List

from tracardi.service.merging.new.utils.converter import _convert
from tracardi.service.merging.new.value_timestamp import ValueTimestamp


class ValueStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata):
        self.profiles = profiles
        self.field_metadata = field_metadata

    def prerequisites(self) -> bool:
        # Checks if the prerequisite of all items being numbers. Cant select min value if there is numbers.
        for field_ref in self.field_metadata.values:
            if not isinstance(field_ref.value, (int, float)):
                return False
        return True


class MinValueStrategy(ValueStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Filter out tuples with None as the fist (value) element
        filtered_data = [field_ref for field_ref in self.field_metadata.values if field_ref.value is not None]

        sorted_data = min(filtered_data, key=lambda field_ref: field_ref.value)

        # Return the first tuple in the sorted list
        return _convert(sorted_data)


class MaxValueStrategy(ValueStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Filter out tuples with None as the fist (value) element
        filtered_data = [field_ref for field_ref in self.field_metadata.values if field_ref.value is not None]

        sorted_data = max(filtered_data, key=lambda field_ref: field_ref.value)

        # Return the first tuple in the sorted list
        return _convert(sorted_data)


class SumValueStrategy(ValueStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Filter out tuples with None as the fist (value) element
        filtered_data = [field_ref.value for field_ref in self.field_metadata.values if field_ref.value is not None]

        # Return the first tuple in the sorted list
        return ValueTimestamp(value=sum(filtered_data))


class AvgValueStrategy(ValueStrategy):

    def merge(self) -> Optional[ValueTimestamp]:
        # Filter out tuples with None as the fist (value) element
        filtered_data = [field_ref.value for field_ref in self.field_metadata.values if field_ref.value is not None]

        # Return the first tuple in the sorted list
        return ValueTimestamp(sum(filtered_data) / len(filtered_data), None)
