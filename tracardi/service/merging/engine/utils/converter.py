from typing import Optional

from datetime import datetime
from tracardi.service.merging.engine.value_timestamp import ValueTimestamp


def _convert(field_ref: ValueTimestamp) -> Optional[ValueTimestamp]:
    if field_ref:
        if isinstance(field_ref.timestamp, datetime):
            field_ref.timestamp = field_ref.timestamp.timestamp()
        return field_ref
    return None
