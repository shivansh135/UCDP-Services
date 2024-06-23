from typing import Protocol, Optional
from tracardi.service.merging.new.value_timestamp import ValueTimestamp


class StrategyProtocol(Protocol):
    def prerequisites(self) -> bool:
        ...

    def merge(self) -> Optional[ValueTimestamp]:
        ...