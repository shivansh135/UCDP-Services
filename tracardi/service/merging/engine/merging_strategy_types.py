from typing import Callable

from tracardi.domain.named_entity import NamedEntity


class StrategyRecord(NamedEntity):
    description: str
    strategy: Callable


# Value Updates
LAST_UPDATE = 'last_update'
FIRST_UPDATE = 'first_update'

# Value Updates
LAST_DATETIME = 'last_datetime'
FIRST_DATETIME = 'first_datetime'

# Bool
ALWAYS_TRUE = 'always_true'
ALWAYS_FALSE = 'always_false'
AND = 'and'
OR = 'or'

# Integer
SUM = 'sum'
AVG = 'avg'
MAX = 'max'
MIN = 'min'

# List
UNIQUE_CONCAT = 'unique_concat'
CONCAT = 'concat'
FIRST = 'first'
LAST = 'last'

FIRST_PROFILE_INSERT_TIME = 'first_profile_insert_time'
LAST_PROFILE_INSERT_TIME = 'last_profile_insert_time'
LAST_PROFILE_UPDATE_TIME = 'last_profile_update_time'

FIRST_ITEM = 'first_item'


DEFAULT_STRATEGIES = [LAST_UPDATE, LAST_PROFILE_UPDATE_TIME, LAST_PROFILE_INSERT_TIME]

allowed_merges_by_type = {
    'str': [LAST_UPDATE, FIRST_UPDATE]
}


