from collections import namedtuple

from dotty_dict import Dotty
from typing import List, Optional, Dict, Set, Tuple

from pydantic import BaseModel

from tracardi.domain.system_entity_property import SystemEntityProperty
from tracardi.service.merging.engine.merging_strategy_types import StrategyRecord, DEFAULT_STRATEGIES
from tracardi.service.merging.engine.strategy_mapping import id_to_strategy
from tracardi.service.merging.engine.strategy_protocol import StrategyProtocol
from tracardi.service.merging.engine.value_timestamp import ValueTimestamp, ProfileValueTimestamp
from tracardi.service.setup.mappings.objects.profile import default_profile_properties

MergedValue = namedtuple('MergedValue', ['value', 'timestamp', 'strategy_id', "changed_fields"])
TimestampTuple = namedtuple('TimestampTuple', ['id', 'fields', 'insert', 'update'])

def to_profile_timestamps(data: List[ValueTimestamp]):
    from tracardi.service.merging.engine.field_manager import ProfileTimestamps
    return ProfileTimestamps(
        insert=data[0].profile_insert,
        update=data[0].profile_update,
        field={item.value: item.timestamp for item in data}
    )


def get_field_settings(profiles: List[Dotty], indexed_custom_profile_field_settings, path) -> Set[SystemEntityProperty]:
    from tracardi.service.merging.engine.field_manager import get_profile_field_settings
    set_of_field_settings = set()
    for profile in profiles:
        # Get all setting for fields
        for field_setting in get_profile_field_settings(
                indexed_custom_profile_field_settings,
                profile,
                DEFAULT_STRATEGIES,
                path,
                skip_values=['id']  # Skip id if in profile (this is nested data not id is needed)
        ):
            set_of_field_settings.add(field_setting)
    return set_of_field_settings

class DictStrategy:

    def __init__(self, profiles: List[Dotty], field_metadata: 'FieldMetaData'):
        self.profiles = profiles
        self.field_metadata = field_metadata


    def prerequisites(self) -> bool:
        for value_meta in self.field_metadata.values:  # List[FieldRef]
            if value_meta.value is None:
                continue
            if not isinstance(value_meta.value, dict):
                return False
        return True

    def merge(self) -> Tuple[Dotty, dict]:

        from tracardi.service.merging.engine.field_manager import ProfileTimestamps
        from tracardi.service.merging.engine.field_manager import FieldManager, index_fields

        print('---------', self.field_metadata.field)

        # Get time changes from profiles for nested fields only

        profile_timestamps_by_id: Dict[str, ProfileTimestamps] = {profile['id']:
            ProfileTimestamps(
                field={field: timestamp[0] for field,timestamp in profile['metadata']['fields'].items()},
                insert=profile['metadata']['time']['insert'],
                update=profile['metadata']['time']['update'])
            for profile in self.profiles}

        # Get the defined settings (mappings) for nested fields

        path = self.field_metadata.field
        indexed_custom_profile_field_settings = index_fields(default_profile_properties, path)

        nested_profiles = [Dotty({
                    "id": value_meta.id,
                    self.field_metadata.field: value_meta.value
                }) for value_meta in self.field_metadata.values]

        set_of_field_settings: Set[SystemEntityProperty] = get_field_settings(nested_profiles, indexed_custom_profile_field_settings, path)

        # print(self.field_metadata.field)
        # print(self.field_metadata.field_values())
        # print(set_of_field_settings)

        fm = FieldManager(
            nested_profiles,
            profile_id_to_timestamps=profile_timestamps_by_id,
            merged_profile_field_settings=set_of_field_settings,
            default_strategies=DEFAULT_STRATEGIES,
            path=path
        )

        result, changed_fields = fm.merge(path)

        print(result)
        print('-----END')
        return result, changed_fields

    # def merge(self):
    #     return ValueTimestamp(value=1)


class FieldMetaData(BaseModel):
    path: Optional[str] = None
    field: str
    values: List[ProfileValueTimestamp]
    type: str
    strategies: List[str] = []
    nested: Optional[bool] = False


    def __hash__(self):
        return hash(f"{self.path}.{self.field}")

    @staticmethod
    def _invoke_strategy(strategy: StrategyProtocol) -> ValueTimestamp:
        if not strategy.prerequisites():
            raise AssertionError("Strategy prerequisites not met.")
        return strategy.merge()


    def _merge(self, profiles: List[Dotty], strategies: List[str]) -> MergedValue:
        for strategy_id in strategies:
            strategy: StrategyRecord = id_to_strategy.get(strategy_id, None)
            if not strategy:
                raise ValueError(f"Unknown merging strategy '{strategy_id}'.")

            # Nested
            if self.nested:

                nested_strategy = DictStrategy(profiles, self)
                if not nested_strategy.prerequisites():
                    raise AssertionError("Strategy prerequisites not met.")
                merged_value, changed_fields = nested_strategy.merge()

                return MergedValue(merged_value, None, strategy_id, changed_fields)

            else:
                strategy_class = strategy.strategy(profiles, self)

                try:
                    result = self._invoke_strategy(strategy_class)
                    return MergedValue(result.value, result.timestamp, strategy_id, None)
                except AssertionError:
                    continue

        values = [(v.value, v.timestamp) for v in self.values]
        raise ValueError(f"Could not merge field '{self.field}', No merging strategy qualified for value merging. Field values {values}")



    def merge(self, profiles: List[Dotty], default_strategies: List[str]) -> MergedValue:
        try:
            return self._merge(profiles, self.strategies)
        except ValueError:
            print("Falling to default strategies", default_strategies)
            return self._merge(profiles, default_strategies)


    def field_values(self):
        return [value.value for value in self.values]

