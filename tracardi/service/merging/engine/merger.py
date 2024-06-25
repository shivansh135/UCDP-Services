from typing import List, Optional
from tracardi.domain.profile import FlatProfile
from tracardi.domain.system_entity_property import SystemEntityProperty
from tracardi.service.merging.engine.field_manager import FieldManager, index_fields, ProfileDataSpliter
from tracardi.service.merging.engine.merging_strategy_types import DEFAULT_STRATEGIES
from tracardi.service.setup.mappings.objects.profile import default_profile_properties


def merge_profiles(profiles: List[FlatProfile], merge_strategies: Optional[List[SystemEntityProperty]] = None):
    if merge_strategies is None:
        merge_strategies = default_profile_properties

    path = ""
    indexed_profile_field_settings = index_fields(merge_strategies, path)

    ps = ProfileDataSpliter(profiles, indexed_profile_field_settings, DEFAULT_STRATEGIES, path=path, skip_values=[])

    merged_profile_fields_settings, profile_to_timestamps = ps.split()

    fm = FieldManager(
        profiles,
        merged_profile_field_settings=merged_profile_fields_settings,
        profile_id_to_timestamps=profile_to_timestamps,
        path=path,
        default_strategies=DEFAULT_STRATEGIES
    )

    return fm.merge(path)
