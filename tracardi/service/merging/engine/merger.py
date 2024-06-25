from typing import List, Optional, Tuple, Dict
from tracardi.domain.profile import FlatProfile
from tracardi.domain.storage_record import RecordMetadata
from tracardi.domain.system_entity_property import SystemEntityProperty
from tracardi.service.merging.engine.field_manager import FieldManager, index_fields, ProfileDataSpliter
from tracardi.service.merging.engine.merging_strategy_types import DEFAULT_STRATEGIES
from tracardi.service.setup.mappings.objects.profile import default_profile_properties


def merge_profiles(profiles_with_meta: List[Tuple[FlatProfile, Optional[RecordMetadata]]],
                   merge_strategies: Optional[List[SystemEntityProperty]] = None) -> Tuple[FlatProfile, Optional[RecordMetadata], dict]:

    if merge_strategies is None:
        merge_strategies = default_profile_properties

    # Get only profiles
    profiles = [profile for profile, metadata in profiles_with_meta]
    path = ""
    indexed_profile_field_settings = index_fields(merge_strategies, path)

    ps = ProfileDataSpliter(profiles,
                            indexed_profile_field_settings,
                            DEFAULT_STRATEGIES,
                            path=path,
                            skip_values=[])

    merged_profile_fields_settings, profile_to_timestamps = ps.split()

    fm = FieldManager(
        profiles,
        merged_profile_field_settings=merged_profile_fields_settings,
        profile_id_to_timestamps=profile_to_timestamps,
        path=path,
        default_strategies=DEFAULT_STRATEGIES
    )

    id_to_metadata: Dict[str, Optional[RecordMetadata]] = {profile['id']: metadata for profile, metadata in profiles_with_meta}
    merged_profile, changed_fields = fm.merge(path)

    merged_profile_id = merged_profile.get('id', None)

    metadata = None
    if merged_profile_id:
        metadata = id_to_metadata.get(merged_profile_id, None)

    return merged_profile, metadata, changed_fields



