from typing import List, Optional, Tuple, Set

from tracardi.domain.profile import Profile, FlatProfile
from tracardi.domain.storage_record import RecordMetadata
from tracardi.service.merging.engine.merger import merge_profiles
from tracardi.service.storage.elastic.interface.merging import move_profile_events_and_sessions, \
    delete_duplicated_profiles, save_merged_profile, load_duplicated_profiles


async def compute_one_profile_in_db(profile: Profile) -> Tuple[Optional[Profile], Set[str]]:
    # Load duplicated profiles
    duplicated_profiles_with_metadata: List[
        Tuple[FlatProfile, Optional[RecordMetadata]]] = await load_duplicated_profiles(profile)

    # If any duplicated profiles
    if duplicated_profiles_with_metadata:
        # Create single flat profile
        merged_flat_profile, metadata, changed_fields = merge_profiles(duplicated_profiles_with_metadata)

        # Make sure stores changed fields
        merged_flat_profile.update_changed_fields(changed_fields)

        # Mark it as merged
        merged_flat_profile.mark_as_merged()

        # Save it
        profile = await save_merged_profile(merged_flat_profile, metadata)

        # Get all ids and id
        profile_ids = set(merged_flat_profile.get('ids', []))
        profile_id = profile.id

        # Update events and sessions
        await move_profile_events_and_sessions(duplicate_profile_ids=profile_ids,
                                               merged_profile_id=profile_id)

        # Delete duplicated profiles
        delete_profile_ids = await delete_duplicated_profiles(duplicated_profiles_with_metadata,
                                                              merged_profile_id=profile_id)

        return profile, delete_profile_ids
