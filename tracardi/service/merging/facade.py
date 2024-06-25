from typing import List, Optional

from tracardi.domain.profile import Profile, FlatProfile
from tracardi.service.merging.engine.merger import merge_profiles
from tracardi.service.profile_merger import ProfileMerger
from tracardi.domain.storage_record import StorageRecord, RecordMetadata, StorageRecords
from tracardi.service.storage.driver.elastic.profile import load_duplicated_profiles_for_profile
from tracardi.service.storage.elastic.interface.merging import move_profile_events_and_sessions
from tracardi.service.tracking.storage.profile_storage import save_profile
from tracardi.service.storage.driver.elastic import profile as profile_db


async def merge_profile_by_merging_keys(profile: Optional[Profile], merge_by) -> Optional[Profile]:
    return await ProfileMerger.invoke_merge_profile(
        profile,
        # Merge when id = tracker_payload.profile.id
        # This basically loads the current profile.
        merge_by=merge_by,
        limit=2000)


def get_merging_keys_and_values(profile: Profile):
    return ProfileMerger.get_merging_keys_and_values(profile)


async def deduplicate_profile(profile_id: str, profile_ids: List[str] = None) -> Optional[Profile]:
    if isinstance(profile_ids, list):
        set(profile_ids).add(profile_id)
        profile_ids = list(profile_ids)
    else:
        profile_ids = [profile_id]

    _duplicated_profiles = await profile_db.load_profile_duplicates(profile_ids)  # 1st records is the newest
    valid_profile_record = _duplicated_profiles.first()  # type: StorageRecord
    if valid_profile_record is None:
        raise ValueError("Could not fetch first profile. Probably already merged.")
    first_profile = valid_profile_record.to_entity(Profile)

    if len(_duplicated_profiles) == 1:
        if first_profile.metadata.system.has_merging_data():
            first_profile.metadata.system.remove_merging_data()
            first_profile.mark_for_update()
            await save_profile(first_profile, refresh=True)

        # If 1 then there is no duplication
        return first_profile

    # Create empty profile where we will merge duplicates
    profile = Profile.new()
    profile.set_meta_data(RecordMetadata(id=profile_id, index=profile.get_meta_data().index))

    similar_profiles = []
    for _profile_record in _duplicated_profiles:
        similar_profiles.append(_profile_record.to_entity(Profile))

    # Merged profiles refresh index
    return await ProfileMerger(profile).compute_one_profile(similar_profiles)


async def merge_profiles_by_id(profile: Profile) -> Optional[Profile]:

    print(profile.id)
    duplicated_profiles = await load_duplicated_profiles_for_profile(profile)
    print(len(duplicated_profiles))

    profiles = [FlatProfile(profile_record) for profile_record in duplicated_profiles]

    print(len(profiles))
    merged, changed_fields = merge_profiles(profiles)

    print(changed_fields)

    merged.update_changed_fields(changed_fields)
    merged.mark_as_merged()

    profile_ids = merged.get('ids', [])

    print([profile_record['id'] for profile_record in profiles if 'a8a92e68-0c7f-497a-a4bb-5f441925f0f2' in profile_record['ids']])

    print(profile_ids)
    profile_id = merged.get("id", None)
    await move_profile_events_and_sessions(duplicate_profile_ids=profile_ids, merged_profile_id=profile_id)

    print(merged['id'])
    return Profile(**merged)


async def compute_one_profile_in_db(profile: Profile) -> Optional[Profile]:
    pass
