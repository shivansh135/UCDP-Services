from typing import List

from tracardi.service.storage.driver.elastic.raw import update_profile_ids
from tracardi.service.storage.driver.elastic import event as event_db
from tracardi.service.storage.driver.elastic import session as session_db


async def move_profile_events_and_sessions(duplicate_profile_ids: List[str], merged_profile_id: str):

    # Changes ids of old events and sessions to match merged profile
    for old_id in duplicate_profile_ids:
        if old_id != merged_profile_id:
            print(old_id, merged_profile_id)
            await update_profile_ids('event', old_id, merged_profile_id)
            await event_db.refresh()
            await update_profile_ids('session', old_id, merged_profile_id)
            await session_db.refresh()
