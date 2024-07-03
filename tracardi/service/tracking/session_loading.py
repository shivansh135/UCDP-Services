from typing import Tuple

from tracardi.domain.entity import Entity
from tracardi.exceptions.log_handler import get_logger
from tracardi.service.tracking.storage.session_storage import load_session
from tracardi.domain.payload.tracker_payload import TrackerPayload
from tracardi.domain.session import Session
from tracardi.service.utils.getters import get_entity_id


logger = get_logger(__name__)

def _new_session(tracker_payload: TrackerPayload) -> Tuple[Session, TrackerPayload]:
    # Artificial session

    # If no session in tracker payload this means that we do not need session.
    # But we may need an artificial session for workflow handling. We create
    # one but will not save it.

    session = tracker_payload.create_default_session()

    assert (session.operation.new is True)

    if isinstance(tracker_payload.profile, Entity):
        session.profile = Entity(id=tracker_payload.profile.id)

    # Set session to tracker payload

    tracker_payload.force_session(session)

    return session, tracker_payload


async def load_or_create_session(tracker_payload: TrackerPayload) -> Tuple[Session, TrackerPayload]:

    session_id = get_entity_id(tracker_payload.session)

    if session_id is None or session_id.strip() == "":

        # Set session to tracker payload

        session, tracker_payload = _new_session(tracker_payload)

    else:

        # Loads session from ES
        session = await load_session(session_id)

        if session is None:
            session, tracker_payload = _new_session(tracker_payload)

        if session.profile is None or not session.profile.id:  # If session profile is none then it is corrupted
            logger.warning(f"Session {session_id} has no profile and is corrupted.")
            session, tracker_payload = _new_session(tracker_payload)

    return session, tracker_payload
