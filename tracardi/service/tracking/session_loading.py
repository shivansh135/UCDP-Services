from typing import Tuple

from tracardi.exceptions.log_handler import get_logger
from tracardi.service.tracking.storage.session_storage import load_session
from tracardi.domain.payload.tracker_payload import TrackerPayload
from tracardi.domain.session import Session
from tracardi.service.utils.getters import get_entity_id

logger = get_logger(__name__)


async def load_or_create_session(tracker_payload: TrackerPayload) -> Tuple[Session, TrackerPayload]:
    session_id = get_entity_id(tracker_payload.session)

    if session_id is None or session_id.strip() == "":

        # Set session to tracker payload. New id created as there is none in session_id

        session = tracker_payload.create_session()

    else:

        # Loads session from ES
        session = await load_session(session_id)

        if session is None:
            # Creates session with delivered session id
            session = tracker_payload.create_session()

        if session.profile is None or not session.profile.id:  # If session profile is none then it is corrupted
            logger.warning(f"Session {session_id} has no profile and is corrupted.")
            session = tracker_payload.create_session()

    return session, tracker_payload
