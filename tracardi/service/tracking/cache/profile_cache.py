from typing import Optional, List

from tracardi.context import get_context, Context
from tracardi.domain.storage_record import RecordMetadata
from tracardi.exceptions.log_handler import get_logger
from tracardi.service.storage.redis.cache import RedisCache
from tracardi.service.storage.redis.collections import Collection
from tracardi.service.storage.redis_client import RedisClient
from tracardi.service.tracking.cache.prefix import get_cache_prefix
from tracardi.domain.profile import Profile

logger = get_logger(__name__)
redis_cache = RedisCache(ttl=None)
_redis = RedisClient()


def get_profile_key_namespace(profile_id, context):
    return f"{Collection.profile}{context.context_abrv()}:{get_cache_prefix(profile_id[0:2])}:"

def delete_profile_cache(profile_id: str, context: Context):
    key_namespace = get_profile_key_namespace(profile_id, context)
    redis_cache.delete(
        profile_id,
        key_namespace
    )


def load_profile_cache(profile_id: str, context: Context) -> Optional[Profile]:

    key_namespace = get_profile_key_namespace(profile_id, context)

    if not redis_cache.has(profile_id, key_namespace):
        return None

    _data = redis_cache.get(
        profile_id,
        key_namespace
    )

    try:
        context, profile, profile_changes, profile_metadata = _data
    except Exception:
        return None

    profile = Profile(**profile)
    if profile_metadata:
        profile.set_meta_data(RecordMetadata(**profile_metadata))

    return profile


def save_profile_cache(profile: Optional[Profile]):
    if profile:

        context = get_context()
        key = get_profile_key_namespace(profile.id, context)

        index = profile.get_meta_data()

        try:
            if index is None:
                raise ValueError("Empty profile index.")

            value = (
                    {
                        "production": context.production,
                        "tenant": context.tenant
                    },
                    profile.model_dump(mode="json", exclude_defaults=True,  exclude={"operation": ...}),
                    None,
                    index.model_dump(mode="json")
                )

            redis_cache.set(
                profile.id,
                value,
                key
            )

        except ValueError as e:
            logger.error(f"Saving to cache failed: Detail: {str(e)}")




def save_profiles_in_cache(profiles: List[Profile]):
    context = get_context()
    if profiles:

        for profile in profiles:
            if profile:
                value = (
                    {
                        "production": context.production,
                        "tenant": context.tenant
                    },
                    profile.model_dump(mode="json", exclude_defaults=True),
                    profile.get_meta_data().model_dump() if profile.has_meta_data() else None
                )

                collection = get_profile_key_namespace(profile.id, context)

                redis_cache.set(profile.id, value, collection)
