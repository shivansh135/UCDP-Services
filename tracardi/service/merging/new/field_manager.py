from collections import namedtuple
from time import time

from pydantic import BaseModel
from uuid import uuid4

from typing import List, Tuple, Dict, Set, Generator, Optional, Union

from datetime import datetime

from tracardi.domain.profile import FlatProfile
from tracardi.domain.system_entity_property import SystemEntityProperty
from tracardi.exceptions.log_handler import get_logger
from tracardi.process_engine.tql.utils.dictonary import flatten
from tracardi.service.merging.new.field_metadata import FieldMetaData
from tracardi.service.merging.new.profile_metadata import ProfileMetaData
from tracardi.service.merging.new.value_timestamp import ValueTimestamp, ProfileValueTimestamp

logger = get_logger(__name__)

ValueAndTimestamp = namedtuple('ValueAndTimestamp', ['profile', 'value', 'timestamp'])


class ProfileTimestamps(BaseModel):
    insert: Optional[datetime]
    update: Optional[datetime]
    field: Dict[str, Optional[Union[datetime, float, str]]]


def split_flat_profile_to_data_and_timestamps(profile: FlatProfile) -> Tuple[FlatProfile, ProfileTimestamps]:
    return profile.copy(), ProfileTimestamps(
        insert=profile.get('metadata.time.insert', None),
        update=profile.get('metadata.time.update', None),
        field={field: item[0] for field, item in profile.get('metadata.fields', {}).items()}
    )

def index_fields(field_settings, path) -> Dict[str, SystemEntityProperty]:
    return {field.property: field for field in field_settings if field.path == path}


def _get_nested(nested_property_settings: List[SystemEntityProperty], field) -> Optional[
    SystemEntityProperty]:
    for nested_property in nested_property_settings:
        if field.startswith(nested_property.property):
            return nested_property
    return None


def get_profile_field_settings(indexed_properties_settings: Dict[str, SystemEntityProperty],
                               profile: FlatProfile,
                               default_strategies: List[str],
                               path,
                               skip_values
                               ) -> Generator[SystemEntityProperty, None, None]:

    """
    Returns setting for all profile fields, if not set then default setting is returned
    """

    profile_fields = flatten(profile.to_dict()).keys()

    # Filter path
    nested_property_settings = [item for item in indexed_properties_settings.values() if item.nested]

    for field in profile_fields:

        # Skip timestamps
        ignored_value = ['metadata.fields'] + skip_values
        if field.startswith(tuple(ignored_value)):
            continue

        # Field exits in settings
        if field in indexed_properties_settings:
            yield indexed_properties_settings[field]

        else:
            # Field is nested
            nested_property = _get_nested(nested_property_settings, field)
            if nested_property:
                yield nested_property
            else:
                # Not defined
                yield SystemEntityProperty(
                    id=str(uuid4()),
                    entity='profile',
                    path=path,
                    property=field,
                    type='unknown',
                    merge_strategies=default_strategies
                )


class ProfileDataSpliter:

    def __init__(self, profiles, indexed_field_settings, default_strategies, path, skip_values):
        self._profiles = profiles
        self._skip_values = skip_values
        self.path = path
        self.default_strategies = default_strategies
        self.indexed_field_settings = indexed_field_settings

    def _get_fields_and_timestamps(self, indexed_properties_settings: Dict[str, SystemEntityProperty],
                                   default_strategies, path: str = "") -> Tuple[
        Set[SystemEntityProperty], Dict[str, ProfileTimestamps]]:

        set_of_field_settings = set()
        profile_id_to_timestamps = {}
        for profile in self._profiles:
            profile, profile_timestamps = split_flat_profile_to_data_and_timestamps(profile)
            profile_id_to_timestamps[str(profile['id'])] = profile_timestamps
            # Get all setting for fields
            for field_setting in get_profile_field_settings(
                    indexed_properties_settings,
                    profile,
                    default_strategies,
                    path,
                    self._skip_values
            ):
                set_of_field_settings.add(field_setting)
        return set_of_field_settings, profile_id_to_timestamps

    def split(self):
        return self._get_fields_and_timestamps(
            self.indexed_field_settings,
            self.default_strategies,
            self.path)


class FieldManager:

    def __init__(self, profiles: List[FlatProfile],
                 merged_profile_field_settings: Set[SystemEntityProperty],
                 profile_id_to_timestamps: Dict[str, ProfileTimestamps],
                 default_strategies, path="",skip_fields=None):

        self.path = path
        self.default_strategies = default_strategies
        self._profiles = profiles
        self._skip_values = skip_fields if skip_fields is not None else []

        self.merged_profile_field_settings = merged_profile_field_settings
        self.profile_id_to_timestamps = profile_id_to_timestamps


    def _get_value_timestamps(self, field, profile_id_to_timestamps: Dict[str, ProfileTimestamps]) -> Generator[ProfileValueTimestamp, None, None]:
        for profile in self._profiles:
            profile_timestamps = profile_id_to_timestamps.get(profile['id'], None)
            if profile_timestamps is None:
                print("--1", profile['id'], field, profile_id_to_timestamps)
                exit()
            yield ProfileValueTimestamp(
                    id=profile.get('id', None),
                    value=profile.get(field),
                    timestamp=profile_timestamps.field.get(field, None),
                    profile_update=profile_timestamps.update,
                    profile_insert=profile_timestamps.insert
                )


    def get_profiles_metadata(self) -> ProfileMetaData:

        fields_to_merge: Set[FieldMetaData] = set()
        for field_setting in self.merged_profile_field_settings:
            field = field_setting.property
            fields_to_merge.add(
                FieldMetaData(
                    type=field_setting.type,
                    path=field_setting.path,
                    field=field,
                    values=list(self._get_value_timestamps(field, self.profile_id_to_timestamps)),
                    strategies=field_setting.merge_strategies,
                    nested=field_setting.nested
                )
            )
        return ProfileMetaData(
            profiles=self._profiles,
            fields_metadata=fields_to_merge,
            default_strategies=self.default_strategies
        )


    def merge(self, path) -> Tuple[FlatProfile, dict]:
        merged_profile = FlatProfile({})
        changed_fields = {}

        profile_metadata = self.get_profiles_metadata()
        timestamp = time()
        for field_meta, merged_value in profile_metadata.merge():
            # print(field_meta.field, merged_value.value, type(merged_value.value), merged_value.strategy_id)
            merged_profile[field_meta.field] = merged_value.value
            # FOr nested fields we get the changed values
            if merged_value.changed_fields:
                changed_fields.update(merged_value.changed_fields)
            else:
                changed_fields[field_meta.field] = [timestamp, 'merge']


        if path:
            return merged_profile.get(path, {}), changed_fields
        return  merged_profile, changed_fields