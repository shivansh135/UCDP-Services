from typing import Optional, List, Tuple

import logging

from app.api.domain.user_payload import UserPayload
from tracardi.config import tracardi
from tracardi.domain.user import User
from tracardi.exceptions.log_handler import log_handler
from tracardi.service.storage.mysql.mapping.user_mapping import map_to_user_table, map_to_user
from tracardi.service.storage.mysql.schema.table import UserTable
from tracardi.service.storage.mysql.service.table_service import TableService, where_tenant_context, sql_functions
from tracardi.service.storage.mysql.utils.select_result import SelectResult
from tracardi.service.sha1_hasher import SHA1Encoder

logger = logging.getLogger(__name__)
logger.setLevel(tracardi.logging_level)
logger.addHandler(log_handler)


class UserService(TableService):


    async def load_all(self, limit:int = None, offset:int = None) -> SelectResult:
        return await self._load_all(UserTable, limit, offset)

    async def load_by_id(self, user_id: str) -> SelectResult:
        return await self._load_by_id(UserTable, primary_id=user_id)

    async def delete_by_id(self, user_id: str) -> str:
        return await self._delete_by_id(UserTable, primary_id=user_id)

    async def upsert(self, user: User):
        return await self._replace(UserTable, map_to_user_table(user))


    # Custom

    async def load_by_credentials(self, email: str, password: str) -> Optional[User]:
        where = where_tenant_context(
            UserTable,
            UserTable.email == email,
            UserTable.password == User.encode_password(password),
            UserTable.disabled == False
        )

        records = await self._query(UserTable, where)

        if not records.exists():
            return None

        return records.map_first_to_object(map_to_user)


    async def load_by_role(self, role: str) -> List[User]:
        where = where_tenant_context(
            UserTable,
            sql_functions().find_in_set(role, UserTable.roles) > 0
        )

        records = await self._query(UserTable, where)

        if not records.exists():
            return []

        return list(records.map_to_objects(map_to_user))

    async def load_by_name(self, name: str, start:int, limit: int) -> List[User]:
        where = where_tenant_context(
            UserTable,
            UserTable.full_name.like(name)
        )

        records = await self._query(UserTable, where, limit=start, offset=limit)

        users: List[User] = list(records.map_to_objects(map_to_user))

        return users

    async def check_if_exists(self, email: str) -> bool:
        where = where_tenant_context(
            UserTable,
            UserTable.email == email
        )

        records = await self._query(UserTable, where)

        return records.exists()

    async def insert_if_none(self, user: User) -> Optional[str]:
        if self.check_if_exists(user.email):
            return await self._insert_if_none(UserTable, map_to_user_table(user))
        return None

    async def update_if_exist(self, user_id:str, user_payload: UserPayload) -> Tuple[bool, User]:

        user_record = await self.load_by_id(user_id)

        if not user_record.exists():
            raise LookupError(f"User does not exist {user_payload.email}")

        existing_user: User = user_record.map_to_object(map_to_user)

        user = User(
            id=user_id,
            password=User.encode_password(user_payload.password) if user_payload.password is not None else existing_user.password,
            full_name=user_payload.full_name if user_payload.full_name is not None else existing_user.full_name,
            email=user_payload.email if user_payload.email is not None else existing_user.email,
            roles=user_payload.roles if user_payload.roles is not None else existing_user.roles,
            disabled=user_payload.disabled if user_payload.disabled is not None else existing_user.disabled,
            preference=existing_user.preference,
            expiration_timestamp=existing_user.expiration_timestamp
        )

        result = await self._replace(UserTable, map_to_user_table(user))

        return result is not None, user