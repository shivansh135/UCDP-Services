from tracardi.domain.resource import Resource
from tracardi.service.storage.mysql.mapping.resource_mapping import map_to_resource
from tracardi.service.storage.mysql.service.resource_service import ResourceService

rs = ResourceService()


async def save_record(resource: Resource):
    return await rs.insert(resource)

async def load(id: str) -> Resource:

    resource = (await rs.load_by_id(id)).map_to_object(map_to_resource)

    if resource is None:
        raise ValueError(f'Resource id {id} does not exist.')

    if not resource.enabled:
        raise ValueError(f'Resource id {id} disabled.')

    return resource
