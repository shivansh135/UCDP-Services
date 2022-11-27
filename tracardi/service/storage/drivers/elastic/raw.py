from tracardi.domain.storage.index_mapping import IndexMapping
from tracardi.domain.storage_record import StorageRecords
from tracardi.service.storage.elastic_client import ElasticClient
from tracardi.service.storage.factory import storage_manager
from tracardi.service.storage.persistence_service import PersistenceService


def index(idx) -> PersistenceService:
    return storage_manager(idx)

#
# def entity(entity: Entity) -> StorageCrud:
#     return StorageFor(entity).index()


# def collection(index, dataset: List) -> CollectionCrud:
#     return StorageForBulk(dataset).index(index)


async def query(index: str, query: dict) -> StorageRecords:
    return await storage_manager(index).query(query)


async def update_profile_ids(index: str, old_profile_id: str, merged_profile_id):
    query = {
        "script": {
            "source": f"ctx._source.profile.id = '{merged_profile_id}'",
            "lang": "painless"
        },
        "query": {
            "term": {
                "profile.id": old_profile_id
            }
        }
    }

    return await storage_manager(index=index).update_by_query(query=query)


async def count_by_query(index: str, query: str, time_span: int) -> StorageRecords:
    result = await storage_manager(index).storage.count_by_query_string(
        query,
        f"{time_span}s" if time_span < 0 else ""
    )
    return result


async def indices():
    es = ElasticClient.instance()
    return await es.list_indices()


async def health():
    es = ElasticClient.instance()
    return await es.cluster.health()


async def aliases():
    es = ElasticClient.instance()
    return await es.list_aliases()


async def clone(source_index, destination_index):
    es = ElasticClient.instance()
    return await es.clone(source_index, destination_index)


async def exists_index(index):
    es = ElasticClient.instance()
    return await es.exists_index(index)


async def exists_alias(alias, index):
    es = ElasticClient.instance()
    return await es.exists_alias(alias, index=index)


async def reindex(source, destination, wait_for_completion=True):
    es = ElasticClient.instance()
    return await es.reindex(source, destination, wait_for_completion)


async def task_status(task_id):
    es = ElasticClient.instance()
    return await es._client.tasks.get(task_id)


async def refresh(index):
    es = ElasticClient.instance()
    return await es.refresh(index)


async def mapping(index) -> IndexMapping:
    return await storage_manager(index).get_mapping()
