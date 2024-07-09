from tracardi.service.storage.elastic_client import ElasticClient


async def elastic_close():
    elastic = ElasticClient.instance()
    await elastic.close()