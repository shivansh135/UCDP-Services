import uuid
from datetime import datetime

from tracardi.service.wf.domain.flow_graph import FlowGraph
from .named_entity import NamedEntityInContext
from typing import Optional, List, Any
from pydantic import BaseModel
from tracardi.service.wf.domain.flow_graph_data import FlowGraphData, EdgeBundle
from tracardi.service.plugin.domain.register import MetaData, Plugin, Spec, NodeEvents, MicroserviceConfig

from ..config import tracardi
from ..exceptions.log_handler import get_logger
from ..service.secrets import decrypt, encrypt, b64_encoder, b64_decoder

from ..service.utils.date import now_in_utc

logger = get_logger(__name__)


class FlowSchema(BaseModel):
    version: str = tracardi.version.version
    uri: str = 'http://www.tracardi.com/2021/WFSchema'
    server_version: str = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.server_version = tracardi.version.version


class Flow(FlowGraph):
    tags: Optional[List[str]] = ["General"]
    lock: bool = False
    type: str
    timestamp: Optional[datetime] = None
    deploy_timestamp: Optional[datetime] = None
    file_name: Optional[str] = None
    wf_schema: FlowSchema = FlowSchema()

    def arrange_nodes(self):
        if self.flowGraph is not None:
            targets = {edge.target for edge in self.flowGraph.edges}
            starting_nodes = [node for node in self.flowGraph.nodes if node.id not in targets]

            start_at = [0, 0]
            for starting_node in starting_nodes:
                node_to_distance_map = self.flowGraph.traverse_graph_for_distances(start_at_id=starting_node.id)

                for node_id in node_to_distance_map:
                    node = self.flowGraph.get_node_by_id(node_id)
                    node.position.y = start_at[1] + 150 * node_to_distance_map[node_id]

                distance_to_nodes_map = {}
                for node_id in node_to_distance_map:
                    if node_to_distance_map[node_id] not in distance_to_nodes_map:
                        distance_to_nodes_map[node_to_distance_map[node_id]] = []
                    distance_to_nodes_map[node_to_distance_map[node_id]].append(node_id)

                for node_ids in distance_to_nodes_map.values():
                    nodes = [self.flowGraph.get_node_by_id(node_id) for node_id in node_ids]
                    row_center = start_at[0] - 200 * len(nodes) + 250
                    for node in nodes:
                        node.position.x = row_center - node.data.metadata.width // 2
                        row_center += node.data.metadata.width

                start_at[0] += len(max(distance_to_nodes_map.values(), key=len)) * 200

    def get_empty_workflow_record(self, type: str) -> 'FlowRecord':

        return FlowRecord(
            id=self.id,
            timestamp=now_in_utc(),
            description=self.description,
            name=self.name,
            tags=self.tags,
            lock=self.lock,
            type=type
        )

    @staticmethod
    def from_workflow_record(record: 'FlowRecord') -> Optional['Flow']:

        if 'type' not in record.draft:
            record.draft['type'] = record.type

        flow = Flow(**record.draft)
        flow.deploy_timestamp = record.deploy_timestamp
        flow.timestamp = record.timestamp
        flow.file_name = record.file_name
        flow.id = record.id

        if not flow.timestamp:
            flow.timestamp = now_in_utc()

        return flow

    @staticmethod
    def new(id: str = None) -> 'Flow':
        return Flow(
            id=str(uuid.uuid4()) if id is None else id,
            timestamp=now_in_utc(),
            name="Empty",
            wf_schema=FlowSchema(version=str(tracardi.version)),
            flowGraph=FlowGraphData(nodes=[], edges=[]),
            type='collection'
        )

    @staticmethod
    def build(name: str, description: str = None, id=None, lock=False, tags=None, type='collection') -> 'Flow':
        if tags is None:
            tags = ["General"]

        return Flow(
            id=str(uuid.uuid4()) if id is None else id,
            timestamp=now_in_utc(),
            name=name,
            wf_schema=FlowSchema(version=str(tracardi.version)),
            description=description,
            tags=tags,
            lock=lock,
            flowGraph=FlowGraphData(
                nodes=[],
                edges=[]
            ),
            type=type
        )

    def __add__(self, edge_bundle: EdgeBundle):
        if edge_bundle.source not in self.flowGraph.nodes:
            self.flowGraph.nodes.append(edge_bundle.source)
        if edge_bundle.target not in self.flowGraph.nodes:
            self.flowGraph.nodes.append(edge_bundle.target)

        if edge_bundle.edge not in self.flowGraph.edges:
            self.flowGraph.edges.append(edge_bundle.edge)
        else:
            logger.warning("Edge {}->{} already exists".format(edge_bundle.edge.source, edge_bundle.edge.target))

        return self


class SpecRecord(BaseModel):
    id: str
    className: str
    module: str
    inputs: Optional[List[str]] = []
    outputs: Optional[List[str]] = []
    init: Optional[str] = ""
    microservice: Optional[MicroserviceConfig] = None
    node: Optional[NodeEvents] = None
    form: Optional[str] = ""
    manual: Optional[str] = None
    author: Optional[str] = None
    license: Optional[str] = "MIT"
    version: Optional[str] = '1.0.x'

    @staticmethod
    def encode(spec: Spec) -> 'SpecRecord':
        return SpecRecord(
            id=spec.id,
            className=spec.className,
            module=spec.module,
            inputs=spec.inputs,
            outputs=spec.outputs,
            init=encrypt(spec.init),
            microservice=spec.microservice,
            node=spec.node,
            form=b64_encoder(spec.form),
            manual=spec.manual,
            author=spec.author,
            license=spec.license,
            version=spec.version
        )

    def decode(self) -> Spec:
        return Spec(
            id=self.id,
            className=self.className,
            module=self.module,
            inputs=self.inputs,
            outputs=self.outputs,
            init=decrypt(self.init),
            microservice=self.microservice,
            node=self.node,
            form=b64_decoder(self.form),
            manual=self.manual,
            author=self.author,
            license=self.license,
            version=self.version
        )


class MetaDataRecord(BaseModel):
    name: str
    brand: Optional[str] = ""
    desc: Optional[str] = ""
    keywords: Optional[List[str]] = []
    type: str = 'flowNode'
    width: int = 300
    height: int = 100
    icon: str = 'plugin'
    documentation: Optional[str] = ""
    group: Optional[List[str]] = ["General"]
    tags: List[str] = []
    pro: bool = False
    commercial: bool = False
    remote: bool = False
    frontend: bool = False
    emits_event: Optional[str] = ""
    purpose: List[str] = ['collection']

    @staticmethod
    def encode(metadata: MetaData) -> 'MetaDataRecord':
        return MetaDataRecord(
            name=metadata.name,
            brand=metadata.brand,
            desc=metadata.desc,
            keywords=metadata.keywords,
            type=metadata.type,
            width=metadata.width,
            height=metadata.height,
            icon=metadata.icon,
            documentation=b64_encoder(metadata.documentation),
            group=metadata.group,
            tags=metadata.tags,
            pro=metadata.pro,
            commercial=metadata.commercial,
            remote=metadata.remote,
            frontend=metadata.frontend,
            emits_event=b64_encoder(metadata.emits_event),
            purpose=metadata.purpose
        )

    def decode(self) -> MetaData:
        return MetaData(
            name=self.name,
            brand=self.brand,
            desc=self.desc,
            keywords=self.keywords,
            type=self.type,
            width=self.width,
            height=self.height,
            icon=self.icon,
            documentation=b64_decoder(self.documentation),
            group=self.group,
            tags=self.tags,
            pro=self.pro,
            commercial=self.commercial,
            remote=self.remote,
            frontend=self.frontend,
            emits_event=b64_decoder(self.emits_event),
            purpose=self.purpose
        )


class PluginRecord(BaseModel):
    start: bool = False
    debug: bool = False
    spec: SpecRecord
    metadata: MetaDataRecord

    @staticmethod
    def encode(plugin: Plugin) -> 'PluginRecord':
        return PluginRecord(
            start=plugin.start,
            debug=plugin.debug,
            spec=SpecRecord.encode(plugin.spec),
            metadata=MetaDataRecord.encode(plugin.metadata)
        )

    def decode(self) -> Plugin:
        data = {
            "start": self.start,
            "debug": self.debug,
            "spec": self.spec.decode(),
            "metadata": self.metadata.decode()
        }
        return Plugin.model_construct(_fields_set=self.model_fields_set, **data)


class FlowRecord(NamedEntityInContext):
    timestamp: Optional[datetime] = None
    deploy_timestamp: Optional[datetime] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = ["General"]
    file_name: Optional[str] = None
    draft: Optional[dict] = {}
    lock: bool = False
    type: str

    def get_empty_workflow(self, id) -> 'Flow':
        return Flow.build(id=id,
                          name=self.name,
                          description=self.description,
                          tags=self.tags,
                          lock=self.lock)
