# from uuid import uuid4

from tracardi.domain.system_entity_property import SystemEntityProperty

default_session_properties = [
    SystemEntityProperty(entity='session', id='f58fc676-93c8-43df-8a56-2a3661f5230f', property='id', type='str', optional=False, default=None),
    SystemEntityProperty(entity='session', id='ba2291f1-dd07-457d-930b-716ed8d8f52e', property='metadata.time.insert', type='datetime', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='00d3bf6c-de74-4ebe-8c7d-036108b20f72', property='metadata.time.create', type='datetime', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='ff12f4bd-1894-4d90-9d82-6444c93a9151', property='metadata.time.update', type='datetime', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='45c471b3-5054-42fe-943e-cd4e444d02d6', property='metadata.time.timestamp', type='float', optional=True, default=None),
    SystemEntityProperty(entity='session', id='fff9bc1b-0cf3-472d-b8c3-843f7f32ed68', property='metadata.time.duration', type='float', optional=False, default='0'),
    SystemEntityProperty(entity='session', id='398553d1-b15c-4074-ad69-661a74a462e8', property='metadata.time.weekday', type='int', optional=True, default=None),
    SystemEntityProperty(entity='session', id='c45083c2-69ef-4c7c-9a27-c30235407a5c', property='metadata.channel', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='e0e856d2-7916-46e1-90f6-a95070396d40', property='metadata.aux', type='dict', optional=True, default='{}'),
    SystemEntityProperty(entity='session', id='69163e08-812b-4fad-be2d-c98cb1d98ac3', property='metadata.status', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='df391730-6120-4fcc-8a8c-a1636641e75f', property='operation.new', type='bool', optional=False, default='False'),
    SystemEntityProperty(entity='session', id='3bb2e4bd-b062-47ae-8514-53b60dc7d1c3', property='operation.update', type='bool', optional=False, default='False'),
    SystemEntityProperty(entity='session', id='0cf07445-bad1-420d-bf13-597ec4db57fd', property='profile.id', type='str', optional=True, default=None),
    SystemEntityProperty(entity='session', id='fc53cd40-6b77-4c11-9c9f-ebd55310b677', property='device.name', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='d5350646-feab-4cca-8753-aef0e8039a3b', property='device.brand', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='802d55af-ca04-46ae-85b2-6de16ca0fb19', property='device.model', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='0a1c120f-f290-4620-9e9c-6739d20cbf3a', property='device.type', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='0bf4cf6b-79fd-4441-bd88-c7168b4a6fdb', property='os.name', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='16d5b4e3-dd9e-4167-a2e1-3751a419c74b', property='os.version', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='d7657443-cd2e-4e2a-9d0e-9a82d8777907', property='app.type', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='bdd843c4-2614-49cf-9d2e-2e8dc9871d98', property='app.name', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='89cdfd3f-4e98-4b74-96ae-51943f414496', property='app.version', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='f73bc735-b3a2-4efa-8ab8-fa81b2202fc9', property='utm.source', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='9c118642-f610-462f-80d5-13b9a47b2529', property='utm.medium', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='c5f13c80-4a67-4a96-9982-8d28d801b573', property='utm.campaign', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='285da13b-46fb-4891-8b40-b0a45b4fa17b', property='utm.term', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='b5466f62-0621-461d-b070-e72d82a55925', property='utm.content', type='str', optional=True, default='NULL'),
    SystemEntityProperty(entity='session', id='435e6ead-10bf-4493-8ffe-75fde517c991', property='context', type='SessionContext', optional=True, default='{}'),
    SystemEntityProperty(entity='session', id='c2b454ef-79b9-412d-95a1-0fd72765099b', property='properties', type='dict', optional=True, default='{}'),
    SystemEntityProperty(entity='session', id='b1a30fb1-b982-409d-b464-757da77d71a5', property='traits', type='dict', optional=True, default='{}'),
    SystemEntityProperty(entity='session', id='1dc12332-36f0-469d-bb2e-83af193da80f', property='aux', type='dict', optional=True, default='{}'),
]


# for x in default_session_properties:
#     print(
# f"""SystemEntityProperty(entity='session', id='{uuid4()}', property='{x.id}', type='{x.type}', optional={x.optional}, default={f"'{x.default}'" if x.default is not None else None}),""")