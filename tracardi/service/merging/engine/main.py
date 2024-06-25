import time

from datetime import datetime

import asyncio
from tracardi.domain.profile import ConsentRevoke, Profile, FlatProfile
from tracardi.domain.storage_record import RecordMetadata
from tracardi.service.merging.engine.merger import merge_profiles



async def main():
    profile1 = FlatProfile({
        'id': "1",
        'active': True,
        "metadata": {
            "time": {
                "insert": "2025-01-10T17:13:28.620880+00:00",
                "create": "2004-01-12T17:13:28.620880+00:00",
                "update": "2025-03-20T10:53:41.924819+00:00",
                "segmentation": None,
                "visit": {
                    "last": "2023-03-18T17:13:28.655439+00:00",
                    "current": "2028-01-20T10:23:52.968274+00:00",
                    "count": 2,
                    "tz": "Europe/Warsaw"
                }
            },
            "aux": {},
            "status": None,
            "fields": {
                # "traits.a": [
                #     "2024-05-20 10:53:41.923018+00:00",
                #     "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                # ],
                "consents.marketing": [
                    "2024-05-20 10:53:41.923037+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "consents.ident": [
                    "2024-05-20 10:53:41.923044+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "aux.isProfessionalQuestion": [
                    "2024-05-20 10:53:41.923050+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "metadata.time.visit.tz": [
                    "2024-05-21 13:33:41.923050+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "data.job.type": [
                    "2024-05-21 13:33:41.923050+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
            },
            "system": {
                "integrations": {},
                "aux": {
                    "a": 1
                }
            }
        },
        "data": {"anonymous": "TRUE", "pii": {"language": {"spoken": ['polish']}},

                 "job": {
                     "type": ""
                 },
            },
        # 'traits': {
        #   "a": 1,
        #   "b": 0
        # },
        'consents': {
            'marketing': ConsentRevoke(revoke=None),
            'ident': ConsentRevoke(revoke=datetime.now())
        },
    })

    profile2 = FlatProfile({
        'id': "2",
        'active': False,
        "metadata": {
            "time": {
                "insert": "2014-05-18T17:13:28.620880+00:00",
                "create": "2024-05-18T17:13:28.620880+00:00",
                "update": "2026-05-20T10:53:41.924819+00:00",
                "segmentation": None,
                "visit": {
                    "last": "2027-05-18T17:13:28.655439+00:00",
                    "current": "2024-05-20T10:23:52.968274+00:00",
                    "count": 2,
                    "tz": "Europe/Berlin"
                }
            },
            "aux": {},
            "status": None,
            "fields": {
                # "traits.a": [
                #     "2024-05-20 10:53:41.923018+00:00",
                #     "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                # ],
                # "traits.b": [
                #     "2024-05-20 10:53:41.923018+00:00",
                #     "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                # ],
                "consent.marketing": [
                    "2024-05-20 11:53:41.923037+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "consent.ident": [
                    "2024-05-20 11:53:41.923044+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "consent.email": [
                    "2024-05-20 11:53:42.923050+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "data.job.type": [
                    "2024-05-21 13:33:41.923050+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
            },
            "system": {
                "integrations": {},
                "aux": {
                    "a": 1
                }
            }
        },
        # 'traits': {
        #     "a": 2,
        #     "b": -11
        # },
        'consents': {
            'marketing': ConsentRevoke(revoke=datetime.now()),
            'ident': ConsentRevoke(revoke=datetime.now()),
            'email': ConsentRevoke(revoke=None),
        }
    })

    profile3 = FlatProfile({
        'id': "3",
        'active': True,
        "metadata": {
            "time": {
                "insert": "2020-05-18T17:13:28.620880+00:00",
                "create": "2024-05-18T17:13:28.620880+00:00",
                "update": "2024-05-20T10:53:41.924819+00:00",
                "segmentation": None,
                "visit": {
                    "last": "2024-05-18T17:13:28.655439+00:00",
                    "current": "2024-05-20T10:23:52.968274+00:00",
                    "count": 3,
                    "tz": "Europe/Berlin"
                }
            },
            "aux": {},
            "status": None,
            "fields": {
                # "traits.a": [
                #     "2024-05-20 12:53:41.923018+00:00",
                #     "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                # ],
                # "traits.c": [
                #     "2024-05-20 12:53:41.923018+00:00",
                #     "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                # ],
                "system.aux.a": [
                    "2024-05-20 12:53:41.923018+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
                "data.job.type": [
                    "2024-05-20 13:33:41.923050+00:00",
                    "19fc6fa0-11f1-4737-bb33-d8e140ee54b2"
                ],
            },
            "system": {
                "integrations": {},
                "aux": {
                    "a": 1
                }
            }
        },
        "data": {
            "job": {
                "type": "job2",
                "salary": 10,
                "company": {
                    "name": "string",
                    "size": 100,
                    "country": "string",
                    "segment": "string"
                },
                "position": "string",
                "department": "string"
            },
            "pii": {
                "civil": {
                    "status": "string"
                },
                "gender": "string",
                "birthday": "2010-01-01 00:00:00",
                "language": {
                    "native": "string",
                    "spoken": [
                        "string"
                    ]
                },
                "lastname": "string",
                "education": {
                    "level": "string"
                },
                "firstname": "string",
                "attributes": {
                    "height": 0,
                    "weight": 0,
                    "shoe_number": 0
                },
                "display_name": "string"
            },
            "media": {
                "image": "string",
                "social": {
                    "reddit": "string",
                    "tiktok": "string",
                    "twitter": "string",
                    "youtube": "string",
                    "facebook": "string",
                    "linkedin": "string",
                    "instagram": "string"
                },
                "webpage": "string"
            },
            "contact": {
                "app": {
                    "slack": "string",
                    "viber": "string",
                    "signal": "string",
                    "wechat": "string",
                    "discord": "string",
                    "twitter": "string",
                    "telegram": "string",
                    "whatsapp": "string"
                },
                "email": {
                    "main": "string",
                    "private": "string",
                    "business": "string"
                },
                "phone": {
                    "main": "string",
                    "mobile": "string",
                    "business": "string",
                    "whatsapp": "string"
                },
                "address": {
                    "town": "string",
                    "other": "string",
                    "county": "string",
                    "street": "string",
                    "country": "string",
                    "postcode": "string"
                }
            },
            "loyalty": {
                "card": {
                    "id": "string",
                    "name": "string",
                    "issuer": "string",
                    "points": 0,
                    "expires": "2022-01-01 00:00:00"
                },
                "codes": [
                    "string"
                ]
            },
            "identifier": {
                "id": "string",
                "badge": "string",
                "token": "string",
                "coupons": [
                    "string"
                ],
                "passport": "string",
                "credit_card": "string"
            },
            "preferences": {
                "other": [
                    "string"
                ],
                "sizes": [
                    "string"
                ],
                "brands": [
                    "string"
                ],
                "colors": [
                    "string"
                ],
                "devices": [
                    "string"
                ],
                "channels": [
                    "string"
                ],
                "payments": [
                    "string"
                ],
                "services": [
                    "string"
                ],
                "purchases": [
                    "string"
                ],
                "fragrances": [
                    "string"
                ]
            }
        },
        # 'traits': {
        #     "a": 2,
        #     "c": 10
        # }
    })

    t = time.time()
    profiles = [(profile1, RecordMetadata(id="1", index="idx1")),
                (profile2, RecordMetadata(id="2", index="idx2")),
                (profile3, RecordMetadata(id="3", index="idx3"))]

    merged, metadata, changed_fields = merge_profiles(profiles)
    print(metadata)
    # print(merged.to_dict()['consents'])
    # p = Profile(**merged)
    # print(p.consents)
    # print("-----------")
    # # print(changed_fields)
    # print(p.data.job)
    print(time.time() - t)


asyncio.run(main())
