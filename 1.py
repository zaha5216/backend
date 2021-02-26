from pymongo import MongoClient
import datetime

# Это входные данные:

client = MongoClient('localhost', 27017)

db = client['testdb']

account_collection = db['account']

account_collection.delete_many({})
account_collection.insert_many([{
    'number': '7800000000000',
    'name': 'Пользователь №',
    'sessions': [
        {
            'created_at': datetime.datetime(2016, 1, 1, 0, 00, 0).isoformat("T"),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgxZc',
            'actions': [
                {
                    'type': 'read',
                    'created_at': datetime.datetime(2016, 1, 1, 1, 20, 1).isoformat("T"),
                },
                {
                    'type': 'read',
                    'created_at': datetime.datetime(2016, 1, 1, 1, 21, 13).isoformat("T"),
                },
                {
                    'type': 'create',
                    'created_at': datetime.datetime(2016, 1, 1, 1, 33, 59).isoformat("T"),
                }
            ],
        }
    ]
}, {
    'number': '7800000000001',
    'name': 'Пользователь №',
    'sessions': [
        {
            'created_at': datetime.datetime(2016, 1, 2, 0, 00, 0).isoformat("T"),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgx3c',
            'actions': [
                {
                    'type': 'read',
                    'created_at': datetime.datetime(2016, 1, 2, 1, 20, 1).isoformat("T"),
                },
                {
                    'type': 'read',
                    'created_at': datetime.datetime(2016, 1, 2, 1, 24, 2).isoformat("T"),
                },
                {
                    'type': 'create',
                    'created_at': datetime.datetime(2016, 1, 2, 1, 33, 12).isoformat("T"),
                },
                {
                    'type': 'create',
                    'created_at': datetime.datetime(2016, 1, 2, 1, 42, 11).isoformat("T"),
                }
            ],
        },
        {
            'created_at': datetime.datetime(2016, 1, 3, 0, 00, 0).isoformat("T"),
            'session_id': '6QBnQhFGgDgC2FDfGwbgEaLbPMMBofPFVrVh9Pn2quooAcgx3c',
            'actions': [
                {
                    'type': 'read',
                    'created_at': datetime.datetime(2016, 1, 3, 1, 20, 1).isoformat("T"),
                },
                {
                    'type': 'read',
                    'created_at': datetime.datetime(2016, 1, 3, 1, 24, 2).isoformat("T"),
                },
                {
                    'type': 'create',
                    'created_at': datetime.datetime(2016, 1, 3, 1, 33, 12).isoformat("T"),
                },
                {
                    'type': 'delete',
                    'created_at': datetime.datetime(2016, 1, 3, 1, 42, 11).isoformat("T"),
                }
            ],
        }
    ]
}])

# Сам запрос:

first_results = account_collection.aggregate(
    [
        {'$unwind': "$sessions"},
        {'$unwind': "$sessions.actions"},
        {
            "$group":
                {
                    "_id": {"number": "$number", "type": "$sessions.actions.type"},
                    "count": {"$sum": 1},
                    "last": {"$last": "$sessions.actions.created_at"}
                }
        },
        {
            "$group":
                {
                    "_id": "$_id.number",
                    "actions": {
                        "$push":
                            {
                                "type": "$_id.type",
                                "last": "$last",
                                "count": "$count",
                            }
                    }
                }
        },
        {
            '$project': {
                    "_id": 0,
                    "number": "$_id",
                    "actions": "$actions",
            }
        },
    ]
)

# Это костыль, потому что я, к сожалению, не успел еще до конца разобраться с MongoDB, первый раз её использовал,

for result in first_results:
    needed_actions = ['create', 'update', 'delete', 'read']
    for action in result['actions']:
        if action['type'] in needed_actions:
            needed_actions.remove(action['type'])
    for action in needed_actions:
        result['actions'].append({
            'type': action,
            'last': '$null',
            'count': 0,
        })
    print(result)
