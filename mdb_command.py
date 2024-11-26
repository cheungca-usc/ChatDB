import datetime
import random

import pandas as pd
import numpy as np
from pymongo import MongoClient
import sys
from random import sample
#connect to MongoDB
def MDB_db_connect():
    client = MongoClient('mongodb+srv://ChatDB:ChatDBpasswd@chatdb.sjqwx.mongodb.net/?retryWrites=true&w=majority&appName=ChatDB')
    db = client['chatDB']
    return db

def MDB_overview(db):
    ''' give overview of datasets '''
    collections = db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        first_document = collection.find_one({}, {"_id": 0})
        if first_document:
            df = pd.DataFrame([first_document])
            print(f'{collection_name} has the following attributes: {list(df.columns)}')
            print(f'The first entry looks like this:\n{df}\n')
        else:
            print(f'{collection_name} is empty.\n')
def convert_time_columns(df):
    ''' in case datasets have columns in the time format '''
    for col in df.columns:
        if df[col].dtype == 'object':
            if isinstance(df[col].iloc[0], datetime.time):
                df[col] = df[col].apply(lambda x: x.strftime("%H:%M:%S") if isinstance(x, datetime.time) else x)
    return df

def MDB_upload(url,db,name):
    '''upload new collection to mongodb'''
    df = pd.read_csv(url)
    df = convert_time_columns(df)
    customized_data = db[name]
    customized_data.insert_many(df.to_dict('records'))
    return customized_data

def nlp_execute_find(prompt, db):
    '''natural language processing for prompt to execute find queries'''
    try:
        learned_params = {
            'collection': '',
            'query': {},
            'projection': None
        }

        if prompt.strip().endswith('find'):
            collections = db.list_collection_names()
            if not collections:
                raise ValueError("No collections found in the database.")

            learned_params['collection'] = random.choice(collections)

        else:
            collections = db.list_collection_names()
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            query_part = prompt.split('where')[-1].strip() if 'where' in prompt else ''
            operator_dict = {
                'greater than': '$gt',
                'less than': '$lt',
                'equal to': '$eq',
                'not equal to': '$ne'
            }

            for operator, mongo_op in operator_dict.items():
                if operator in query_part:
                    parts = query_part.split(operator)
                    field = parts[0].strip()
                    value = parts[1].strip()

                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        pass  # Leave it as a string

                    learned_params['query'][field] = {mongo_op: value}
                    break
        prompt = ' '.join(prompt.split(' ')[1:]).strip()
        if prompt.startswith('all columns') or 'from' not in prompt:
            learned_params['projection'] = []
        else:
            learned_params['projection'] = prompt.split('from')[0].strip().split(',')
        find_command = f"db.{learned_params['collection']}.find({learned_params['query']})"
        if learned_params['projection']:
            find_command += f", {learned_params['projection']}"

        collection = db[learned_params['collection']]
        find_result = list(collection.find(learned_params['query'], learned_params['projection']))

        return find_command, find_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def nlp_execute_limit(prompt, db):
    '''natural language processing for prompt to execute limit queries'''
    if prompt.endswith('limit'):
        collections = db.list_collection_names()
        collection = sample(collections, 1)[0]

        # Randomly select fields for projection
        all_fields = db[collection].find_one()
        if not all_fields:
            raise ValueError(f"The collection '{collection}' is empty.")
        projection_fields = sample(list(all_fields.keys()), random.randint(1, len(all_fields)))
        projection = {field: 1 for field in projection_fields}

        limit = random.randint(1, 10)
    else:
        learned_params = {'collection': '', 'limit': None, 'projection': []}

        words = prompt.lower().split()
        if 'limit' in words:
            index = words.index('limit')
            learned_params['limit'] = int(words[index + 1]) if words[index + 1].isdigit() else None

        if 'limited' in words:
            index = words.index('limited')
            learned_params['limit'] = int(words[index + 1]) if words[index + 1].isdigit() else None

        collections = db.list_collection_names()
        for collection in collections:
            if collection in prompt:
                learned_params['collection'] = collection
                break

        prompt = ' '.join(prompt.split(' ')[1:]).strip()
        if prompt.startswith('all columns') or 'from' not in prompt:
            learned_params['projection'] = []
        else:
            learned_params['projection'] = prompt.split('from')[0].strip().split(',')
        collection = learned_params['collection']
        projection = {field: 1 for field in learned_params['projection']} if learned_params['projection'] else None
        limit = learned_params['limit']

    query = {}
    command = f"db.{collection}.find({query}, {projection}).limit({limit})"

    # Execute the query
    collection_obj = db[collection]
    results = list(collection_obj.find(query, projection).limit(limit))

    return command, results


def nlp_execute_group(prompt, db):
    '''natural language processing for prompt to execute groupby queries'''
    try:
        operation_map = {
            'count': '$count',
            'sum': '$sum',
            'average': '$avg',
            'avg': '$avg',
            'total': '$sum',
            'minimum': '$min',
            'min': '$min',
            'maximum': '$max',
            'max': '$max',
            'highest': '$max',
            'lowest': '$min'
        }
        learned_params = {
            'collection': '',
            'groupby_field': '',
            'aggregation_field': '',
            'operation': '$avg'  # Default to average
        }

        collections = db.list_collection_names()
        if prompt.strip().endswith("group"):
            learned_params['collection'] = random.choice(collections)
            fields = list(db[learned_params['collection']].find_one().keys())
            learned_params['groupby_field'] = random.choice([f for f in fields if f != "_id"])
            learned_params['aggregation_field'] = random.choice(fields)
            learned_params['operation'] = random.choice(list(operation_map.values()))
        else:
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            if "group by" in prompt.lower():
                groupby_start = prompt.split("group by")[1].strip()
                learned_params['groupby_field'] = groupby_start.split()[0]
            elif "group" in prompt.lower():
                groupby_start = prompt.split("group")[1].strip()
                learned_params['groupby_field'] = groupby_start.split()[2]

            for keyword, op in operation_map.items():
                if keyword in prompt.lower():
                    learned_params['operation'] = op
                    if keyword != 'count':
                        aggregation_field_start = prompt.split(f"{keyword} of")[1].strip().split()[0]
                        learned_params['aggregation_field'] = aggregation_field_start
                    break

        groupby_field = learned_params['groupby_field']
        aggregation_field = learned_params['aggregation_field']

        if learned_params['operation'] == '$count':
            pipeline = [{
                "$group": {
                    "_id": f"${groupby_field}",
                    "result": {
                        "$sum": 1
                    }
                }
            }]
        else:
            pipeline = [
            {"$group": {
                "_id": f"${groupby_field}",
                "result": {
                    learned_params['operation']: f"${aggregation_field}"
                }
            }}
        ]
        aggregation_command = f"db.{learned_params['collection']}.aggregate({pipeline})"

        collection = db[learned_params['collection']]
        aggregation_result = list(collection.aggregate(pipeline))

        return aggregation_command, aggregation_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def nlp_execute_sort(prompt, db):
    '''natural language processing for prompt to execute orderby queries'''
    try:
        learned_params = {'collection': '', 'field': '', 'order': '','projection':''}

        if prompt.strip().endswith('sort'):
            collections = db.list_collection_names()
            if not collections:
                raise ValueError("No collections found in the database.")
            learned_params['collection'] = random.choice(collections)

            sample_doc = db[learned_params['collection']].find_one()
            if not sample_doc:
                raise ValueError(f"The collection '{learned_params['collection']}' is empty.")

            keys = list(sample_doc.keys())
            keys.remove('_id')

            learned_params['field'] = random.choice(keys)

            learned_params['order'] = random.choice(['asc', 'desc'])

        else:
            collections = db.list_collection_names()
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            for kw in ['by']:
                if kw in prompt:
                    learned_params['field'] = prompt.split(kw)[1].strip().split(' ')[0]
                    break

            if 'asc' in prompt or 'ascending' in prompt:
                learned_params['order'] = 'asc'
            elif 'desc' in prompt or 'descending' in prompt:
                learned_params['order'] = 'desc'
            else:
                learned_params['order'] = 'asc'
        prompt = ' '.join(prompt.split(' ')[1:]).strip()
        if prompt.startswith('all columns') or 'from' not in prompt:
            learned_params['projection'] = []
        else:
            learned_params['projection'] = prompt.split('from')[0].strip().split(',')
        projection = {field: 1 for field in learned_params['projection']} if learned_params['projection'] else {'_id': 0}

        sort_order = 1 if learned_params['order'] == 'asc' else -1
        find_command = f"db.{learned_params['collection']}.find().sort({{{learned_params['field']}: {sort_order}}})"
        if projection:
            find_command = f"db.{learned_params['collection']}.find({{}}, {projection}).sort({{{learned_params['field']}: {sort_order}}})"

        collection = db[learned_params['collection']]
        results = list(collection.find().sort(learned_params['field'], sort_order))

        return find_command, results

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def nlp_execute_distinct(prompt, db):
    '''natural language processing for prompt to execute distinct queries'''
    try:
        learned_params = {'collection': '', 'field': ''}

        if prompt.strip().endswith('distinct'):
            collections = db.list_collection_names()
            if not collections:
                raise ValueError("No collections found in the database.")

            learned_params['collection'] = random.choice(collections)

            sample_doc = db[learned_params['collection']].find_one()
            if not sample_doc:
                raise ValueError(f"The collection '{learned_params['collection']}' is empty.")

            keys = list(sample_doc.keys())
            keys.remove('_id')  # Exclude '_id'
            learned_params['field'] = random.choice(keys)

        else:
            collections = db.list_collection_names()
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            if "of" in prompt:
                learned_params['field'] = prompt.split("of")[1].split("from")[0].strip()

        distinct_command = (
            f"db.{learned_params['collection']}.distinct('{learned_params['field']}')"
        )

        collection = db[learned_params['collection']]
        distinct_result = collection.distinct(learned_params['field'])

        return distinct_command, distinct_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def identify_mdb_keyword(prompt):
    '''give keywords for natural language processing'''
    keywords = ['find','select','where','limit', 'limited','group', 'per', 'each', 'average', 'sum', 'count', 'min', 'max', 'sort', 'distinct']
    for k in keywords:
        if k in prompt.split():
            return k
def mdb_response(prompt,db_connection):
    kw = identify_mdb_keyword(prompt)
    if kw in ['find', 'select']:
        return nlp_execute_find(prompt, db_connection)
    if kw in ['limit', 'limited']:
        return nlp_execute_limit(prompt, db_connection)
    if kw in ['group', 'per','each','average','count','sum', 'min', 'max']:
        return nlp_execute_group(prompt, db_connection)
    if kw in ['sort']:
        return nlp_execute_sort(prompt, db_connection)
    if kw =='distinct':
        return nlp_execute_distinct(prompt, db_connection)




