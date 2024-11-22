import datetime
import random

import pandas as pd
import numpy as np
from pymongo import MongoClient
import sys
from random import sample
#connect to MongoDB
def MDB_db_connect():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['chatDB']
    return db

def MDB_overview(db):
    collections = db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        first_document = collection.find_one()
        if first_document:
            df = pd.DataFrame([first_document])
            print(f'{collection_name} has the following attributes: {list(df.columns)}')
            print(f'The first entry looks like this:\n{df}\n')
        else:
            print(f'{collection_name} is empty.\n')
def convert_time_columns(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            if isinstance(df[col].iloc[0], datetime.time):
                df[col] = df[col].apply(lambda x: x.strftime("%H:%M:%S") if isinstance(x, datetime.time) else x)
    return df

def MDB_upload(url,db,name):
    df = pd.read_csv(url)
    df = convert_time_columns(df)
    customized_data = db[name]
    customized_data.insert_many(df.to_dict('records'))
    return customized_data

def nlp_execute_find(prompt, db):
    try:
        # Initialize parameters
        learned_params = {
            'collection': '',
            'query': {},
            'projection': None
        }

        # Parse the prompt
        if prompt.strip().endswith('find'):
            # Randomly pick a collection if the prompt is ambiguous
            collections = db.list_collection_names()
            if not collections:
                raise ValueError("No collections found in the database.")

            # Randomly pick a collection
            learned_params['collection'] = random.choice(collections)

        else:
            # Extract the collection from the prompt
            collections = db.list_collection_names()
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            # Extract conditions from the prompt
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

                    # Attempt to cast the value to a number if possible
                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        pass  # Leave it as a string

                    learned_params['query'][field] = {mongo_op: value}
                    break

        # Generate the MongoDB find command
        find_command = f"db.{learned_params['collection']}.find({learned_params['query']})"
        if learned_params['projection']:
            find_command += f", {learned_params['projection']}"

        # Execute the find query
        collection = db[learned_params['collection']]
        find_result = list(collection.find(learned_params['query'], learned_params['projection']))

        return find_command, find_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def nlp_execute_limit(prompt, db):
    if prompt.endswith('limit'):
        collections = db.list_collection_names()
        collection = sample(collections, 1)[0]

        # Randomly select fields for projection
        all_fields = db[collection].find_one()
        if not all_fields:
            raise ValueError(f"The collection '{collection}' is empty.")
        projection_fields = sample(list(all_fields.keys()), random.randint(1, len(all_fields)))
        projection = {field: 1 for field in projection_fields}

        # Randomly select a limit
        limit = random.randint(1, 10)
    else:
        # Default learned parameters
        learned_params = {'collection': '', 'limit': None, 'projection': []}

        # Parse the prompt for limit
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

        if 'columns' in words:
            columns_index = words.index('columns')
            columns = prompt.split('columns')[1].split('from')[0].strip()
            learned_params['projection'] = [col.strip() for col in columns.split(',')]

        collection = learned_params['collection']
        projection = {field: 1 for field in learned_params['projection']} if learned_params['projection'] else None
        limit = learned_params['limit']

        # Build MongoDB command
    query = {}
    command = f"db.{collection}.find({query}, {projection}).limit({limit})"

    # Execute the query
    collection_obj = db[collection]
    results = list(collection_obj.find(query, projection).limit(limit))

    return command, results


def nlp_execute_groupby(prompt, db):
    try:
        # Define operation map for aggregation (e.g., count, sum, avg)
        operation_map = {
            'count': '$count',
            'sum': '$sum',
            'average': '$avg',
            'avg': '$avg',
            'total': '$sum',
            'minimum': '$min',
            'min': '$min',
            'maximum': '$max',
            'max': '$max'
        }

        # Initialize learned parameters
        learned_params = {
            'collection': '',
            'groupby_field': '',
            'aggregation_field': '',
            'operation': '$avg'  # Default to average
        }

        # List collections in the database
        collections = db.list_collection_names()

        # Handle ambiguous prompts
        if prompt.strip().endswith("groupby"):
            learned_params['collection'] = random.choice(collections)
            fields = db[learned_params['collection']].find_one().keys()
            learned_params['groupby_field'] = random.choice([f for f in fields if f != "_id"])
            learned_params['aggregation_field'] = random.choice(fields)
            learned_params['operation'] = random.choice(list(operation_map.values()))
        else:
            # Extract collection name (e.g., "iris")
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            # Extract groupby field (e.g., "class")
            if "group by" in prompt.lower():
                groupby_start = prompt.split("group by")[1].strip()
                learned_params['groupby_field'] = groupby_start.split()[0]# Get the part after "group by"
            elif "group" in prompt.lower():
                groupby_start = prompt.split("group")[1].strip()  # Get the part after "group"
                learned_params['groupby_field'] = groupby_start.split()[2]

            # Identify operation (e.g., "average")
            for keyword, op in operation_map.items():
                if keyword in prompt.lower():
                    learned_params['operation'] = op
                    if keyword != 'count':  # If not count, we need an aggregation field
                        aggregation_field_start = prompt.split(f"{keyword} of")[1].strip().split()[0]
                        learned_params['aggregation_field'] = aggregation_field_start
                    break


        # Correct handling of multi-word attributes like "sepal width"
        groupby_field = learned_params['groupby_field']
        aggregation_field = learned_params['aggregation_field']


        # Construct the MongoDB aggregation pipeline with correct field reference
        if learned_params['operation'] == '$count':
            pipeline = [{
                "$group": {
                    "_id": f"${groupby_field}",  # Group by the specified field
                    "result": {
                        "$sum": 1  # Count the documents in each group
                    }
                }
            }]
        else:
            pipeline = [
            {"$group": {
                "_id": f"${groupby_field}",  # group by field
                "result": {
                    learned_params['operation']: f"${aggregation_field}"  # reference aggregation field
                }
            }}
        ]
        print(pipeline)
        # Generate the aggregation command
        aggregation_command = f"db.{learned_params['collection']}.aggregate({pipeline})"

        # Execute the aggregation query
        collection = db[learned_params['collection']]
        aggregation_result = list(collection.aggregate(pipeline))

        return aggregation_command, aggregation_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def nlp_execute_orderby(prompt, db):
    try:
        # Initialize parameters
        learned_params = {'collection': '', 'field': '', 'order': ''}

        # Handle case where the prompt ends with "orderby"
        if prompt.strip().endswith('orderby'):
            collections = db.list_collection_names()
            if not collections:
                raise ValueError("No collections found in the database.")
            # Randomly pick a collection
            learned_params['collection'] = random.choice(collections)

            # Fetch columns (keys) from the collection
            sample_doc = db[learned_params['collection']].find_one()
            if not sample_doc:
                raise ValueError(f"The collection '{learned_params['collection']}' is empty.")

            keys = list(sample_doc.keys())
            keys.remove('_id')  # Exclude '_id' from ordering

            # Randomly pick a field for ordering
            learned_params['field'] = random.choice(keys)

            # Randomly pick an order (ascending or descending)
            learned_params['order'] = random.choice(['asc', 'desc'])

        else:
            # Parse the prompt for collection, field, and order details
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
                learned_params['order'] = 'asc'  # Default to ascending

        sort_order = 1 if learned_params['order'] == 'asc' else -1
        find_command = f"db.{learned_params['collection']}.find().sort({{{learned_params['field']}: {sort_order}}})"

        collection = db[learned_params['collection']]
        results = list(collection.find().sort(learned_params['field'], sort_order))

        return find_command, results

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def nlp_execute_distinct(prompt, db):
    try:
        # Initialize parameters
        learned_params = {'collection': '', 'field': ''}

        # Handle case where the prompt ends with "distinct"
        if prompt.strip().endswith('distinct'):
            collections = db.list_collection_names()
            if not collections:
                raise ValueError("No collections found in the database.")

            # Randomly pick a collection
            learned_params['collection'] = random.choice(collections)

            # Fetch sample document to identify fields
            sample_doc = db[learned_params['collection']].find_one()
            if not sample_doc:
                raise ValueError(f"The collection '{learned_params['collection']}' is empty.")

            # Randomly pick a field
            keys = list(sample_doc.keys())
            keys.remove('_id')  # Exclude '_id'
            learned_params['field'] = random.choice(keys)

        else:
            # Parse the prompt for collection and field
            collections = db.list_collection_names()
            for collection in collections:
                if collection in prompt:
                    learned_params['collection'] = collection
                    break

            if "of" in prompt:
                learned_params['field'] = prompt.split("of")[1].split("from")[0].strip()

        # Build the MongoDB distinct command
        distinct_command = (
            f"db.{learned_params['collection']}.distinct('{learned_params['field']}')"
        )

        # Execute the distinct query
        collection = db[learned_params['collection']]
        distinct_result = collection.distinct(learned_params['field'])

        return distinct_command, distinct_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None, []

def identify_mdb_keyword(prompt):
    keywords = ['find','where','limit', 'limited','groupby','group', 'per', 'each', 'average', 'sum', 'count', 'min', 'max', 'orderby','order', 'distinct']
    for k in keywords:
        if k in prompt.split():
            return k
def mdb_response(prompt,db_connection):
    kw = identify_mdb_keyword(prompt)
    if kw == 'find':
        return nlp_execute_find(prompt, db_connection)
    if kw in ['limit', 'limited']:
        return nlp_execute_limit(prompt, db_connection)
    if kw in ['group', 'per','each','average','count','sum', 'min', 'max']:
        return nlp_execute_groupby(prompt, db_connection)
    if kw in ['orderby','order']:
        return nlp_execute_orderby(prompt, db_connection)
    if kw =='distinct':
        return nlp_execute_distinct(prompt, db_connection)




