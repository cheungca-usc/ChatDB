import datetime
import random

import pandas as pd
from pymongo import MongoClient
import sys
import seaborn as sns


def convert_to_double(value):
    try:
        return float(value)
    except ValueError:
        return value


def convert_to_int(value):
    try:
        return int(value)
    except ValueError:
        return value


def delete_value(target_dataframe,option, key, operator, value):
    value = convert_to_double(value)
    query = build_query(key, operator, value)
    if option == "one":
        result = target_dataframe.delete_one(query)
        if result.deleted_count > 0:
            print("Document deleted successfully.")
        else:
            print("No matching document found to delete.")
    if option == "many":
        result = target_dataframe.delete_many(query)
        if result.deleted_count > 0:
            print(f" {result.deleted_count} Document deleted successfully.")
        else:
            print("No matching document found to delete.")


# add eq neq gt lt lte in nin
def build_query(key, operator, value):
    operator_mapping = {
        "equal": "$eq",
        "not_equal": "$ne",
        "greater": "$gt",
        "greater_equal": "$gte",
        "less": "$lt",
        "less_equal": "$lte",
        "in": "$in",
        "not_in": "$nin"
    }

    if operator in operator_mapping:
        mongo_operator = operator_mapping[operator]

        if mongo_operator in ["$in", "$nin"]:
            if isinstance(value, str):
                value = value.split(",")
            elif not isinstance(value, list):
                raise ValueError("The 'in' and 'not_in' operators require a list or comma-separated string.")

        return {key: {mongo_operator: value}}
    else:
        print(
            "Invalid operator. Available operators are: equal, not_equal, greater, greater_equal, less, less_equal, in, not_in.")
        return None


def find_all(target_dataframe,command, key, value, operator):
    value = convert_to_double(value)
    query = build_query(key, operator, value)
    if command == "find_all":
        result = list(target_dataframe.find(query, {'_id': 0}))
        data = pd.DataFrame(result)
    else:
        result = target_dataframe.find_one(query, {'_id': 0})
        value = list(result.values())
        col = list(result.keys())
        data = pd.DataFrame([value], columns=col)
    if result:
        print(data)
    else:
        print("No matching query")


def find_limit(target_dataframe,key, value, operator, limit_num):
    value = convert_to_double(value)
    query = build_query(key, operator, value)
    result = list(target_dataframe.find(query, {'_id': 0}).limit(convert_to_int(limit_num)))
    data = pd.DataFrame(result)
    if result:
        print("Query find:")
        print(data)
    else:
        print("No matching query")


# def find_sort(key, value, operator, sortby, order):
#     order = convert_to_int(order)
#     value = convert_to_double(value)
#     if order == 1 or order == -1:
#         results = target_dataframe.find({}, {'_id': 0}).sort(sortby, order)
#         data = pd.DataFrame(results)
#         print(data)
#     else:
#         print("Not valid order")

def groupby_option(pipeline, option, A, B):
    group_stage = {"$group": {"_id": {b: f"${b}" for b in B}}}
    if option == "avg":
        for a in A:
            group_stage["$group"]["avg_" + a] = {"$avg": f"${a}"}
        pipeline.append(group_stage)
        pipeline.append({'$project': {**{b: f"$_id.{b}" for b in B}, **{f"avg_{a}": 1 for a in A}, '_id': 0}})
    elif option == "sum" or "total":
        for a in A:
            group_stage["$group"]["total_" + a] = {"$sum": f"${a}"}
        pipeline.append(group_stage)
        pipeline.append({
            '$project': {
                **{field: f"$_id.{field}" for field in B},
                **{f"total_{a}": 1 for a in A},
                '_id': 0
            }
        })
        # todo miss title
    elif option == "count":
        for a in A:
            group_stage["$group"]["count_" + a] = {"$sum": 1}
        pipeline.append(group_stage)
        pipeline.append({
            '$project': {
                **{b: 1 for b in B},
                **{f"count_{a}": 1 for a in A},
                '_id': 0
            }
        })
    else:
        print("Invalid option. Choose from 'avg', 'sum', or 'count'.")
        return None
    return pipeline


def suggest_query(select_attr_list,
                  from_table,
                  where_key=None, where_operator=None, where_val=None,
                  groupby_attr_list=None,
                  agg_attr_list=None, agg_aggregator=None,
                  orderby_attr_list=None, orderby_direction=None,
                  skip = None, limit =None):
    pipeline = []

    if where_key and where_operator and where_val:
        query = build_query(where_key, where_operator, where_val)
        match_stage = {
            "$match":query
        }
        pipeline.append(match_stage)

    if groupby_attr_list:
        pipeline = groupby_option(pipeline, agg_aggregator, agg_attr_list, groupby_attr_list)

    # If there is a select list, add a $project stage
    if select_attr_list:
        project_stage = {"$project": {}}
        for attr in select_attr_list:
            project_stage["$project"][attr] = 1
        project_stage["$project"]["_id"] = 0
        pipeline.append(project_stage)

    # If there is an ORDER BY clause, add a $sort stage
    if orderby_attr_list and orderby_direction:
        sort_stage = {"$sort": {}}
        direction = 1 if orderby_direction.lower() == "asc" else -1
        for attr in orderby_attr_list:
            sort_stage["$sort"][attr] = direction
        pipeline.append(sort_stage)

    if skip is not None:
        pipeline.append({"$skip": convert_to_int(skip)})
    if limit is not None:
        pipeline.append({"$limit": convert_to_int(limit)})

    results = from_table.aggregate(pipeline)
    data = pd.DataFrame(results)
    return data


def find_sort(target_dataframe,key, value, operator, sortby, order):
    suggest_query(select_attr_list=key, from_table=target_dataframe,where_key=key, where_val=value, where_operator=operator,
                  orderby_attr_list=sortby, orderby_direction=order)


def update_option(target_dataframe,option1, option2, key1, operator, value1, key2, value2):
    global results
    value2 = convert_to_double(value2)
    value1 = convert_to_double(value1)
    query1 = build_query(key1, operator, value1)

    update_operation = {key2: value2}

    if option2 == "set":
        update = {"$set": update_operation}
    elif option2 == "inc":
        update = {"$inc": update_operation}
    elif option2 == "unset":
        update = {"$unset": update_operation}
    elif option2 == "push":
        update = {"$push": update_operation}
    elif option2 == "pull":
        update = {"$pull": update_operation}
    elif option2 == "addToSet":
        update = {"$addToSet": update_operation}
    elif option2 == "rename":
        update = {"$rename": update_operation}
    elif option2 == "mul":
        update = {"$mul": update_operation}
    else:
        raise ValueError("Unsupported update option")

    if option1 == "one":
        results = target_dataframe.update_one(query1, update)
    if option1 == "many":
        results = target_dataframe.update_many(query1, update)
    if results.modified_count > 0:
        print("Document updated successfully.")
        find_all("find_all", key1, value1, operator)
    else:
        print("No document was updated.")


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
def MDB_connect():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['chatDB']
    return db
def choose_one_randon_field(db):
    doc = db.find_one()
    field_names = [key for key in doc.keys() if key != '_id']
    random_field = random.choice(field_names)
    return random_field

def generate_random_query(target_dataframe):
    select_attr_list = []
    select_attr_list.append(choose_one_randon_field(target_dataframe))
    print(select_attr_list)
    from_table = target_dataframe
    where_attr = choose_one_randon_field(target_dataframe)
    operators = ["equal",
        "not_equal",
        "greater",
        "greater_equal",
        "less",
        "less_equal"]
    where_operator = random.choice(operators)
    doc = db.find_one()
    where_val = doc[where_attr]
    groupby_attr_list = None
    agg_attr_list = None
    agg_aggregator = None
    orderby_attr_list = choose_one_randon_field(target_dataframe)
    orderby_direction = random.choice(['asc','desc'])
    print(suggest_query(select_attr_list, from_table, where_attr, where_operator, where_val,
                        groupby_attr_list, agg_attr_list, agg_aggregator,
                        orderby_attr_list, orderby_direction, None, None))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py [operation] [arguments]")
        sys.exit(1)
    db =MDB_connect()
    collection_coffee_data = db['Coffee']
    collection_spotify_data = db['Spotify']

    collection_planet_data = db['Planets']
    target_dataframe = collection_spotify_data

    command = sys.argv[1].lower()

    if command == "find_all" or command == "find_one":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        find_all(target_dataframe,command, key, value, operator)

    if command == "find_limit":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        limit_num = sys.argv[5]
        find_limit(target_dataframe,key, value, operator, limit_num)

    if command == "find_sort":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        sortby = sys.argv[5]
        order = sys.argv[6]
        find_sort(target_dataframe,key, value, operator, sortby, order)

    if command == "delete":
        option = sys.argv[2]
        key = sys.argv[3]
        value = sys.argv[4]
        delete_value(target_dataframe,option, key, value)

    if command == "groupby":
        option = sys.argv[2]
        A = sys.argv[3]
        if option == "count":
            B = ""
        else:
            B = sys.argv[4:]

        pipeline = groupby_option([], option, ["transaction_qty"], ["store_location", "transaction_date"])
        # pipeline = groupby_option([],option,A,["store_location","transaction_date"])
        results = target_dataframe.aggregate(pipeline)
        data = pd.DataFrame(results)
        print(data)

    if command == "update":
        option1 = sys.argv[2]
        option2 = sys.argv[3]
        key1 = sys.argv[4]
        operator = sys.argv[5]
        value1 = sys.argv[6]
        key2 = sys.argv[7]
        value2 = sys.argv[8]
        update_option(target_dataframe,option1, option2, key1, operator, value1, key2, value2)

    if command == "suggest":
        generate_random_query(target_dataframe)

