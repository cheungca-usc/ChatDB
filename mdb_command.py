import datetime

import pandas as pd
from pymongo import MongoClient
import sys


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


def delete_value(option, key, operator,value):
    value = convert_to_double(value)
    query = build_query(key,operator,value)
    if option == "one":
        result = collection_coffee_shop.delete_one(query)
        if result.deleted_count > 0:
            print("Document deleted successfully.")
        else:
            print("No matching document found to delete.")
    if option == "many":
        result = collection_coffee_shop.delete_many(query)
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


def find_all(command, key, value, operator):
    value = convert_to_double(value)
    query = build_query(key, operator, value)
    if command == "find_all":
        result = list(collection_coffee_shop.find(query, {'_id': 0}))
        data = pd.DataFrame(result)
    else:
        result = collection_coffee_shop.find_one(query, {'_id': 0})
        value = list(result.values())
        col = list(result.keys())
        data = pd.DataFrame([value], columns=col)
    if result:
        print(data)
    else:
        print("No matching query")


def find_limit(key, value, operator, limit_num):
    value = convert_to_double(value)
    query = build_query(key, operator, value)
    result = list(collection_coffee_shop.find(query, {'_id': 0}).limit(convert_to_int(limit_num)))
    data = pd.DataFrame(result)
    if result:
        print("Query find:")
        print(data)
    else:
        print("No matching query")


def find_sort(key, value,operator,sortby,order):
    order = convert_to_int(order)
    value = convert_to_double(value)
    query = build_query(key, operator, value)
    if order == 1 or order == -1:
        results = collection_coffee_shop.find({}, {'_id': 0}).sort(sortby, order)
        data = pd.DataFrame(results)
        print(data)
    else:
        print("Not valid order")


def groupby_option(option, A, B):
    if option == "avg":
        pipeline = [
            {"$group": {"_id": f"${B}", "average": {"$avg": f"${A}"}}},
            {'$project': {f"{B}": '$_id',f"avg_{A}": "$average",'_id': 0}}
        ]
    elif option == "sum":
        pipeline = [
            {"$group": {"_id": f"${B}", "total": {"$sum": f"${A}"}}},
            {'$project': {f"{B}": '$_id', f"sum_{A}": "$total", '_id': 0}}
        ]
    elif option == "count":
        pipeline = [
            {"$group": {"_id": f"${A}", "count": {"$sum": 1}}}
        ]
    else:
        print("Invalid option. Choose from 'avg', 'sum', or 'count'.")
        return None

    results = collection_coffee_shop.aggregate(pipeline)
    data = pd.DataFrame(results)
    print(data)


def update_option(option1, option2, key1, operator, value1, key2, value2):
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
        results = collection_coffee_shop.update_one(query1, update)
    if option1 == "many":
        results = collection_coffee_shop.update_many(query1, update)
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


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py [operation] [arguments]")
        sys.exit(1)
    client = MongoClient('mongodb://localhost:27017/')
    db = client['chatDB']
    collection_coffee_shop = db['coffee_shop']

    # collection_coffee_shop.delete_many({})
    # df = pd.read_excel('Coffee Shop Sales.xlsx')
    # df = convert_time_columns(df)
    # collection_coffee_shop.insert_many(df.to_dict('records'))

    command = sys.argv[1].lower()

    if command == "find_all" or command == "find_one":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        find_all(command, key, value, operator)

    if command == "find_limit":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        limit_num = sys.argv[5]
        find_limit(key, value, operator, limit_num)

    if command == "find_sort":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        sortby = sys.argv[5]
        order = sys.argv[6]
        find_sort(key,value,operator,sortby,order)

    if command == "delete":
        option = sys.argv[2]
        key = sys.argv[3]
        value = sys.argv[4]
        delete_value(option, key, value)

    if command == "groupby":
        option = sys.argv[2]
        A = sys.argv[3]
        if option == "count":
            B = ""
        else:
            B = sys.argv[4]
        groupby_option(option, A, B)

    if command == "update":
        option1 = sys.argv[2]
        option2 = sys.argv[3]
        key1 = sys.argv[4]
        operator = sys.argv[5]
        value1 = sys.argv[6]
        key2 = sys.argv[7]
        value2 = sys.argv[8]
        update_option(option1, option2,key1, operator, value1, key2, value2)
