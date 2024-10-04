import pandas as pd
from pymongo import MongoClient
import datetime
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



def delete_value(option, key, value):
    value = convert_to_double(value)
    query = {key: value}
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
def find_all(key, value, operator):
    value = convert_to_double(value)
    query = build_query(key,operator,value)
    result = list(collection_coffee_shop.find(query, {'_id': 0}))
    data = pd.DataFrame(result)
    if result:
        print("Query find:")
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

def find_sort(key, order):
    order = convert_to_int(order)
    if order == 1 or order == -1:
        results = collection_coffee_shop.find({},{'_id': 0}).sort(key, order)
        data = pd.DataFrame(results)
        print(data)
    else:
        print("Not valid order")
def groupby_option(option, A, B):
    if option == "avg":
        pipeline = [
            {"$group": {"_id": f"${B}", "average": {"$avg": f"${A}"}}}
        ]
    elif option == "sum":
        pipeline = [
            {"$group": {"_id": f"${B}", "total": {"$sum": f"${A}"}}}
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


def update_option(option, key1, operator, value1, key2, value2):
    global results
    value2 = convert_to_double(value2)
    value1 = convert_to_double(value1)
    query1 = build_query(key1, operator, value1)
    query2 = {key2: value2}

    if option == "one":
        results = collection_coffee_shop.update_one(query1, {"$set": query2})
    if option == "many":
        results = collection_coffee_shop.update_many(query1, {"$set": query2})
    if results.modified_count > 0:
        print("Document updated successfully.")
    else:
        print("No document was updated.")



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py [operation] [arguments]")
        sys.exit(1)
    client = MongoClient('mongodb://localhost:27017/')
    db = client['chatDB']
    collection_coffee_shop = db['coffee_shop']
    command = sys.argv[1].lower()

    if command == "find_all":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        find_all(key, value,operator)

    if command == "find_limit":
        key = sys.argv[2]
        value = sys.argv[4]
        operator = sys.argv[3]
        limit_num = sys.argv[5]
        find_limit(key, value,operator,limit_num)

    if command == "find_sort":
        key = sys.argv[2]
        order = sys.argv[3]
        find_sort(key,order)

    if command == "delete":
        option = sys.argv[2]
        key = sys.argv[3]
        value = sys.argv[4]
        delete_value(option, key, value)

    if command == "groupby":
        option = sys.argv[2]
        A = sys.argv[3]
        B = sys.argv[4]
        groupby_option(option, A, B)

    if command == "update":
        option = sys.argv[2]
        key1 = sys.argv[3]
        operator = sys.argv[4]
        value1 = sys.argv[5]
        key2 = sys.argv[6]
        value2 = sys.argv[7]
        update_option(option,key1,operator,value1,key2,value2)

