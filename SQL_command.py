import mysql.connector
import seaborn as sns
from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import pandas as pd
import time
import numpy as np

# MySQL database, chatDB, contains three default datasets: Planets, coffee_shop_sales, and Spotify
engine = create_engine("mysql+mysqlconnector://root:password@localhost/chatDB")

def SQL_db_connect():
    ''' Connect to chatDB database and return connection string. '''    
    connection = engine.connect()
    return connection

def SQL_upload(url, name, db_connection):
    ''' Upload new dataset inside database given URL. '''
    custom_ds = pd.read_csv(url)
    custom_ds.to_sql(name, db_connection, if_exists='replace', index=False)

def SQL_overview(db_connection, dataset_name):
    ''' Give an initial overview of a dataset (# of rows, # of cols, first few entries) and return pandas dataframe. '''
    myresult = db_connection.execute(text("SELECT * FROM " + dataset_name)).fetchall()
    result_df = pd.DataFrame(myresult)
    num_rows = result_df.shape[0]
    num_cols = result_df.shape[1]
    print("There are " + str(num_rows) + " rows and " + str(num_cols) + " columns.")
    print("The first few entries look like this: \n")
    return result_df

def select_clause(attr_list): 
    ''' Returns the SELECT clause of a SQL query. '''
    if len(attr_list)==0:
        clause = '*'
    else:
        clause = ', '.join(attr_list)
    return 'SELECT ' + clause

def agg_clause(attr_list, aggregator, operator = '', rename = ''):
    ''' Returns the aggregate portion of the SELECT clause of a SQL query. '''
    if len(attr_list)==0:
        return ''

    clause = (' ' + operator + ' ').join(attr_list)
    clause = aggregator + "(" + clause + ")"
    
    if rename:
        clause += ' AS ' + rename
    
    return clause

def from_clause(table_name):
    ''' Returns the FROM clause of a SQL query. '''
    return 'FROM ' + table_name

def where_clause(attr, operator, val):
    ''' Returns the WHERE clause of a SQL query. '''
    if len(attr)==0:
        return ''
    return 'WHERE ' + attr + ' ' + operator + ' ' + str(val)

def groupby_clause(attr_list):
    ''' Returns the GROUP BY clause of a SQL query. '''
    if len(attr_list)==0:
        return ''
    clause = ', '.join(attr_list)
    return 'GROUP BY ' + clause

def orderby_clause(attr_list, direction = 'ASC'):
    ''' Returns the ORDER BY clause of a SQL query. '''
    if len(attr_list)==0:
        return ''
    clause = ', '.join(attr_list)
    return 'ORDER BY ' + clause + ' ' + direction

def suggest_query(select_attr_list = [], 
                     agg_attr_list = [], agg_aggregator = '', agg_operator = '', agg_rename = '',
                     from_table = '', 
                     where_attr = '', where_operator = '', where_val = '',
                     groupby_attr_list = [], 
                     orderby_attr_list = [], orderby_direction = ''):
    ''' Constructs a complete SQL query and returns it as a string. '''

    s1 = select_clause(select_attr_list)
    s2 = agg_clause(agg_attr_list, agg_aggregator, agg_operator, agg_rename)
    f = from_clause(from_table)
    w = where_clause(where_attr, where_operator, where_val)
    g = groupby_clause(groupby_attr_list)
    o = orderby_clause(orderby_attr_list, orderby_direction)

    result = ''
    for clause in [s1, s2, f, w, g, o]:
        if not clause:
            continue
        elif result and (clause == s2):
            result += ', ' + s2
        else:
            result += '\n' + clause

    return (result + ';').strip()

def execute_query(db_connection, query_string):
    ''' Executes a complete SQL query given the query string. '''
    print(db_connection)
    myresult = db_connection.execute(text(query_string)).fetchall()
    return pd.DataFrame(myresult)

def random_query(df, dataset_name):
    n_cols = df.shape[1]
    cols = df.columns

    select_attr_list = np.random.choice(cols, size=np.random.choice(range(0,n_cols)), replace=False)
    # print(select_attr_list)

    where_attr = ''
    if len(select_attr_list)==0:
        where_attr = np.random.choice(cols)
    else:
        where_attr = np.random.choice(select_attr_list)
    where_val = np.random.choice(df[where_attr].dropna())
    if str(where_val).isnumeric():
        where_operator = np.random.choice(['=', '>', '<', '>=', '<=', '<>'])
    else:
        where_operator = np.random.choice(['=', '<>'])

    if len(select_attr_list)==0:
        orderby_attr_list = [np.random.choice(cols)]
    else:
        orderby_attr_list = [np.random.choice(select_attr_list)]
    orderby_direction = np.random.choice(['ASC', 'DESC'])
    # print(orderby_attr_list)
    # print(orderby_direction)
        
    query_string = suggest_query(select_attr_list = select_attr_list, 
                     # agg_attr_list = agg_attr_list, agg_aggregator = agg_aggregator, agg_operator = agg_operator, agg_rename = agg_rename,
                     from_table = dataset_name, 
                     where_attr = where_attr, where_operator = where_operator, where_val = where_val,
                     # groupby_attr_list = groupby_attr_list, 
                     orderby_attr_list = orderby_attr_list, orderby_direction = orderby_direction)
    print(query_string)
    return query_string








