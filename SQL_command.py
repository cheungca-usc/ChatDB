import mysql.connector
import seaborn as sns
from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import pandas as pd
import time
import numpy as np
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import re

engine = create_engine("mysql+mysqlconnector://root:password@localhost/chatDB")

def SQL_db_connect():
    ''' Connect to chatDB database and return connection string. '''    
    connection = engine.connect()
    return connection

def SQL_load_default():
    ''' Loads Planets, Coffee, and Spotify as default datasets '''
    planets = sns.load_dataset("planets")    
    planets.to_sql('Planets', engine, if_exists='replace', index=False)
    
    coffee = pd.read_csv('coffee_shop_sales.csv')
    coffee.to_sql('Coffee', engine, if_exists='replace', index=False)
    
    spotify = pd.read_csv('spotify_data.csv')
    spotify.to_sql('Spotify', engine, if_exists='replace', index=False)

def SQL_rename(custom_ds):
    ''' Deals with columns= names containing non alphanumeric characters by replacing them with _'''
    column_names = []
    for i in range(len(custom_ds.columns)):
        name = custom_ds.columns[i]
        for char in name:
            if char.isalnum() == False:
                name = name.replace(char, '_')
        name = re.sub(r'_{2,}', '_', name)
        column_names.append(name)
    custom_ds.columns = column_names
    return custom_ds

def SQL_upload(url, name, db_connection):
    ''' Upload new dataset inside database given URL. '''
    custom_ds = pd.read_csv(url)
    custom_ds = SQL_rename(custom_ds)
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
    if attr_list is None:
        clause = ''
    elif len(attr_list)==0:
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

def where_clause(attr, operator, val, is_string_type = False):
    ''' Returns the WHERE clause of a SQL query. '''
    if len(attr)==0:
        return ''
    if is_string_type:
        val = "'" + str(val).replace("'", "''") + "'"
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
                     where_attr = '', where_operator = '', where_val = '', where_type = False,
                     groupby_attr_list = [], 
                     orderby_attr_list = [], orderby_direction = ''):
    ''' Constructs a complete SQL query and returns it as a string. '''

    s1 = select_clause(select_attr_list)
    s2 = agg_clause(agg_attr_list, agg_aggregator, agg_operator, agg_rename)
    f = from_clause(from_table)
    w = where_clause(where_attr, where_operator, where_val, where_type)
    g = groupby_clause(groupby_attr_list)
    o = orderby_clause(orderby_attr_list, orderby_direction)

    result = ''
    for clause in [s1, s2, f, w, g, o]:
        if len(clause)==0:
            continue
        elif (clause == s2):
            if result[-1]!=' ':
                result += ', ' + s2
            else:
                result += s2
        else:
            result += '\n' + clause

    return (result + ';').strip()

def execute_query(db_connection, query_string):
    ''' Executes a complete SQL query given the query string. '''
    print(db_connection)
    myresult = db_connection.execute(text(query_string)).fetchall()
    return pd.DataFrame(myresult)

def random_query(df, dataset_name):
    ''' Generates a random query as if no useful natural language input given by user '''
    n_cols = df.shape[1]
    cols = df.columns

    #SELECT
    select_attr_list = np.random.choice(cols, size=np.random.choice(range(0,n_cols)), replace=False)

    #WHERE
    where_attr = ''
    if len(select_attr_list)==0:
        where_attr = np.random.choice(cols)
    else:
        where_attr = np.random.choice(select_attr_list)
    where_val = np.random.choice(df[where_attr].dropna())
    if is_string_dtype(df[where_attr]):
        where_type = True
        where_operator = np.random.choice(['=', '<>'])
    else:
        where_type = False
        where_operator = np.random.choice(['=', '>', '<', '>=', '<=', '<>'])

    #GROUP BY
    group_or_not = np.random.choice([0,1])
    if group_or_not==1:
        groupable_col = ''
        min_unique = np.inf
        for col in df.columns:
            num_vals = len(df[col].unique())
            if num_vals < min_unique:
                groupable_col = col
                min_unique = min(min_unique, num_vals)
        groupby_attr_list = [groupable_col]
    else:
        groupby_attr_list = []

    #AGG
    numeric_attr = df.select_dtypes(include=np.number).columns
    non_numeric_attr = df.select_dtypes(exclude=np.number).columns
    numeric_or_not = np.random.choice([0,1])

    if len(groupby_attr_list) == 0:
        min_num_agg = 0
    else:
        min_num_agg = 1
        
    if numeric_or_not == 0:
        agg_attr_list = [np.random.choice(non_numeric_attr)]
        agg_aggregator = np.random.choice(['MAX', 'MIN', 'COUNT']) 
    else:
        agg_attr_list = np.random.choice(numeric_attr, size=np.random.choice(range(min_num_agg, numeric_attr.shape[0])), replace=False)
        agg_aggregator = np.random.choice(['MAX', 'MIN', 'COUNT', 'AVG', 'SUM'])
    
    if len(agg_attr_list) > 0:
        if min_num_agg == 1:
            select_attr_list = [groupable_col]
        else:
            select_attr_list = None
        
    agg_operator = np.random.choice(['+', '-', '*'])
    agg_rename = np.random.choice(['aggregate', ''])

    #ORDER BY
    if select_attr_list is None:
        orderby_attr_list = []
    elif len(select_attr_list)==0:
        orderby_attr_list = [np.random.choice(cols)]
    else:
        orderby_attr_list = [np.random.choice(select_attr_list)]
    orderby_direction = np.random.choice(['ASC', 'DESC'])

    #QUERY
    query_string = suggest_query(select_attr_list = select_attr_list, 
                     agg_attr_list = agg_attr_list, agg_aggregator = agg_aggregator, agg_operator = agg_operator, agg_rename = agg_rename,
                     from_table = dataset_name, 
                     where_attr = where_attr, where_operator = where_operator, where_val = where_val, where_type = where_type,
                     groupby_attr_list = groupby_attr_list, 
                     orderby_attr_list = orderby_attr_list, orderby_direction = orderby_direction)
    print(query_string)
    return query_string








