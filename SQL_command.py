import mysql.connector
import seaborn as sns
from sqlalchemy import create_engine, inspect
from sqlalchemy import text
import requests
import pandas as pd
import time
import numpy as np
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import re
from sqlalchemy.types import Integer
from nltk.tokenize import word_tokenize
from random import sample

# DATABASE SETUP

engine = create_engine("mysql+mysqlconnector://root:password@localhost/chatDB")
# engine = create_engine("mysql+mysqlconnector://chatdb:chatdbpassword@18.218.34.75/chatDB")

def SQL_db_connect():
    ''' connect to mySQL chatDB database '''
    connection = engine.connect()

    return connection

def SQL_load_default(db_connection):
    ''' reload default datasets and erase leftover uploads '''
    
    imdb_top_1000_movie = pd.read_csv('imdb_top_1000_movie.csv')  
    imdb_top_1000_movie.to_sql('imdb_movie', engine, if_exists='replace', index=False)
    
    imdb_top_1000_tv = pd.read_csv('imdb_top_1000_tv.csv')
    imdb_top_1000_tv.to_sql('imdb_tv', engine, if_exists='replace', index=False)
    
    netflix_movie_tv = pd.read_csv('netflix_movie_tv.csv')
    netflix_movie_tv.to_sql('netflix', engine, if_exists='replace', index=False)

    insp = inspect(engine)

    for t in insp.get_table_names():
        if t not in ['imdb_movie', 'imdb_tv', 'netflix']:
            db_connection.execute(text(f'DROP TABLE IF EXISTS {t};'))

def SQL_rename(custom_ds):
    ''' fix formatting of custom uploaded datasets '''
    
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
    ''' upload custom dataset '''
    
    custom_ds = pd.read_csv(url)
    custom_ds = SQL_rename(custom_ds)
    custom_ds.to_sql(name, db_connection, if_exists='replace', index=False)

def SQL_overview(db_connection):
    ''' give overview of datasets '''
    
    insp = inspect(engine)

    for t in insp.get_table_names():
        myresult = db_connection.execute(text(f"SELECT * FROM {t} LIMIT 1;")).fetchall()
        result_df = pd.DataFrame(myresult)
        print(f'{t} has the following attributes: {list(result_df.columns)}')
        print(f'The first entry look like this: {result_df}')
        print('\n')

def SQL_get_columns(db_connection, dataset_name):
    ''' view column names and types '''
    
    myresult = [col[:2] for col in db_connection.execute(text("SHOW COLUMNS FROM " + dataset_name)).fetchall()]
    return myresult

def SQL_view_column(db_connection, dataset_name, col_name):
    ''' view a column '''
    
    SQL_command = "SELECT " + col_name + " FROM " + dataset_name
    myresult = db_connection.execute(text(SQL_command)).fetchall()
    return pd.DataFrame(myresult)

# CLAUSES

def join_clause(tables, kind, condition):
    output = f"{tables[0]} t1 {kind} join {tables[1]} t2"
    if kind != 'cross':
        output += f" on t1.{condition[0]} {condition[2]} t2.{condition[1]}"
    return output

def from_clause(table):
    return 'FROM ' + table

def filter_clause(attr, operator, val, is_val_str):
    if val is None:
        if operator == '=':
            operator = 'is'
        else:
            operator = 'is not'
        val = 'None'
    else:
        if is_val_str:
            val = "'" + str(val).replace("'", "''") + "'"
    return f'{attr} {operator} {val}'

def where_clause(filter):
    return 'WHERE ' + filter

def groupby_clause(attr_list):
    clause = ', '.join(attr_list)
    return 'GROUP BY ' + clause

def agg_clause(attrs, aggregator, operator, rename):
    output = (f' {operator} ').join(attrs)
    output = f'{aggregator}({output})'
    if rename:
        output += f' AS {rename}'
    
    return output

def select_clause(attrs, all): 
    if all:
        content = '*'
    else:
        content = ', '.join(attrs)

    return 'SELECT ' + content

def orderby_clause(attr_direction):
    content = [f'{a} {d}' for a, d in attr_direction.items()]
    content = ', '.join(content)
    return 'ORDER BY ' + content

def limit_clause(num):
    return 'LIMIT ' + str(num)

# NLP AND QUERY CONSTRUCTION

def execute_query(db_connection, query_string):
    myresult = db_connection.execute(text(query_string)).fetchall()
    return pd.DataFrame(myresult)

def construct_where(db_connection, param_dict = None):
    s, f, w, w1, w2, lo = '', '', '', '', '', '' 

    if param_dict is None:
        # pick random table from database
        insp = inspect(engine)
        table = sample(insp.get_table_names(), 1)[0]
        f = from_clause(table)

        # see columns 
        cols = SQL_get_columns(db_connection, table)
        n = len(cols)

        # pick random column and value
        attr = sample(cols, 1)[0]
        where_attr = SQL_view_column(db_connection, table, attr[0])
        where_val = np.random.choice(where_attr.iloc[:, 0].dropna())

        # pick random operator
        if attr[1] == 'text':
            where_type = True
            where_operator = sample(['=', '<>'], 1)[0]
        else:
            where_type = False
            where_operator = sample(['=', '>', '<', '>=', '<=', '<>'], 1)[0]

        # pick random projection
        attrs = sample([col[0] for col in cols], sample(range(1, n-1), 1)[0])
        s = select_clause(attrs, all = sample([True, False], 1)[0])

        # construct query
        w = where_clause(filter_clause(attr[0], where_operator, where_val, where_type))
    else: 
        # projection and table
        if len(param_dict['project']) > 0:
            s = select_clause(param_dict['project'], False)
        else:
            s = select_clause(param_dict['project'], True)
        f = from_clause(param_dict['table'])

        # where filters
        w1 = filter_clause(param_dict['filter1'][0], param_dict['filter1'][1], 
                         param_dict['filter1'][2], param_dict['filter1'][3])
        if param_dict['filter2']:
            w2 = filter_clause(param_dict['filter2'][0], param_dict['filter2'][1], 
                             param_dict['filter2'][2], param_dict['filter2'][3])
            lo = param_dict['log_op']

        # construct query
        w = where_clause(f'{w1} {lo} {w2}')
    
    return '\n'.join([s, f, w]).strip() + ';'

def nlp_execute_where(prompt, db_connection):
    if prompt.endswith('where'):
        learned_params = None
    else:
        learned_params = {'table': False,
                  'filter1': False,
                  'filter2': False,
                  'log_op': False,
                  'project': False,
                 }
        if prompt.startswith('all columns'):
            learned_params['project'] = []
        else:
            learned_params['project'] = prompt.split('from')[0].strip().split(', ')
    
        insp = inspect(engine)
        for table in insp.get_table_names():
            if table in prompt:
                learned_params['table'] = table
                break
    
        where = prompt.split('where')[1].strip()
        filters = []
        for log_op in ['and', 'or']:
            if log_op in where.split(' '):
                learned_params['log_op'] = log_op
                filters = where.split(log_op)
            else:
                filters.append(where)
            break
        for i in range(len(filters)):
            done = False
            for op in operator_dict:
                for phrase in operator_dict[op]:
                    if phrase in filters[i]:
                        cols = SQL_get_columns(db_connection, table)
                        for col in cols:
                            filter = filters[i].strip().split(' ')
                            if col[0] == filter[0]:
                                is_string = col[1] == 'text'
                                learned_params[f'filter{i + 1}'] = [filter[0], op, filter[-1], is_string]
                                break
                        done = True
                        break
                if done:
                    break
    query_string = construct_where(db_connection, learned_params)
    query_result = execute_query(db_connection, query_string)
    return query_string, query_result

def clean_prompt(prompt):
    prompt = prompt.lower()
    prompt = prompt.split(' ')
    return prompt

keywords = ['group by', 'join', 'where', 'order by', 'limit']

def identify_keyword(prompt):
    for k in keywords:
        if k in prompt.split(' '):
            return k

operator_dict = {'=': ['equal'], '<>': ['not equal'], '>': ['greater', 'later', 'larger', 'bigger', 'more', 'after'], 
                 '<': ['less', 'smaller', 'earlier', 'fewer', 'before']}

def respond(prompt, db_connection):
    kw = identify_keyword(prompt)
    if kw == 'where':
        return nlp_execute_where(prompt, db_connection)



































