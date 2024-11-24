import mysql.connector
from sqlalchemy import create_engine, inspect, text
import requests
import pandas as pd
import numpy as np
import re
from random import sample

# DATABASE SETUP

# engine = create_engine("mysql+mysqlconnector://root:password@localhost/chatDB")
engine = create_engine("mysql+mysqlconnector://chatdb:chatdbpassword@18.118.113.189/chatDB")

def SQL_db_connect():
    ''' connect to mySQL chatDB database '''
    connection = engine.connect()

    return connection

def SQL_load_default(db_connection):
    ''' load default datasets and erase leftover uploads '''
    
    # imdb_top_1000_movie = pd.read_csv('imdb_top_1000_movie.csv')  
    # imdb_top_1000_movie.to_sql('imdb_movie', engine, if_exists='replace', index=False)
    
    # imdb_top_1000_tv = pd.read_csv('imdb_top_1000_tv.csv')
    # imdb_top_1000_tv.to_sql('imdb_tv', engine, if_exists='replace', index=False)
    
    # netflix_movie_tv = pd.read_csv('netflix_movie_tv.csv')
    # netflix_movie_tv.to_sql('netflix', engine, if_exists='replace', index=False)

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

        # construct where
        w = where_clause(filter_clause(attr[0], where_operator, where_val, where_type))

        # pick random projection
        attrs = sample([col[0] for col in cols], sample(range(1, n-1), 1)[0])
        s = select_clause(attrs, all = sample([True, False], 1)[0])

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

        # construct where
        w = where_clause(f'{w1} {lo} {w2}')
    
    return '\n'.join([s, f, w]).strip() + ';'
    
def nlp_execute_where(prompt, db_connection):

    operator_dict = {'=': ['equal'], '<>': ['not equal'], '>': ['greater', 'later', 'larger', 'bigger', 'more', 'after'], 
                 '<': ['less', 'smaller', 'earlier', 'fewer', 'before']}
    
    # general queries
    if prompt.endswith('where'):
        learned_params = None

    # specific queries
    else:
        learned_params = {'table': '',
                  'filter1': [],
                  'filter2': [],
                  'log_op': '',
                  'project': [],
                 }

        # select
        if prompt.startswith('all columns'):
            learned_params['project'] = []
        else:
            learned_params['project'] = prompt.split('from')[0].strip().split(', ')

        # from
        insp = inspect(engine)
        for table in insp.get_table_names():
            if table in prompt:
                learned_params['table'] = table
                break

        # where condition(s)
        where = prompt.split('where')[1].strip()
        filters = []
        for log_op in ['and', 'or']:
            if log_op in where.split(' '):
                learned_params['log_op'] = log_op
                filters = where.split(f' {log_op} ')
                break
            else:
                if log_op == 'and':
                    continue
                else:
                    filters.append(where)
                
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

def construct_orderby(db_connection, param_dict = None):
    s, f, o = '', '', ''

    if param_dict is None:
        # pick random table from database
        insp = inspect(engine)
        table = sample(insp.get_table_names(), 1)[0]
        f = from_clause(table)

        # see columns 
        cols = SQL_get_columns(db_connection, table)
        n = len(cols)

        # pick random column and direction
        order_dict = {sample(cols, 1)[0][0]: sample(['ASC', 'DESC'], 1)[0]}

        # construct order by
        o = orderby_clause(order_dict)

        # pick random projection
        attrs = sample([col[0] for col in cols], sample(range(1, n-1), 1)[0])
        s = select_clause(attrs, all = sample([True, False], 1)[0])

    else: 
        # projection and table
        if len(param_dict['project']) > 0:
            s = select_clause(param_dict['project'], False)
        else:
            s = select_clause(param_dict['project'], True)
        f = from_clause(param_dict['table'])

        # construct order by
        o = orderby_clause(param_dict['order'])
    
    return '\n'.join([s, f, o]).strip() + ';'

def nlp_execute_orderby(prompt, db_connection):

    directions = {'ascending': 'ASC', 'descending': 'DESC'}

    # general queries
    if prompt.endswith('order by'):
        learned_params = None

    #specific queries
    else:
        learned_params = {'table': '',
                  'order': {},
                  'project': [],
                 }

        # select
        if prompt.startswith('all columns'):
            learned_params['project'] = []
        else:
            learned_params['project'] = prompt.split('from')[0].strip().split(', ')

        # from
        insp = inspect(engine)
        for table in insp.get_table_names():
            if table in prompt:
                learned_params['table'] = table
                break

        # order by
        orders = prompt.split(' in ')[1].split(', ')
        for order in orders:
            tokens = order.split(' ')
            learned_params['order'][tokens[-1]] = directions[tokens[0]]

    query_string = construct_orderby(db_connection, learned_params)
    query_result = execute_query(db_connection, query_string)
    return query_string, query_result

def construct_limit(db_connection, param_dict = None):
    s, f, l = '', '', ''

    if param_dict is None:
        # pick random table from database
        insp = inspect(engine)
        table = sample(insp.get_table_names(), 1)[0]
        f = from_clause(table)

        # see columns 
        cols = SQL_get_columns(db_connection, table)
        n = len(cols)

        # pick random limit
        l = limit_clause(sample(range(1, 21), 1)[0])

        # pick random projection
        attrs = sample([col[0] for col in cols], sample(range(1, n-1), 1)[0])
        s = select_clause(attrs, all = sample([True, False], 1)[0])

    else: 
        # projection and table
        if len(param_dict['project']) > 0:
            s = select_clause(param_dict['project'], False)
        else:
            s = select_clause(param_dict['project'], True)
        f = from_clause(param_dict['table'])

        # construct limit
        l = limit_clause(param_dict['limit'])
    
    return '\n'.join([s, f, l]).strip() + ';'

def nlp_execute_limit(prompt, db_connection):    
    # general queries
    if prompt.endswith('limit'):
        learned_params = None

    # specific queries
    else:
        learned_params = {'table': '',
                  'limit': 0,
                  'project': [],
                 }

        # select
        if prompt.startswith('all columns'):
            learned_params['project'] = []
        else:
            learned_params['project'] = prompt.split('from')[0].strip().split(', ')

        # from
        insp = inspect(engine)
        for table in insp.get_table_names():
            if table in prompt:
                learned_params['table'] = table
                break

        # limit
        learned_params['limit'] = prompt.split(' ')[-1]

    query_string = construct_limit(db_connection, learned_params)
    query_result = execute_query(db_connection, query_string)
    return query_string, query_result

def construct_join(db_connection, param_dict = None):
    
    s, f = '', ''

    if param_dict is None:
        # random tables
        cols = []
        tables = sample(['netflix', 'imdb_movie', 'imdb_tv'], 2)

        # table attributes
        t1_cols = SQL_get_columns(db_connection, tables[0])
        cols += [(0, col) for col in t1_cols]
        t2_cols = SQL_get_columns(db_connection, tables[1])
        cols += [(1, col) for col in t2_cols]

        # join condition
        joinable = [a[0] for b in t2_cols for a in t1_cols if a[0]==b[0]]
        join_col = sample(joinable, 1)[0]

        # construct from
        f = from_clause(join_clause(tables, sample(['inner', 'left', 'right'], 1)[0], [join_col, join_col, '=']))

        # pick random projection
        attrs = []
        for col in sample(cols, sample([2, 3, 4], 1)[0]):
            table_idx, attr = col
            rand_col = f't{table_idx+1}.{attr[0]}'
            attrs.append(rand_col)
        s = select_clause(attrs, False)

    else: 
        # unraveling table-attribute dictionary
        project = []
        tables = []
        idx = 1
        for t, names in param_dict['project'].items():
            tables.append(t)
            for n in names:
                project.append(f't{str(idx)}.{n}')
            idx += 1

        # projection
        s = select_clause(project, False)
        # join
        j = join_clause(tables, 'inner', param_dict['condition'])
        # tables
        f = from_clause(j)
    
    return '\n'.join([s, f]).strip() + ';'

def nlp_execute_join(prompt, db_connection):    
    # general queries
    if prompt.endswith('join'):
        learned_params = None

    # specific queries
    else:
        learned_params = {'project': {}, 'kind': 'inner', 'condition': []}

        # select
        prompts = prompt.split(' and ')
        t1 = prompts[0].split(' ')
        learned_params['project'][t1[0]] = [col.replace(',', '') for col in t1[1:]]
        t2 = prompts[1].split(' ')
        learned_params['project'][t2[0]] = [col.replace(',', '') for col in t2[1:-3]]

        # from and join
        on = t2[-1]
        learned_params['condition'] = [on, on, '=']

    query_string = construct_join(db_connection, learned_params)
    query_result = execute_query(db_connection, query_string)
    return query_string, query_result

def construct_groupby(db_connection, param_dict = None):
    
    s, f, g = '', '', ''

    if param_dict is None:        
        # pick random table from database
        insp = inspect(engine)
        table = sample(insp.get_table_names(), 1)[0]
        f = from_clause(table)

        # see columns 
        cols = SQL_get_columns(db_connection, table)
        n = len(cols)

        # pick random group by column
        group_col = sample(cols, 1)[0]
        g = groupby_clause([group_col[0]])
        
        # aggregate
        subset = list(set(cols) - set([group_col]))
        attr = sample(subset, 1)[0]
        if attr[1] == 'text':
            aggregator = np.random.choice(['MAX', 'MIN', 'COUNT']) 
        else:
            aggregator = np.random.choice(['MAX', 'MIN', 'COUNT', 'AVG', 'SUM'])            
        a = agg_clause([attr[0]], aggregator, False, sample(['aggregate', False], 1)[0])

        # projection
        s = select_clause([group_col[0], a], False)

    else: 
        
        # projection and aggregate
        a = agg_clause(param_dict['vals'], param_dict['agg'], param_dict['op'], param_dict['alias'])
        s = select_clause(param_dict['groups'] + [a], False)
        # tables
        f = from_clause(param_dict['table'])
        # group by
        g = groupby_clause(param_dict['groups'])
    
    return '\n'.join([s, f, g]).strip() + ';'

def nlp_execute_groupby(prompt, db_connection):    
    # general queries
    if prompt.endswith('group by'):
        learned_params = None

    # specific queries
    else:
        learned_params = {'table': '', 'agg': '', 'vals': [], 'op': False, 'alias': '', 'groups': []}

        aggs = {'COUNT': ['count', 'number'], 'MAX': ['largest', 'max', 'maximum', 'longest', 'most', 'best', 'greatest', 'biggest'], 
               'MIN': ['min', 'minimum', 'smallest', 'fewest', 'lowest', 'worst'], 'AVG': ['avg', 'average', 'mean'], 'SUM': ['sum', 'total']}
        ops = ['*', '/', '+', '-']
        group_synonyms = ['each', 'per']

        # from
        insp = inspect(engine)
        for table in insp.get_table_names():
            if table in prompt:
                learned_params['table'] = table
                break

        # tokenize
        tokens = prompt.split(' ')

        # agg
        prompt_agg = tokens[0]
        for agg in aggs:
            done = False
            for phrase in aggs[agg]:
                if phrase == prompt_agg:
                    learned_params['agg'] = agg
                    done = True
                    break
            if done:
                break

        # vals and operator
        for token in tokens:
            done = False
            if '(' in token:
                for op in ops:
                    if op in token:
                        learned_params['op'] = op
                        learned_params['vals'] = token.replace('(', '').replace(')', '').split(op)
                        done = True
                        break
            if done:
                break
            learned_params['vals'] = [prompt.split(learned_params['table'])[1].strip().split()[0]]
            done = True

        # alias
        learned_params['alias'] = prompt_agg + '_' + prompt.split(learned_params['table'])[1].strip().split()[0]
    
        # groups
        for syn in group_synonyms:
            if syn in tokens:
                learned_params['groups'] = prompt.split(f' {syn} ')[1].split(' and ')
                break

    query_string = construct_groupby(db_connection, learned_params)
    query_result = execute_query(db_connection, query_string)
    return query_string, query_result

def clean_prompt(prompt):
    prompt = prompt.lower()
    prompt = prompt.split(' ')
    return prompt

def identify_keyword(prompt):
    keywords = ['group', 'per', 'each', 'join', 'joined', 'where', 'order', 'limit', 'limited']
    for k in keywords:
        if k in prompt.split():
            return k

def respond(prompt, db_connection):
    kw = identify_keyword(prompt)
    if kw == 'where':
        return nlp_execute_where(prompt, db_connection)
    if kw == 'order':
        return nlp_execute_orderby(prompt, db_connection)
    if kw in ['limit', 'limited']:
        return nlp_execute_limit(prompt, db_connection)
    if kw in ['join', 'joined']:
        return nlp_execute_join(prompt, db_connection)
    if kw in ['group', 'per', 'each']: 
        return nlp_execute_groupby(prompt, db_connection)



































