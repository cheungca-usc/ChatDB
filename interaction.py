
from SQL_command import SQL_db_connect, SQL_load_default, SQL_rename, SQL_upload, SQL_overview, SQL_get_columns, SQL_view_column, join_clause, from_clause, filter_clause, where_clause, groupby_clause, agg_clause, select_clause, orderby_clause, limit_clause, execute_query, construct_where, nlp_execute_where, construct_orderby, nlp_execute_orderby, construct_limit, nlp_execute_limit, construct_join, nlp_execute_join, construct_groupby, nlp_execute_groupby, clean_prompt, identify_keyword, respond
from mdb_command import MDB_db_connect,MDB_upload,MDB_overview,mdb_response
conn = SQL_db_connect()
SQL_load_default(conn)
mdb_conn = MDB_db_connect()
print('''Hello! I am ChatDB. I can help you generate SQL and NoSQL queries. 

To start, please choose a database: 
1) SQL
2) NoSQL
''')

db_type = ''

x = input()
while True:
    if x=='1':
        print('''The SQL database contains the following tables: 
        imdb_movie - the top 1000 movies on imdb
        imdb_tv - the top 1000 tv shows on imdb
        netflix - netflix titles \n''')

        SQL_overview(conn)

        db_type = 'SQL'
        break
    elif x=='2':
        print('''The NoSQL datbase contains the following tables: 
        iris - the 50 instances of 3 types of iris 
        bezdekIris - the 50 instances of 3 types of bezdekIris
        water_quality - the water quality in different regions in LA\n
        ''')
        MDB_overview(mdb_conn)

        db_type = 'NoSQL'
        break
    else:
        print("I did not recognize that option. Please choose option 1 or 2.")
        x = input()

while True:
    print("Do you want to upload a dataset? (yes/no)")
    x = input().strip().lower()

    if x == 'yes':
        while True:
            print(f"Enter the URL of your {db_type} dataset:")
            y = input().strip()

                # Validate the URL
            if y.strip().startswith("http"):
                break
            else:
                print("Invalid URL. Please enter a valid URL.")

        print("What is the name of this dataset?")
        z = input().strip()
        dataset_name = z

        if db_type == 'SQL':
            SQL_upload(y, dataset_name, conn)
        elif db_type == 'NoSQL':
            MDB_upload(y, mdb_conn, z)

        print(f"Your dataset, {dataset_name}, has been added to the database.")
        print("Anything else? (Enter 'no' to exit or 'yes' to add another dataset)")
        x = input().strip().lower()

    elif x == 'no':
        break

    else:
        print("Invalid input. Please answer 'yes' or 'no'.")


print("What kind of query are you interested in?")
x = input()

while True:
    if x == 'no':
        break
    
    try:    
        if db_type == 'SQL':
            print("\nHere is a SQL query to fit your request.\n")
            print("Command:")
            response = respond(x, conn)
            print(response[0])
            print("\nOutput:")
            print(response[1])
        else:
            print("\nHere is a MongoDB(NoSQL) query to fit your request.\n")
            print("Command:")
            response = mdb_response(x,mdb_conn)
            print(response[0])
            print("\nOutput:")
            print(response[1])
        
    except:
        print("I did not recognize that. Please try a different prompt.")
        
    print("Anything else? (Enter 'no' to exit)")
    x = input()

print("Goodbye!")

conn.close()


