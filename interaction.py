from SQL_command import SQL_db_connect
from SQL_command import SQL_upload
from SQL_command import SQL_overview
from SQL_command import suggest_query
from SQL_command import execute_query
from SQL_command import random_query

# Intro

print('''Hello! I am ChatDB. I can help you generate SQL and NoSQL queries. 

To start, please choose a dataset: 
1) Planets dataset 
2) Coffee dataset
3) Spotify dataset
4) I would like to upload my own dataset''')

# Selecting dataset

x = input()

conn = SQL_db_connect()
dataset_name = ""
if x=='1':
    print("\nLet's take a look at the Planets dataset.")
    dataset_name = "Planets"
elif x=='2':
    print("\nLet's take a look at the Coffee dataset.")
    dataset_name = "Coffee"
elif x=='3':
    print("\nLet's take a look at the Spotify dataset.")
    dataset_name = "Spotify"
elif x=='4':
    print("\nEnter URL of your dataset (SQL).")
    dataset_name = "Custom"
    y = input()
    SQL_upload(y, dataset_name, conn)
else:
    print("I did not recognize that option. Please try again.")

# Overview of dataset

df = SQL_overview(conn, dataset_name)
print(df.head())
print("\nWhat kind of query are you interested in?")
x = input()

# Example querries
while True:
    if x == 'No':
        break
        
    print("\nHere are some possible SQL queries to fit your request.\n")
    print("Command:")
    example = random_query(df, dataset_name)
    
    print("\nOutput:")
    try:
        conn = SQL_db_connect()
        print(execute_query(conn, example))
    except:
        print("not valid query")
        
    print("Anything else? (Enter 'No' to exit)")
    x = input()

print("Goodbye!")





