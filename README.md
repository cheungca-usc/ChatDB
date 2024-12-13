# ChatDB
google drive 
https://drive.google.com/drive/folders/1eb7mgp-_-qsxYpCdyZUbTnOmxKJclfqI?usp=sharing

ChatDB analyzes users’ natural language input and generates corresponding SQL (MySQL) and NoSQL (MongoDB) queries along with their resulting outputs.
- The SQL database contains the following default datasets: imdb_movie, imdb_tv, and netflix. 
- The NoSQL database contains the following default datasets: iris, bezedkIris, and water_quality.
- Users can also upload custom datasets. 
- Sample SQL queries use “WHERE”, “JOIN”, “ORDER BY”, “LIMIT”, or “GROUP BY”. Sample NoSQL queries use “find”, “group”, “sort”, “limit”, “distinct”.
- File structure: interaction.py calls functions from SQL_command.py and mdb_command.py.

To run ChatDB:

1) pip install -r requirements.txt
1) python3 interaction.py
2) Select a database.
3) Optionally upload a custom dataset (e.g., https://raw.githubusercontent.com/cheungca-usc/ChatDB/refs/heads/main/databases/spotify_data.csv).
4) Provide query prompts.


Example SQL query requests:

WHERE
- example using where
- all columns of netflix table where type equals MOVIE and runtime is less than 100
- title, year from imdb_tv where year is before 2010 or year is later than 2016
- title, year, genre from imdb_movie where gross is more than 50000000
- Songs_Artist from spotify table where Songs_Artist is alphabetically before B
ORDER BY
- give me an example using order by
- title from netflix table in ascending order by imdb_rating
- imdb_id, title from imdb_tv in descending order by votes 
- title, year, genre from imdb_movie in ascending order by year, descending order by genre
- all columns from spotify table in descending order by Streams
LIMIT
- show me a query using limit
- imdb_id from netflix table limited to the first 10
- all columns from imdb_tv limited to the first 5
- director from imdb_movie limited to the first 20
- Songs_Artist, Streams from spotify table limited to the first 2
JOIN
- example using join
- netflix title, year and imdb_tv genre, votes joined on imdb_id
- netflix title and imdb_movie gross joined on title
GROUP BY
- give me an example using group by
- number of netflix title per type and certificate
- average imdb_tv votes per year
- longest imdb_movie runtime for each genre
- best imdb_movie performance (gross/meta_score) per year
- total imdb_movie upvote (votes/imdb_rating) for each year and genre


Example NoSQL query prompts:

find
- Example using find
- find all columns of iris table where sepal_width equal to 1.2
- find sepal_width,class from bIris table where sepal_width greater than 1.4
- find bIris where sepal_width less than 1.4
- find bIris where sepal_width not equal to 1.4
Group
- Example using group
- group iris by class and calculate the average of sepal_width
- group iris by class and calculate the min of sepal_width
- group iris by class and calculate highest of sepal_width
- group iris by class and count the class number
- group iris by class and calculate the sum of sepal_width
sort
- Example using sort
- order by sepal_width in iris with ascending
- order by year in water_quality with descending
- orderby sepal_width from iris by ascending
Limit
- Example using limit
- Get limit 5 values of class from iris
- get class from iris limit 5
- all columns of limit 5 values for water_quality
Distinct
- Example using distinct
- Get distinct values of class from iris

 
