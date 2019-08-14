PROJECT DECSRIPTION:
Sparkfy is a music streaming company and has been collecting data about people's listening patterns in Json files generated from their app and stored in their local server. They need to be able to make sense of the data they have generated, so the project is about first creating Fact and Dimension tables to Model the business of Sparkify, after which a pipeline is created to move data from the Json files into the created tables so analysis can be done easily.


SCHEMA DETAILS:

songplays
Contains the records of all songs that have been played. It has foreign keys to songs, artists and users tables. It has the following columns 
songplay_id SERIAL PRIMARY KEY : Serial so that the columns can be auto generated as integers
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id int, 
location varchar, 
user_agent varchar

users 
Contains details of the users of the application. The ON CONFLICT clause has been used on the insert statement to the table to ensure that a user can easily be changed from a free user to a paid user while also avoiding duplication. The columns are
user_id int PRIMARY KEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar

songs 
Has the data about the songs. The ON CONFLICT clause has been used on the insert statement to avaoid duplicate entires to the table. It has a foreign key to the artist table and has the following columns
song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar, 
year int, 
duration float

artists 
Has the data about the artists. The ON CONFLICT clause has been used on the insert statement to avaoid duplicate entires to the table. It has the following columns
artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
lattitude float, 
longitude float

time 
It contians a break down of the play times of each song played as in the songplays table and has the following columns
start_time timestamp, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int


STEPS TO EXECUTE
Goto the console and first type "python create_table.py" to create all the tables
Then type "etl.py" to run the etl pipe line to load the data from the json files into the created tables as appropriate# data_modelling_postgres
