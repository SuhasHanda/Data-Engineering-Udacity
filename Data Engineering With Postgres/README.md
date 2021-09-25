## Project: Data Modeling with Postgres
* Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

* Project Description
In this project, I have applied data modeling with Postgres and build an ETL pipeline using Python.I have also defined fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

* Star Schema for Song Play Analysis
Star schema allows for less redundant data against using single table or so with all information. This is not as rigid as snowflake but allows better read speeds and less rigid 

Using the song and log datasets, I created a schema which is optimized for queries on song play analysis. This includes the following tables.

* Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
2. songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* Dimension Tables
1. users - users in the app
2. user_id, first_name, last_name, gender, level
3. songs - songs in music database
4. song_id, title, artist_id, year, duration
5. artists - artists in music database
6. artist_id, name, location, latitude, longitude
7. time - timestamps of records in songplays broken down into specific units
8. start_time, hour, day, week, month, year, weekday 

* Project Steps
Below are steps you can follow to complete the project:

* Create Tables
1. CREATE statements in sql_queries.py to create each table.
2. DROP statements in sql_queries.py to drop each table if it exists.
3. test.ipynb to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.
6. etl.ipynb notebook to develop ETL processes for each table. At the end of each table section, or at the end of the notebook, run test.ipynb to confirm that records were successfully inserted into each table. Remember to rerun create_tables.py to reset your tables before each time you run this notebook.

* Build ETL Pipeline
Use what you've completed in etl.ipynb to complete etl.py, where you'll process the entire datasets. Remember to run create_tables.py before running etl.py to reset your tables. Run test.ipynb to confirm your records were successfully inserted into each table.

* Document Process
README.md file contains purpose of this database in the context of the startup, Sparkify, and their analytical goals.