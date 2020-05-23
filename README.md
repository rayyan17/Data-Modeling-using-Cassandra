# DATA MODELING USING CASSANDRA
A song company is keeping its music app data in the comma separated csv files. The data is available in raw format and 
our goal is to properly organize the data into tables based on there usage.  

# NoSQL DataBase
We will create a KeySpace Cluster which will comprise of 3 tables: 
1. music_app_history : It contains the info related to songs and its users
2. user_song_list: It contains the list of songs that a user listens to
3. top_song_users:  It contains data about the top songs a user listens


## ETL Pipeline
### Extraction:
We extract data for music listened by the users from the following driectory:
```
event_data/
```


### Transformation
For the 3 tables we will select the corresponding columns from the event log data files

### Load
All the data from directories is transferred to NoSQL database using CQL


## Running the project
In order to run the project from the scratch run the following commands from your terminal:
```
python3 etl.py
```
