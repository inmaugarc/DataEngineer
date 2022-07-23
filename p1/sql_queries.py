# Script containing the basic queries for this dataset

# DROP TABLES
#   """
#    - Queries to drop tables
#    - Every table has its drop query    
#    """

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Query to create songplay table
# This is the fact table
# And contains 9 fields
# that describe when a user plays a song of an artist

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent TEXT)
    """)

# Query to create the table containing users of our app
# This is a dimension table
# And describes the main characteristics of a user

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY UNIQUE NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR)
    """)

# Query to create the song table
# This is a dimension table
# with the songs the users listen to
# Every song is determined by 5 fields (id,title,artist,year,duration)

#artist_id VARCHAR REFERENCES artists (artist_id), 

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL, 
    year INT,
    duration FLOAT NOT NULL)
    """)

# Query to create the artists table
# This is a dimensional table and
# contains the main information of an artist

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT)
    """)
# Query to create our time records
# This is a dimension table that
# gives us information about the time
# we have the exact time when a user listens a song
# And then we augment this time, by adding the week, day of week, year, etc...

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT, 
    week INT, 
    month INT,
    year INT, 
    weekday INT)
""")

# INSERT RECORDS
# Queries to insert records to our fact table

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)    
    ON CONFLICT DO NOTHING
""")

# Query to insert records to our user table
user_table_insert = ("""
    INSERT INTO users (user_id,first_name,last_name,gender,level)             
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level || 'free';
""")

# Query to insert records to our songs table
song_table_insert = ("""
    INSERT INTO songs (song_id,title,artist_id,year,duration)        
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (song_id) DO NOTHING
""")

# Query to insert records to our artist table
artist_table_insert = ("""
    INSERT INTO artists(artist_id,name,location,latitude,longitude)
    VALUES (%s,%s,%s,%s,%s)   
    ON CONFLICT (artist_id) DO NOTHING
""")

# Query to insert records to our time table
time_table_insert = ("""
    INSERT INTO time(start_time,hour,day,week,month,year,weekday)        
    VALUES (%s,%s,%s,%s,%s,%s,%s)      
    ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS
# This song_select query finds the song ID and artist ID based on the title, artist name, and duration of a song.

song_select = ("""
    SELECT songs.song_id,artists.artist_id 
    FROM songs JOIN artists ON lower(songs.artist_id)=lower(artists.artist_id)
    WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s
""")

# QUERY LISTS
# Queries to create and delete all the tables of our database

create_table_queries = [
    songplay_table_create, 
    user_table_create, 
    song_table_create, 
    artist_table_create, 
    time_table_create]
                     
drop_table_queries = [
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop]