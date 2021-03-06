"""Script for All Data Manipulation Queries"""


# CREATE TABLE QUERIES

create_music_app_history = """
CREATE TABLE IF NOT EXISTS music_app_history \
(artist text, song text, song_length decimal, \
session_id int, item_in_session int, PRIMARY KEY (session_id, item_in_session))
"""


create_user_song_list = """
CREATE TABLE IF NOT EXISTS user_song_list \
(artist text, song text, user_first_name text, \
user_last_name text, item_in_session int, user_id int, \
session_id int, PRIMARY KEY (user_id, session_id, item_in_session))
"""


create_top_song_users = """
CREATE TABLE IF NOT EXISTS top_song_users \
(song text, user_first_name text, \
user_last_name text, PRIMARY KEY (song, user_first_name, user_last_name))
"""

# DROP TABLE QUERIES
drop_music_app_history = """DROP TABLE IF EXISTS music_app_history"""
drop_user_song_list = """DROP TABLE IF EXISTS user_song_list"""
drop_top_song_users = """DROP TABLE IF EXISTS top_song_users"""


# INSERT INTO TABLE QUERIES
insert_into_music_app = """
INSERT INTO music_app_history (artist, song, song_length, session_id, item_in_session) VALUES (%s, %s, %s, %s, %s)
"""

insert_into_user_song = """
INSERT INTO user_song_list (artist, song, user_first_name, user_last_name, item_in_session, user_id, session_id) \
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

insert_into_top_songs = """
INSERT INTO top_song_users (song, user_first_name, user_last_name) VALUES (%s, %s, %s)
"""


drop_table_queries = [drop_music_app_history, drop_user_song_list, drop_top_song_users]
create_table_queries = [create_music_app_history, create_user_song_list, create_top_song_users]

