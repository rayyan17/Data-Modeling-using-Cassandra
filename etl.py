"""ETL Pipeline"""
import csv
import glob
import os

from cassandra.cluster import Cluster

from nosql_queries import create_table_queries, insert_into_music_app, insert_into_top_songs, insert_into_user_song, \
    drop_table_queries


def get_file_paths():
    """

    Get all the files in event folder

    Returns:
        file_path_list (list): list of all the files in your directory

    """
    print(os.getcwd())

    filepath = os.getcwd() + '/event_data'
    file_path_list = glob.glob(os.path.join(filepath, '*'))

    return file_path_list


def transform_multiple_files_to_single_file():
    """

    Fetch all the event files, extract important columns and combine them into 1 single file
    """
    full_data_rows_list = []

    for f in get_file_paths():
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                # print(line)
                full_data_rows_list.append(line)

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length',
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


def create_cassandra_cluster():

    cluster = Cluster()
    session = cluster.connect()

    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")

    session.set_keyspace('udacity')

    return cluster, session


def create_tables(session):
    for create_table in create_table_queries:
        session.execute(create_table)


def drop_tables(session):
    for drop_table in drop_table_queries:
        session.execute(drop_table)


def insert_into_tables(session):
    file = 'event_datafile_new.csv'
    with open(file, encoding='utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # skip header
        for line in csvreader:
            session.execute(insert_into_music_app, (line[0], line[9], float(line[5]), int(line[8]), int(line[3])))
            session.execute(insert_into_user_song, (line[0], line[9], line[1], line[4],
                                                    int(line[3]), int(line[10]), int(line[8])))

            session.execute(insert_into_top_songs, (line[9], line[1], line[4]))


def main():
    transform_multiple_files_to_single_file()
    cluster, session = create_cassandra_cluster()

    create_tables(session)
    insert_into_tables(session)
    drop_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == '__main__':
    main()
