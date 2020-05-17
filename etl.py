"""ETL Pipeline"""
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


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

