'''
Create database from csv files and accept user input.
Input is used to propagate test environment.
'''

import argparse
import os
import sqlite3 as sq
import pandas as pd
import workload as wl
from utilities import createSchema, fetchData, insertData

DEBUG = False
CURR_PATH = os.getcwd()

def main():
    db_path = CURR_PATH + '/Data/e2cDB.db'
    conn = sq.connect(db_path)
    cur = conn.cursor()

    createSchema(cur, conn)

    # with conn: # alternative to committing

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cur.fetchall())

    conn.close()

def debugMain():
    db_path = CURR_PATH + '/Data/e2cDB.db'
    conn = sq.connect(db_path)
    cur = conn.cursor()

    # Set up schema
    createSchema(cur, conn)

    # Fetch data from .csv
    scenario_path = CURR_PATH + '/Data/testScenario.csv'
    scenario_data = fetchData(scenario_path)

    # Convert list to DB entries
    insertData(cur, conn, scenario_data)

    # Create and propagate workload table
    wl.createWorkload(cur, conn)

    conn.close()

def init():
    global DEBUG

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', help = "run a short debug test", action = "store_true")
    args = parser.parse_args()

    if args.debug:
        DEBUG = True
        print('Running in debug mode.')

if __name__ == '__main__':
    init()

    if DEBUG == True: debugMain()
    else: main()