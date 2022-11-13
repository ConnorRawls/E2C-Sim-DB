'''
Create database from csv files and accept user input.
Input is used to propagate test environment.
'''

import argparse
import os
import sqlite3 as sq
import pandas as pd

DEBUG = False
CURR_PATH = os.getcwd()

def main():
    conn = sq.connect("e2cDB.db")
    cur = conn.cursor()

    #just for testing purposes
    if DEBUG == True: debugTest(cur)

    #what else goes in the eet table??????? missing data here
    eet = """ CREATE TABLE eet (
                task_id INT NOT NULL,
                machine_id INT NOT NULL,

                FOREIGN KEY (task_id) REFERENCES task_type(task_id),
                FOREIGN KEY (machine_id) REFERENCES machine_type(machine_id)
    ); """

    machine_type = """ CREATE TABLE machine_type (
                machine_id INT PRIMARY KEY,
                no_of_replicas INT NOT NULL,
                idle_power FLOAT NOT NULL,
                max_power FLOAT NOT NULL,
                num_of_cores INT NOT NULL,
                cpu_clock FLOAT NOT NULL,
                memory FLOAT NOT NULL
    ); """

    #need to add the "detail" attribute, wtf is detail?
    task_type = """ CREATE TABLE task_type (
                task_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
            
                urgency FLOAT NOT NULL
    ); """

    scenario = """ CREATE TABLE scenario (
                scenario_id INT PRIMARY KEY,
                task_id INT NOT NULL,
                start_time FLOAT NOT NULL,
                end_time FLOAT NOT NULL,
                num_of_tasks INT NOT NULL,
                dist_id INT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES task_type(task_id),
                FOREIGN KEY (dist_id) REFERENCES distribution(dist_id)
    ); """

    distribution = """ CREATE TABLE distribution (
                    dist_id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
    ); """


    cur.execute(eet)
    cur.execute(machine_type)
    cur.execute(task_type)
    cur.execute(scenario)
    cur.execute(distribution)
    conn.commit()

    # with conn: # alternative to committing

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cur.fetchall())






















    conn.close()

# Utilities
# --------------------------------------------------------------------
def debugTest(cur):
    cur.execute("DROP TABLE IF EXISTS eet")
    cur.execute("DROP TABLE IF EXISTS machine_type")
    cur.execute("DROP TABLE IF EXISTS task_type")
    cur.execute("DROP TABLE IF EXISTS scenario")
    cur.execute("DROP TABLE IF EXISTS distribution")

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
    main()