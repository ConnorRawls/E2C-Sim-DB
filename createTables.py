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

    createSchema(cur, conn)

    # with conn: # alternative to committing

    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cur.fetchall())

    conn.close()

# Utilities
# --------------------------------------------------------------------
def createSchema(cur, conn):
    # Machine Types
    # Pre-defined table of machine types and their characteristics.
    machine_types = """ CREATE TABLE IF NOT EXISTS machine_types (
                machine_id INT PRIMARY KEY,
                no_of_replicas INT NOT NULL,
                idle_power FLOAT NOT NULL,
                max_power FLOAT NOT NULL,
                num_of_cores INT NOT NULL,
                cpu_clock FLOAT NOT NULL,
                memory FLOAT NOT NULL
    ); """

    # Task Types
    # Pre-defined table of task types and their characteristics.
    task_types = """ CREATE TABLE IF NOT EXISTS task_types (
                task_id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                urgency FLOAT NOT NULL
    ); """

    # Expected Execution Time
    # Derived from task_types & machine_types.
    # Each entry contained expected execution time of task type on given
    # machine type.
    eet = """ CREATE TABLE IF NOT EXISTS eet (
                task_id INT NOT NULL,
                machine_id INT NOT NULL,
                expected_ex_time FLOAT NOT NULL,

                FOREIGN KEY (task_id) REFERENCES task_types(task_id),
                FOREIGN KEY (machine_id) REFERENCES machine_types(machine_id)
    ); """

    # Scenario
    # Characterization of distribution of tasks.
    scenario = """ CREATE TABLE IF NOT EXISTS scenario (
                scenario_id INT PRIMARY KEY,
                task_id INT NOT NULL,
                start_time FLOAT NOT NULL,
                end_time FLOAT NOT NULL,
                num_of_tasks INT NOT NULL,
                dist_id INT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES task_types(task_id),
                FOREIGN KEY (dist_id) REFERENCES distribution(dist_id)
    ); """

    # Distribution
    # Possible distribution schemes for task scenarios.
    distribution = """ CREATE TABLE IF NOT EXISTS distribution (
                    dist_id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
    ); """

    cur.execute(machine_types)
    cur.execute(task_types)
    cur.execute(eet)
    cur.execute(scenario)
    cur.execute(distribution)
    conn.commit()

# Fetches data from .csv and converts to list of tuples
def fetchData(file_path):
    data = []

    with open(file_path, 'r') as file:
        next(file)
        lines = file.readlines()

        for line in lines:
            line = line.replace('\n', '')
            line_parsed = line.split(',')
            data.append(tuple(line_parsed))

    return data

def debugMain():
    conn = sq.connect("e2cDB.db")
    cur = conn.cursor()

    # Set up schema
    createSchema(cur, conn)

    scenario_path = CURR_PATH + '/testScenario.csv'
    scenario_data = fetchData(scenario_path)

    print(f'Length of scenario data: {len(scenario_data)}')
    print(f'Example entry: {scenario_data[0]}')

    cur.executemany(
        'INSERT INTO scenario ' \
        '(scenario_id, task_id, start_time, end_time, num_of_tasks, dist_id) ' \
        'VALUES (?, ?, ?, ?, ?, ?);', scenario_data
    )
    conn.commit()

    conn.close()

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

    if DEBUG == True: debugMain()
    else: main()