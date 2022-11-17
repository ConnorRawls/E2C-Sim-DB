'''
Create database from csv files and accept user input.
Input is used to propagate test environment.
'''

import argparse
import os
import sqlite3 as sq
import pandas as pd
import workload as wl
from utilities import createSchema, fromCSV, insertData

CURR_PATH = os.getcwd()

def main():
    db_path = CURR_PATH + '/Data/e2cDB.db'
    conn = sq.connect(db_path)
    cur = conn.cursor()

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

    schemas = [
        machine_types,
        task_types,
        eet,
        scenario,
        distribution    
    ]

    # Set up schemas
    createSchema(cur, conn, schemas)

    # Fetch data from .csv
    scenario_path = CURR_PATH + '/Data/testScenario.csv'
    scenario_data = fromCSV(scenario_path)

    # Convert list to DB entries
    insertData(cur, conn, scenario_data, 'scenario')

    # Create and propagate workload table
    wl.createWorkload(cur, conn)

    conn.close()

if __name__ == '__main__': main()