'''
Create database from csv files and accept user input.
Input is used to propagate test environment.
'''

import os
import sqlite3 as sq
import pandas as pd
import workload as wl
from utilities import createSchema, fromCSV, insertData, printTable, deleteTables

CURR_PATH = os.getcwd()

def main():
    db_path = CURR_PATH + '/Data/e2cDB.db'
    conn = sq.connect(db_path)
    cur = conn.cursor()

    # Remove all tables just b/c
    cur.execute("SELECT * FROM sqlite_master where type = 'table';")
    tables_raw = cur.fetchall()
    tables = []
    for table in tables_raw: tables.append(table[1])
    deleteTables(cur, conn, tables)

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
                task_id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                detail VARCHAR(255) NOT NULL,
                urgency FLOAT NOT NULL
    ); """

    # Expected Execution Time
    # Derived from task_types & machine_types.
    # Each entry contained expected execution time of task type on given
    # machine type.
    eet = """ CREATE TABLE IF NOT EXISTS eet (
                task_id VARCHAR(255) NOT NULL,
                machine_id INT NOT NULL,
                expected_ex_time FLOAT NOT NULL,

                FOREIGN KEY (task_id) REFERENCES task_types(task_id),
                FOREIGN KEY (machine_id) REFERENCES machine_types(machine_id)
    ); """

    # Scenario
    # Characterization of distribution of tasks.
    scenario = """ CREATE TABLE IF NOT EXISTS scenario (
                scenario_id INT PRIMARY KEY,
                task_id VARCHAR(255) NOT NULL,
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

    # --- scenario ---
    # Fetch data from scenario.csv
    scenario_path = CURR_PATH + '/Data/Testing/testScenario.csv'
    scenario_data = fromCSV(scenario_path)

    # Convert list to DB entries
    insertData(cur, conn, scenario_data, 'scenario')

    # --- tast_types ---
    task_types_path = CURR_PATH + '/Data/Testing/testTasks.csv'
    task_types_data = fromCSV(task_types_path)

    insertData(cur, conn, task_types_data, 'task_types')

    # --- distribution ---
    distribution_path = CURR_PATH + '/Data/Testing/testDistribution.csv'
    distribution_data = fromCSV(distribution_path)

    insertData(cur, conn, distribution_data, 'distribution')

    # --- machine_types ---
    machine_types_path = CURR_PATH + '/Data/Testing/testMachines.csv'
    machine_types_data = fromCSV(machine_types_path)

    insertData(cur, conn, machine_types_data, 'machine_types')

    # Create and propagate workload table
    wl.createWorkload(cur, conn)

    printTable(cur, 'workload')

    conn.close()

if __name__ == '__main__': main()