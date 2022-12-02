import random
import sys
import sqlite3 as sq
import pandas as pd
import numpy as np
from Utilities.utilities import *

# Develop and propagate workload table
# createWorkload(Cursor, Connection, int)
def createWorkload(cur, conn, num_wl):
    init(cur, conn)

    # Fetch from scenario table
    scenario_df = pd.read_sql_query('SELECT * FROM scenario', conn)

    for count in range(num_wl):
        # DEBUG
        print(f'Count: {count}')
        t_instances = {}
        # For each scenario create list of distributed instances
        row_id = 1
        for _, row in scenario_df.iterrows():
            task_id = row['task_id']
            start_time = float(row['start_time'])
            end_time = float(row['end_time'])
            num_of_tasks = int(row['num_of_tasks'])
            dist_id = row['dist_id']
            cur.execute(f"SELECT name FROM task_types WHERE task_id = '{task_id}';")
            task_name = cur.fetchall() # Returns list so must processess further
            try:
                task_name = task_name[0][0]
            except IndexError:
                print(
                    f'(workload.py) Unknown task_id found in scenario table: {task_id}\n'
                    'Double check scenario and task_types tables.\n'
                    'Skipping for now...'
                )
                continue

            # ***
            # print(f'(workload.py) dist_id: {dist_id}')

            t_instances[(task_id, row_id, task_name)] = \
                fetchArrivals(start_time, end_time, num_of_tasks, dist_id, cur)

            row_id += 1
            # ***
            # print(f'(workload.py) t_instances: {t_instances[(task_id, scenario_id, task_name)]}')

        # Create workload local list
        workload = []
        for scenario_key in t_instances:
            task_id = scenario_key[0]
            task_name = scenario_key[2]
            for arrival_time in t_instances[scenario_key]:
                entry = (task_id, task_name, arrival_time)
                workload.append(entry)

        # Sort list by arrival_time
        workload.sort(key = lambda x: x[2])

        # Needed to sort before adding work_id
        instance_id = 1
        for i in range(len(workload)):
            entry = workload[i]
            entry = (instance_id,) + entry
            workload[i] = entry
            instance_id += 1

        # Create workload schema
        workload_schema = f""" CREATE TABLE IF NOT EXISTS workload{count+1} (
            instance_id INT PRIMARY KEY,
            task_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            arrival_time FLOAT NOT NULL,

            FOREIGN KEY (task_id) REFERENCES task_types(task_id)
        ); """
        createSchema(cur, conn, workload_schema)

        # ***
        # for tuple in workload:
        #     for idx in tuple: print(f'{idx}: {type(idx)}')
        #     break
        # conn.close()
        # sys.exit()

        # Merge list into workload table
        insertData(cur, conn, workload, f'workload{count+1}')

# Creates list of task instances from distribution scheme
# [int] fetchArrivals(float, float, int, int, Cursor)
def fetchArrivals(start_time, end_time, num_of_tasks, dist_id, cur):
    cur.execute(f"SELECT name FROM distribution WHERE dist_id = '{dist_id}';")
    dist_name = cur.fetchall()
    dist_name = dist_name[0][0]

    scale = start_time + ((end_time - start_time) / 2)

    # Should probably switch based on dist_id
    if dist_name == 'exponential':
        return np.random.exponential(scale, num_of_tasks)

def init(cur, conn):
    # Obtain list of all tables
    cur.execute("SELECT * FROM sqlite_master where type = 'table';")
    tables_raw = cur.fetchall()
    tables = []
    for table in tables_raw: tables.append(table[1])

    # Delete all previous workload tables
    for table in tables:
        if 'workload' in table:
            deleteTables(cur, conn, table)