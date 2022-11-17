import random
import sqlite3 as sq
import pandas as pd
import numpy as np
from utilities import createSchema, insertData

def createWorkload(cur, conn):
    # Fetch from scenario table
    scenario_df = pd.read_sql_query('SELECT * FROM scenario', conn)

    t_instances = {}
    # For each scenario create list of distributed instances
    for _, row in scenario_df.iterrows():
        scenario_id = row['scenario_id']
        task_id = row['task_id']
        start_time = float(row['start_time'])
        end_time = float(row['end_time'])
        num_of_tasks = int(row['num_of_tasks'])
        dist_id = row['dist_id']
        cur.execute(f"SELECT name FROM task_types WHERE task_id = '{task_id}';")
        task_name = cur.fetchall()

        print(f'\n\n{task_name}\n\n')

        t_instances[(task_id, scenario_id, task_name)] = \
            fetchArrivals(start_time, end_time, num_of_tasks, dist_id, cur)

    # Create workload local list
    workload = []
    work_id = 1
    for scenario_key in t_instances:
        for arrival_time in t_instances[scenario_key]:
            entry = (work_id,) + scenario_key + (arrival_time,)
            workload.append(entry)
            work_id += 1

    # Sort list by arrival_time
    workload.sort(key = lambda x: x[4])

    # Create workload schema
    workload_schema = """ CREATE TABLE IF NOT EXISTS workload (
        work_id INT PRIMARY KEY,
        task_id INT NOT NULL,
        scenario_id FLOAT NOT NULL,
        name VARCHAR(255) NOT NULL,
        arrival_time FLOAT NOT NULL,

        FOREIGN KEY (task_id) REFERENCES task_types(task_id),
        FOREIGN KEY (scenario_id) REFERENCES scenario(scenario_id)
    ); """
    cur.execute('DROP TABLE IF EXISTS workload;')
    conn.commit()
    createSchema(cur, conn, workload_schema)

    # Merge list into workload table
    insertData(cur, conn, workload, 'workload')

def fetchArrivals(start_time, end_time, num_of_tasks, dist_id, cur):
    cur.execute(f"SELECT name FROM distribution WHERE dist_id = '{dist_id}';")
    dist_name = cur.fetchall()

    print(dist_name)

    scale = start_time + ((end_time - start_time) / 2)

    # Should probably switch based on dist_id
    if dist_name == 'exponential':
        return np.random.exponential(scale, num_of_tasks)