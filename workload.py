import random
import sqlite3 as sq
import pandas as pd
import numpy as np
from utilities import createTable

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
        cur.execute(f"SELECT name FROM task_types WHERE task_id = {task_id};")
        task_name = cur.fetchall()

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

    # Create workload table
    createTable(cur, conn)

    # Merge list into workload table
    cur.executemany(
        'INSERT OR IGNORE INTO workload ' \
        '(work_id, task_id, scenario_id, name, arrival_time) ' \
        'VALUES (?, ?, ?, ?, ?);', workload
    )

def fetchArrivals(start_time, end_time, num_of_tasks, dist_id, cur):
    cur.execute(f"SELECT name FROM distribution WHERE dist_id = {dist_id};")
    dist_name = cur.fetchall()

    print(dist_name)

    scale = start_time + ((end_time - start_time) / 2)

    # Should probably switch based on dist_id
    if dist_name == 'exponential':
        instance_buff = np.random.exponential(scale, num_of_tasks)

    return instance_buff