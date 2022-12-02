'''
Convert scenario .csv files into singular scenario DB table.
'''

import os
import sqlite3 as sq
from Utilities.utilities import *

OP_TABLE = 'scenario'
CURR_PATH = os.getcwd()

db_path = CURR_PATH + '/Data/e2cDB.db'
conn = sq.connect(db_path)
cur = conn.cursor()

# Remove scenario table (easier than truncating)
deleteTables(cur, conn, OP_TABLE)

# Create new scenario table
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
createSchema(cur, conn, scenario)

scenario_dir = CURR_PATH + '/../E2C-Sim/V1/workloads/scenarios'

# Go through scenario files
data = []
scenario_id = 1 # Adds new PK attribute for each scenario entry
for scenario_file in os.listdir(scenario_dir):
    scenario_file = os.path.join(scenario_dir, scenario_file)
    if os.path.isdir(scenario_file): continue
    
    # Dump all lines to buffer of tuples
    data_buff = fromCSV(scenario_file)

    # Append data collective with buffer
    for entry in data_buff:
        entry = (str(scenario_id),) + entry
        data.append(entry)
        scenario_id += 1

# Insert each data into scenario table
insertData(cur, conn, data, OP_TABLE)