import random
import sqlite3 as sq
import pandas as pd

def createWorkload(cur, conn):
    # Fetch from scenario table
    scenario_df = pd.read_sql_query('SELECT * FROM scenario', conn)

    # For each task type create list of distributed instances

    # Create (truncate?) workload table

    # Merge lists into workload table