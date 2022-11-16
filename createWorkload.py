import argparse
import os
import sqlite3 as sq
import pandas as pd

DEBUG = False
CURR_PATH = os.getcwd()

def main():
    conn = sq.connect("e2cDB.db")
    cur = conn.cursor()

    conn.create_function("WORKLOAD", 1, _workload)

def _workload(seed):
    # Fetch from scenario table and propagate workload table
    print()

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