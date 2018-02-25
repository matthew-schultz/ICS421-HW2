# runSQL.py
import multiprocessing
import pickle
import socket
import sqlite3
import sys
import SQLDriver
# import Error


def main():
    if(len(sys.argv) >= 3):
        print('executing runSQL')
        clustercfg = sys.argv[1]
        node_sql = sys.argv[2]

        sql_driver = SQLDriver.SQLDriver()
        cfg_dict = sql_driver.get_cfg_dict()

        #read ddlfile as a string to be executed as sql
        with open(node_sql, 'r') as myfile:
            ddlSQL=myfile.read().replace('\n', '')

    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cluster.cfg books.sql\"')

if __name__ == '__main__':
    main()
