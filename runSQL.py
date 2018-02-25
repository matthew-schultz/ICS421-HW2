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

        sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)
#        cfg_dict = sql_driver.get_cfg_dict(clustercfg)

        #read ddlfile as a string to be executed as sql
        with open(node_sql, 'r') as myfile:
            node_sql = myfile.read().replace('\n', '')

        cat_db_name = sql_driver.get_cat_db()
        cat_msg = sql_driver.create_catalog(cat_db_name)
        print(__file__ + ': create_catalog() returned "' + cat_msg + '"')

        sql_driver.multiprocess_node_sql(node_sql, cat_db_name)
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cluster.cfg books.sql\"')

if __name__ == '__main__':
    main()
