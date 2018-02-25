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
#        cat_db_name = cfg_dict['catalog.db']
#        cat_msg = sql_driver.create_catalog(cat_db_name)
#        print(__file__ + ': create_catalog() returned "' + cat_msg + '"')
'''        #create a pool of resources, allocating one resource for each node
        pool = multiprocessing.Pool(int(cfg_dict['numnodes']))
        node_sql_response = []
        for current_node_num in range(1, int(cfg_dict['numnodes']) + 1):
            db_host = cfg_dict['node' + str(current_node_num) + '.hostname']
            db_port = int(cfg_dict['node' + str(current_node_num) + '.port'])
            node_db = cfg_dict['node' + str(current_node_num) + '.db']
            node_sql_response.append(pool.apply_async(sql_driver.send_node_sql, (node_sql, db_host, db_port, current_node_num, cat_db_name, node_db, )))
        for current_node_num in range(1, int(cfg_dict['numnodes']) + 1):
            node_sql_response.pop(0).get()'''
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runSQL.py cluster.cfg books.sql\"')

if __name__ == '__main__':
    main()
