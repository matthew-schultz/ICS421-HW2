# runSQL.py
# import configparser
import multiprocessing
import pickle
import socket
import sqlite3
import sys
# import Error

class SQLDriver:
#    def __init__(self):
#        self.data = []

    def get_cfg_dict(self):
        print('getting cfg_dict')

    def run_sql(sql, dbname):
        print('runDDL.py: executing sql statement ' + sql) 
        try:
            sqlConn = sqlite3.connect(dbname)
            c = sqlConn.cursor()
            c.execute(sql)
            print(str(c.fetchall()))
            sqlConn.commit()
            sqlConn.close()
        except sqlite3.IntegrityError as e:
            print(e)
        except sqlite3.OperationalError as e:
            print(e)


