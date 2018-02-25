# runSQL.py
# import configparser
import multiprocessing
import pickle
import socket
import sqlite3
import sys
# import Error

class SQLDriver:
    def __init__(self, caller_file, cfg_dict):
        self.caller_file = caller_file
        self.cfg_dict = get_cfg_dict(self, clustercfg)
        print(self.caller_file)

    def create_catalog(self, dbname):
        sqlConn = sqlite3.connect(dbname)
        c = sqlConn.cursor()
        # catSQL = 'DROP TABLE dtables;\n'
        catSQL = '''CREATE TABLE IF not exists dtables(tname char(32), 
                nodedriver char(64), 
                nodeurl char(128), 
                nodeuser char(16), 
                nodepasswd char(16), 
                partmtd int, 
                nodeid int, 
                partcol char(32), 
                partparam1 char(32),
                partparam2 char(32),
                CONSTRAINT unique_nodeid UNIQUE(nodeid))'''
        # Create table
        tableCreatedMsg = ''
        try:
            c.execute(catSQL)
        except Error:
            tableCreatedMsg = 'failure'
        else:
            tableCreatedMsg = 'success'
        sqlConn.commit()
        sqlConn.close()
        return tableCreatedMsg

    def _get_cfg_dict(self):
        print('getting cfg_dict')

    def get_cfg_dict(self, clustercfg):
        print(self.caller_file +': reading config file "' + clustercfg + '"')
        file = open(clustercfg)
        content = file.read()
        config_array = content.split("\n")
        # configList = []
        config_dict = {}
        for config in config_array:
            if config:
                c = config.split("=")
                # print (c[0] + ' is ' + c[1])
                config_key = c[0]
                configValue = c[1]
                if(('node' in config_key or 'catalog' in config_key) and 'hostname' in config_key):
                    #print('config_key has node hostname')
                    nodename = config_key.split(".")[0]
                    hostname = configValue.split(":")
                    configIP = hostname[0]
                    configPort = hostname[1].split("/")[0]
                    configDb = hostname[1].split("/")[1]
                    '''print('nodename is ' + nodename)
                    print('configValue is ' + configValue)
                    print('configIP is ' + configIP)
                    print('configPort is ' + configPort)
                    print('configDb is ' + configDb)'''        
                    config_dict[nodename + '.port'] = configPort
                    config_dict[nodename + '.db'] = configDb + '.db'
                    configValue = configIP
                config_dict[config_key]=configValue
            '''#configList.append(c)[1]
                    print (c[0] + '=' + c[1])
                    config_dict[c[0]] = c[1]'''
        print(self.caller_file +': config file "' + clustercfg + '" read successfully')
        file.close()
        return config_dict

    def multiprocess_node_sql():


    def run_sql(self, sql, dbname):
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

    def send_node_sql(self, )
