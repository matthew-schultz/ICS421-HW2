# runSQL.py
# import configparser
import multiprocessing
import pickle
import socket
import sqlite3
import sys
# import Error

class SQLDriver:
    '''
    Parameters
    caller_file : the name of the file that instantiated this class
    clustercfg  : the name of the config file for the database cluster

    this function also creates a dictionary from the database cluster config file
    '''
    def __init__(self, caller_file, clustercfg):
        self.caller_file = caller_file 
        self.cfg_dict = self.get_cfg_dict(clustercfg)

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

    def get_cat_db(self):
        return self.cfg_dict['catalog.db']

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


    def multiprocess_node_sql(self, node_sql, cat_db_name):   
        # create a pool of resources, allocating one resource for each node
        pool = multiprocessing.Pool(int(self.cfg_dict['numnodes']))
        node_sql_response = []
        for current_node_num in range(1, int(self.cfg_dict['numnodes']) + 1):
            db_host = self.cfg_dict['node' + str(current_node_num) + '.hostname']
            db_port = int(self.cfg_dict['node' + str(current_node_num) + '.port'])
            node_db = self.cfg_dict['node' + str(current_node_num) + '.db']
            node_sql_response.append(pool.apply_async(self.send_node_sql, (node_sql, db_host, db_port, current_node_num, cat_db_name, node_db, )))
        for current_node_num in range(1, int(self.cfg_dict['numnodes']) + 1):
            node_sql_response.pop(0).get()


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


    def send_node_sql(self, ddlSQL, dbhost, dbport, nodeNum, catDbName, nodeDbName):
        print('runDDL.py: connecting to host ' + dbhost)

        mySocket = socket.socket()
        try:
            mySocket.connect((dbhost, dbport))
            listToBePickled = []
            listToBePickled.append(nodeDbName)
            listToBePickled.append(ddlSQL)
            data_string = pickle.dumps(listToBePickled)
            # packet = '<dbname>' + nodeDbName + '</dbname>' + ddlSQL
            # print('runDDL.py: send data "' + packet + '"')
            print('runDDL.py: send pickled data_array "' + '[%s]' % ', '.join(map(str, listToBePickled)) + '"')   
            # mySocket.send(packet.encode())
            mySocket.send(data_string)

            data = str(mySocket.recv(1024).decode())
            print('runDDL.py: recv ' + data + ' from host ' + dbhost)

            if(data == 'success'):
                tname = getTname(ddlSQL)
                # print('tname is ' + tname)
                catSQL = 'DELETE FROM dtables WHERE nodeid='+ str(nodeNum) + ';'            
                if SQLIsCreate(ddlSQL):
                    # print ('ddlSQL is a create statement')
                    # catSQL = 'TRUNCATE TABLE tablename;'
                    catSQL = 'INSERT INTO dtables VALUES ("'+ tname +'","","' + dbhost + '","","",0,' + str(nodeNum) + ',NULL,NULL,NULL)'
                RunSQL(catSQL, catDbName)
                # print('runDDL.py: ' + catSQL)
                # print('')
        except OSError:
            print('runDDL.py: failed to connect to host ' + dbhost)
        mySocket.close()
