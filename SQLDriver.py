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
        if clustercfg is not None:
            self.cfg_dict = self.get_cfg_dict(clustercfg)

    def create_catalog(self, dbname):
        sqlConn = sqlite3.connect(dbname)
        c = sqlConn.cursor()
        # cat_sql = 'DROP TABLE dtables;\n'
        cat_sql = '''CREATE TABLE IF not exists dtables(tname char(32), 
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
            c.execute(cat_sql)
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

    def get_table_name(self, data):
        tname = ''
        
        dataArray = data.split(' ')
        count = 0
        for d in dataArray:
            if d.upper() == 'TABLE':
                # check if table name will come after an 'if exists...' statement
                if dataArray[count + 1] == 'if':
                     tname = dataArray[count+4]
                else: tname = dataArray[count + 1]
            count = count + 1   
        tname = tname.split('(')[0] #remove trailing '('
        return tname


    def multiprocess_node_sql(self, node_sql, cat_db):   
        # create a pool of resources, allocating one resource for each node
        pool = multiprocessing.Pool(int(self.cfg_dict['numnodes']))
        node_sql_response = []
        for current_node_num in range(1, int(self.cfg_dict['numnodes']) + 1):
            db_host = self.cfg_dict['node' + str(current_node_num) + '.hostname']
            db_port = int(self.cfg_dict['node' + str(current_node_num) + '.port'])
            node_db = self.cfg_dict['node' + str(current_node_num) + '.db']
            node_sql_response.append(pool.apply_async(self.send_node_sql, (node_sql, db_host, db_port, current_node_num, cat_db, node_db, )))
        for current_node_num in range(1, int(self.cfg_dict['numnodes']) + 1):
            node_sql_response.pop(0).get()


    def run_sql(self, sql, dbname):
        print(self.caller_file + ': executing sql statement ' + sql) 
        try:
            sqlConn = sqlite3.connect(dbname)
            c = sqlConn.cursor()
#            c.execute(sql)
            result = ''
            for row in c.execute(sql):
                print(str(row))
                result += str(row) + '\n'
#            result = str(c.fetchall())
            sqlConn.commit()
            sqlConn.close()
            # create sql_result to store if sql was success and rows
            sql_result = []
            sql_result.append('success')
            sql_result.append(result)
            return result
        except sqlite3.IntegrityError as e:
            print(e)
            sql_result = []
            sql_result.append('failure')
            sql_result.append('')
            return sql_result
        except sqlite3.OperationalError as e:
            print(e)
            sql_result = []
            sql_result.append('failure')
            sql_result.append('')
            return sql_result

    def send_node_sql(self, node_sql, dbhost, dbport, node_num, cat_db, node_db):
        print(self.caller_file + ' connecting to host ' + dbhost)

        my_socket = socket.socket()
        try:
            my_socket.connect((dbhost, dbport))
            req_to_pickle = []
            req_to_pickle.append(node_db)
            req_to_pickle.append(node_sql)
            data_string = pickle.dumps(req_to_pickle)
            print(self.caller_file + ' send pickled data_array "' + '[%s]' % ', '.join(map(str, req_to_pickle)) + '"')   
            # my_socket.send(packet.encode())
            my_socket.send(data_string)

            data = str(my_socket.recv(1024).decode())

            # return from Main() if no data was received
            if not data:
                return
            data_arr = pickle.loads(data)
            print(caller_file + ': Received' + repr(data_arr))

            dbfilename = data_arr[0]

            print(self.caller_file + ' recv ' + data_arr[0] + ' from host ' + dbhost)
            print(self.caller_file + ' recv ' + data_arr[1] + ' from host ' + dbhost)

            # get response list from parDBd

            if(data_arr[0] == 'success'):
                tname = self.get_table_name(node_sql)
                # print('tname is ' + tname)
                cat_sql = 'DELETE FROM dtables WHERE nodeid='+ str(node_num) + ';'            
                if self.table_is_created(node_sql):
                    # print ('node_sql is a create statement')
                    # cat_sql = 'TRUNCATE TABLE tablename;'
                    cat_sql = 'INSERT INTO dtables VALUES ("'+ tname +'","","' + dbhost + '","","",0,' + str(node_num) + ',NULL,NULL,NULL)'
                self.run_sql(cat_sql, cat_db)
                # print(self.caller_file + ' ' + cat_sql)
                # print('')
        except OSError:
            print(self.caller_file + ' failed to connect to host ' + dbhost)
        my_socket.close()


    def table_is_created(self, sql):
        isInsert = False
        # remove leading whitespace from sql string and split
        sqlArray = sql.lstrip().split(" ")
        if sqlArray[0].upper() == 'CREATE':
            isInsert = True
        return isInsert
