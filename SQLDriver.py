# runSQL.py
# import configparser
import csv
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
            # print(caller_file + ': cfg_dict is: ' + str(self.cfg_dict) )
            # print('dict fields :')
            # for x in self.cfg_dict:
            #    print(x,':',self.cfg_dict[x])

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

    def update_catalog(nodeid, partcol, partparam1, partparam2, partmtd):
        print('')
        

    def get_node_dict_from_catalog(self, nodenum):
        node_dict = []        
        # partmtd_sql = 'SELECT partmtd from dtables WHERE nodeid=' + nodenum + ';'
        # partmtd = select_value_from_db(self, dbname, tablename, valuename, where_col, where_val)
        # node_dict.append(partmtd)

        return []

    '''
    Need to include '' in where_val arg string if that db column is of string type
    Arguments are in sql order "SELECT valuename FROM tablename WHERE where_col = where_val;"
    '''
    def select_value_from_db(self, dbname, valuename, tablename, where_col, where_val):
        value = ''
        select_sql = 'SELECT ' + valuename + ' FROM ' + tablename + ' WHERE ' \
        + where_col + '= ' + str(where_val) + ';'

        sql_result = self.run_sql(select_sql, dbname)
        if(sql_result[0] == 'success'):
            value = sql_result[1]
        print('select was ', value)
        return value

    def get_partmtd(self, nodenum):
        dbname = self.cfg_dict['catalog.db'] #MAGIC VALUE FOR NOW; CAN THIS BE CHANGED/IS THIS ROBUST?         
        tablename = 'dtables'
        valuename = 'partmtd'
        where_col = 'nodeid'
        partmtd = self.select_value_from_db(dbname, valuename, tablename, where_col, nodenum)
        return partmtd

    def get_tuples_from_csv(self, csv_filename):
        tuples = []
        with open(csv_filename, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter='\n')
            for row in reader:
                tuples.append(row)
        return tuples

    '''def compare_num_nodes_partitions(self):
        if(cfg_dict['numnodes'] != cfg_dict[]):
            print('')
'''            # throw error

    def partition_all(self, tuples):
        for sql_tuple in tuples:
            # insert = self.insert_tuple(self.cfg_dict['tablename'], sql_tuple)
            insert_sql = 'INSERT into ' + self.cfg_dict['tablename'] + ' VALUES(' + sql_tuple[0] + ');'
            print('insert is ',insert_sql)
            # print('idx is ' + str(idx))
            

    def insert_tuple(self, tablename, sql_tuple):            
        # values_string = ''
        # print('tup is' + sql_tuple[0])
        insert_sql = 'INSERT into ' + tablename + ' VALUES(' + sql_tuple[0] + ');'
        # self.run_sql(insert_sql, tablename + '.db')
        return insert_sql



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
                print('str is ' + str(row))
                result += str(row) + '\n'
#            result = str(c.fetchall())
            sqlConn.commit()
            sqlConn.close()
            # create sql_result to store if sql was success and rows
            sql_result = []
            sql_result.append('success')
            sql_result.append(result)
            # print(self.caller_file + ': returning results of sql statement ' + sql) 
            return sql_result
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

            data = my_socket.recv(1024)
            data_arr = pickle.loads(data)

            # return from Main() if no data was received
            if not data_arr:
                return
            # data_arr = pickle.loads(data)
            print(self.caller_file + ': Received' + repr(data_arr))

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
