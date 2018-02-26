#loadCSV.py

import sys
import SQLDriver

def test_insert_dtables(sql_driver):
    insert_dtables = 'insert_dtables.sql'
    with open(insert_dtables, 'r') as myfile:
        data=myfile.read().replace('\n', '')
        # print(__file__ + ': data is ' + data)
        sql_driver.run_sql(data, 'mycatdb.db')

def test_run(sql_driver):
    dbname = 'mycatdb.db'
    tablename = 'dtables'
    valuename = 'partmtd'
    where_col = 'nodeid'
    where_val = 1
    value = sql_driver.select_value_from_db(dbname, valuename, tablename, where_col, where_val);
    print('value is: ' + value[1])

def tests(sql_driver):
#    test_insert_dtables(sql_driver):
    test_run(sql_driver)

def main():
    if(len(sys.argv) >= 3):
        try:
            print('executing loadCSV')
            clustercfg = sys.argv[1]
            csvfile = sys.argv[2]
            sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)

            # response = sql_driver.load_csv('','', csvfile)
            # print(__file__ + ": load_csv() response is " + str(response))            

            # def load_csv(self, db, table, csv):
            response_list = []
            tuples = sql_driver.get_tuples_from_csv(csvfile)

            for current_node_num in range(1, int(sql_driver.cfg_dict['numnodes']) + 1):
                partmtd = sql_driver.get_partmtd(current_node_num)
                print('partmtd is: ',partmtd)
                print('current_node_num is :', current_node_num)

            tests(sql_driver)

            # return response_list


        except FileNotFoundError as e:
            print(__file__ + ': ' + str(e))
    else:
        print(__file__ + ': ERROR need at least 2 arguments to run properly (e.g. \"python3 loadCSV.py cluster.cfg books.csv\"')

if __name__ == '__main__':
    main()
