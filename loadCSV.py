#loadCSV.py

import sys
import SQLDriver


def main():
    if(len(sys.argv) >= 3):
        print('executing loadCSV')
        clustercfg = sys.argv[1]
        node_sql = sys.argv[2]
        sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)
    else:
          print(__file__ + ': ERROR need at least 2 python arguments to run properly (e.g. \"python3 loadCSV.py cluster.cfg books.csv\"')

if __name__ == '__main__':
    main()
