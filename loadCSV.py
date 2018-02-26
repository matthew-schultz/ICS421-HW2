#loadCSV.py

import sys
import SQLDriver


def main():
    if(len(sys.argv) >= 3):
        try:
            print('executing loadCSV')
            clustercfg = sys.argv[1]
            csvfile = sys.argv[2]
            sql_driver = SQLDriver.SQLDriver(__file__, clustercfg)

            response = sql_driver.load_csv('','', csvfile)
            print(__file__ + ": load_csv() response is " + str(response))
        except FileNotFoundError as e:
            print(__file__ + ': ' + str(e))
    else:
        print(__file__ + ': ERROR need at least 2 arguments to run properly (e.g. \"python3 loadCSV.py cluster.cfg books.csv\"')

if __name__ == '__main__':
    main()
