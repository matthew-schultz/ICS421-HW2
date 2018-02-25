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
        ddlfile = sys.argv[2]

        sd = SQLDriver.SQLDriver()
        sd.get_cfg_dict()
    else:
          print(__file__ + ': ERROR need at least 3 arguments to run properly (e.g. \"python3 runDDL.py cluster.cfg plants.sql\"')

if __name__ == '__main__':
    main()
