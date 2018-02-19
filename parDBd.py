#parDBD.py
import pickle
import socket
import sqlite3
import sys
#import sqlite3.OperationalError


# Create table
def CreateTable(dbfilename, ddlSQL):
    sqlConn = sqlite3.connect(dbfilename)
    c = sqlConn.cursor()
    tableCreatedMsg = ''
    try:
       c.execute(ddlSQL)
    except sqlite3.OperationalError as e:
       print(e)
       tableCreatedMsg = 'failure'
    else:
        tableCreatedMsg = 'success'
    c.close()
    sqlConn.commit()
    sqlConn.close()
    return tableCreatedMsg


def Main():
    if(len(sys.argv) >= 3):
        host = sys.argv[1]
        #host = ''
        port = int(sys.argv[2])

        mySocket = socket.socket()
        mySocket.bind((host,port))
        mySocket.listen(1)
        runDDLConn, addr = mySocket.accept()
        print ("parDBd: Connection from " + str(addr))
        data = runDDLConn.recv(1024)
        # return from Main() if no data was received
        if not data:
            return
        data_arr = pickle.loads(data)
        print('Received' + repr(data_arr))

        dbfilename = data_arr[0]
        print('dbfilename is ' + dbfilename)
        ddlSQL = data_arr[1]
        print ('ddlSQL is ' + ddlSQL)

        response = CreateTable(dbfilename, ddlSQL)
        print(response)

        print ('parDBd: send response "' + str(response) +  '" for sql "' + str(ddlSQL) + '"')
        runDDLConn.send(response.encode())

        runDDLConn.close()
        mySocket.close()
    else:
        print("parDBd: ERROR need at least 3 arguments to run properly (e.g. \"python3 parDBd.py 171.0.0.2 5000\"")


if __name__ == '__main__':
    try:
        Main()
    except OSError as e:
        print('failed due to OSError; please retry in a minute\n' + str(e))
