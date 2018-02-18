#parDBD.py
import socket
import sqlite3
import sys
#import sqlite3.OperationalError

def GetDbFilename(packet):
    filename = ''
    filename = packet.split('</dbname>')[0]
    filename = filename.split('<dbname>')[1]
    return filename


def GetSQL(packet):
     sql = ''
     sql = packet.split('</dbname>')[1]
     return sql

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
        packet = runDDLConn.recv(1024).decode()
        if not packet:
            return
        print ("parDBd: recv " + str(packet))
        dbfilename = GetDbFilename(packet)
        print('dbfilename is ' + dbfilename)
        ddlSQL = GetSQL(packet)

        sqlConn = sqlite3.connect(dbfilename)
        c = sqlConn.cursor()

    # Create table
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

        response = tableCreatedMsg
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


