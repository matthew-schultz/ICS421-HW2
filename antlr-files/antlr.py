import sys
from antlr4 import *
from SQLiteLexer import SQLiteLexer
from PrintSQLiteListener import PrintSQLiteListener
from SQLiteParser import SQLiteParser
 
def getTableName(sqlString):
    tableName = ""
    return tableName

def main(argv):
    input = FileStream(argv[1])
    lexer = SQLiteLexer(input)
    stream = CommonTokenStream(lexer)
    parser = SQLiteParser(stream)
    tree = parser.sql_stmt()
#    output = open("sqlOutput.txt","w")
    
    SQLite = PrintSQLiteListener()
    walker = ParseTreeWalker()
    walker.walk(SQLite, tree)    
#    output.close()      

def test(argv):
    print("this is a test")
 
if __name__ == '__main__':
    if(len(sys.argv) >= 2):
        main(sys.argv)
    else:
        print(__file__ + ': ERROR need at least 2 arguments to run properly (e.g. \"python3 antlr.py books.sql\"')
#    test(sys.argv)
