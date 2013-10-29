
import MySQLdb
import sys
import pp
import threading
from multiprocessing import Process

def insertdb(n):
    #insert into mydb
    db = MySQLdb.connect(passwd="1234", db="mydb", user="root")
    c = db.cursor()
    c.execute("""INSERT INTO test (num) VALUES (%s)""" ,(n))
    db.commit()
    c.close()
    db.close()
    return n

def insert(n):
    if n < 100:
        job = job_server1.submit(insertdb, (n, ), (), ("MySQLdb", ))
        result = job()
        print "insert num is ", result, '\n'
        print "job_server1"
        job_server1.print_stats()
    else:        
        job = job_server2.submit(insertdb, (n, ), (), ("MySQLdb", ))
        result = job()
        print "insert num is ", result, '\n'
        print "job_server2"
        job_server2.print_stats()


def selectdb(n):  
    #insert from mydb      
    db = MySQLdb.connect(passwd="1234", db="mydb", user="root")
    c = db.cursor()
    c.execute("""SELECT num From test where num = %s""", (n,))
    r = c.fetchall()
    c.close()
    db.close()
    return r

def select(n):
    if n < 100:
        job = job_server1.submit(selectdb, (n, ), (), ("MySQLdb", ))
        result = job()
        print "select num is ", result, '\n'
        print "job_server1"
        job_server1.print_stats()
    else:        
        job = job_server2.submit(selectdb, (n, ), (), ("MySQLdb", ))
        result = job()
        print "select num is ", result, '\n'
        print "job_server2"
        job_server2.print_stats()

def threadInsert(arr):
    print arr
    for i in range(len(arr)):
        print arr[i]
        t = threading.Thread(target=insert, args=(arr[i],))
        t.start()

def threadSelect(arr):    
    for i in range(len(arr)):
        t = threading.Thread(target=select, args=(arr[i],))
        t.start()

if __name__ == '__main__':
    
    print """           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                pp mysqldb insert test
            ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

    # tuple of all parallel python servers to connect with
    #ppservers = ("127.0.0.1:60000", )
    ppservers1 = ("192.168.56.101:1234", )
    #ppservers2 = ("192.168.56.103:2345", )
    ppservers2 = ("192.168.56.102:1234", )

    job_server1 = pp.Server(ncpus=0, ppservers=ppservers1,)
    job_server2 = pp.Server(ncpus=0, ppservers=ppservers2,)

    while True:
        ch = raw_input( "insert(i) or select(s) or exit(e)" )
        if ch == 'i':
            arr = []
            for i in range(5):
                num = input("\n insert num ? ")
                arr.append(num)
            threadInsert(arr)            
    #       thread.start_new_thread(insert, )
        elif ch == 's':
            arr = []
            for i in range(5):
                num = input("\n select num ? ")
                arr.append(num)
            threadSelect(arr)
    #        thread.start_new_thread(select, )
        elif ch == 'e':
            break

    print "job_server1"
    job_server1.print_stats()
    print "job_server2"
    job_server2.print_stats()


