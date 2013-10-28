
import MySQLdb
import sys
import pp


def insertdb(n):
    #insert into mydb
    db = MySQLdb.connect(passwd="1234", db="mydb", user="root")
    c = db.cursor()
    c.execute("""INSERT INTO test (num) VALUES (%s)""" ,(n))
    db.commit()
    c.close()
    db.close()
    return n

def insert():
    num = input("insert num ? ")
    
    if num < 10:
        job = job_server1.submit(insertdb, (num, ), (), ("MySQLdb", ))
        result = job()
        print "insert num is ", result, '\n'
        print "job_server1"
        job_server1.print_stats()
    else:        
        job = job_server2.submit(insertdb, (num, ), (), ("MySQLdb", ))
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

def select():
    num = input("select num ? ")
    
    if num < 10:
        job = job_server1.submit(selectdb, (num, ), (), ("MySQLdb", ))
        result = job()
        print "select num is ", result, '\n'
        print "job_server1"
        job_server1.print_stats()
    else:        
        job = job_server2.submit(selectdb, (num, ), (), ("MySQLdb", ))
        result = job()
        print "select num is ", result, '\n'
        print "job_server2"
        job_server2.print_stats()



print """           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            pp mysqldb insert test
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""

# tuple of all parallel python servers to connect with
#ppservers = ("127.0.0.1:60000", )
ppservers1 = ("192.168.56.102:1234", )
ppservers2 = ("192.168.56.103:2345", )

job_server1 = pp.Server(ncpus=0, ppservers=ppservers1)
job_server2 = pp.Server(ncpus=0, ppservers=ppservers2)

print sys.argv[0]
print len(sys.argv)

while True:
    ch = raw_input( "insert(i) or select(s) or exit(e)" )
    if ch == 'i':
        insert()
    elif ch == 's':
        select()
    elif ch == 'e':
        break

print "job_server1"
job_server1.print_stats()
print "job_server2"
job_server2.print_stats()