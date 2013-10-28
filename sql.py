import MySQLdb



db = MySQLdb.connect(passwd="1234", db="mydb", user="root")
c = db.cursor()

n = input(" input : ");

c.execute("""SELECT num From test where num = %s""",(n,))

#print c.fetchone(),'\n'

print c.fetchall(),'\n'


c.execute("""INSERT INTO test (num) VALUES (%s)""" ,(5))
print c.fetchall(),'\n'
c.execute("""select * from test""")
print c.fetchall()
#db.commit()
c.close()
db.close()
