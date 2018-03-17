#!/usr/bin/python
import psycopg2
import csv
import time
import datetime

fp = open('roadNet-TX.txt')
reader = csv.reader(filter(lambda row: row[0]!='#', fp), delimiter='\t')

# Opening CSV file 

fp = open('roadNet-TX.txt')
reader = csv.reader(filter(lambda row: row[0]!='#', fp), delimiter='\t')

# Connecting to postgres. 
#Attributes: host, user,password and database may vary in different systems 

database = psycopg2.connect( host="localhost", user="postgres", password="postgres", database="postgres")

#Inserting Data to table, counting elapsed time 

print "Loading data..."
start_time = time.time()

cursor = database.cursor()
cursor.execute("create table road(FromNodeId integer,ToNodeId integer)")

for row in reader:
    cursor.execute("INSERT INTO road VALUES (%s,%s);",row)

elapsed_time= time.time() - start_time
print "Time elapsed:"
print str(datetime.timedelta(seconds=elapsed_time))

#Executing and calculating time for queries specified

database.commit()
queries=[]

queries.append("SELECT count(*) FROM road;")
queries.append("""SELECT "fromnodeid", count(*) FROM road GROUP BY "fromnodeid" HAVING count(*)>8;""")
queries.append("""SELECT R."fromnodeid", count(*) FROM road R, road Q WHERE R."tonodeid"=Q."tonodeid"
GROUP BY R."fromnodeid" HAVING count(*)>8;""")

for i in queries:
    print "Executing command: " + i
    start_time = time.time()
    cursor.execute(i)
    elapsed_time= time.time() - start_time
print "Time elapsed:"
print str(datetime.timedelta(seconds=elapsed_time))
print

#Closing Descriptors

cursor.close()
database.commit()
database.close()
fp.close()
