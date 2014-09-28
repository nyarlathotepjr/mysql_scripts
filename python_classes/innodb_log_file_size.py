import MySQLdb
import re 
import time 

connection = MySQLdb.connect(read_default_group='client')
cursor = connection.cursor() 

cursor.execute("SHOW /*!50000 ENGINE*/ INNODB STATUS")
innodb_status_raw = cursor.fetchone()[2]
innodb_l = innodb_status_raw.split("\n")
crit = re.compile('Log sequence number')

for i in innodb_l: 
 if re.match(crit,i):
  lsn = i.split()

flsn =  lsn[3] 

cursor.execute("SET wait_timeout = 120")

time.sleep(60) 
cursor.execute("SHOW /*!50000 ENGINE*/ INNODB STATUS")

innodb_status_raw = cursor.fetchone()[2]
innodb_l = innodb_status_raw.split("\n")
crit = re.compile('Log sequence number')

for i in innodb_l:
 if re.match(crit,i):
  lsn = i.split()

slsn = lsn[3]

mbpermin = (float(slsn) - float(flsn))/ 1024.0 / 1024.0

print 'The MB written per minute to the redo logs is: %s' % mbpermin

hour_redo_size = float(mbpermin) * 60

print 'The redo log size for an hour of writes in MB is: %s' % hour_redo_size
