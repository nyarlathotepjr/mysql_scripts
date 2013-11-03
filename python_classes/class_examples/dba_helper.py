#!/usr/bin/python
import sys,re,logging,textwrap,MySQLdb, pprint, re, time 

class MySQLClient(object):


	def __init__(self, *args, **kargs):
	
		self.connection = MySQLdb.connect(*args, **kargs)
		self.cursor = self.connection.cursor()

	def flush_tables(self):
		
		cursor = self.cursor()
		cursor.execute('FLUSH /*!40101 LOCAL */ TABLES')
		cursor.close()

	def show_databases(self):
		
		sql = "SHOW DATABASES"
		db_list = self.cursor.execute(sql)
		db_list = self.cursor.fetchall()
		return db_list

	def process_list(self):
		sql = "show full processlist;"
		proc_list = self.cursor.execute(sql)
		rows = self.cursor.fetchall() 
		return rows 

	def storage_engine(self):
		sql = "select engine,sum(index_length+data_length)/1024/1024,count(engine) from information_schema.tables group by engine"
		engines = self.cursor.execute(sql)
		engines = self.cursor.fetchall()
		return engines 

	def global_status(self): 

		sql = "show global status"
		status = self.cursor.execute(sql)
		gstatus = dict(self.cursor.fetchall())
		return gstatus


	def innodb_waitfree(self):
		gstatus = self.global_status()
		return gstatus['Innodb_buffer_pool_wait_free']

	 
	def innodb_log_writes(self): 
		sql = "show engine innodb status"
		innodb_status_raw = self.cursor.execute(sql)
		innodb_status_raw = self.cursor.fetchone()[2]
		innodb_l = innodb_status_raw.split("\n")
		crit = re.compile('Log sequence number')
		
		for i in innodb_l:
 		 if re.match(crit,i):
  		  lsn = i.split()
		
		flsn =  lsn[3]

		time.sleep(60)
		innodb_status_raw = self.cursor.execute(sql)
                innodb_status_raw = self.cursor.fetchone()[2]
                innodb_l = innodb_status_raw.split("\n")

		for i in innodb_l:
                 if re.match(crit,i):
                  lsn = i.split()
		
		slsn = lsn[3]
		mbpermin = (float(slsn) - float(flsn))/ 1024.0 / 1024.0
		return mbpermin

