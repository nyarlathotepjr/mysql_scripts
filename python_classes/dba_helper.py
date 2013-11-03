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
		nslist = ['Compression','Flashcache_enabled','Slave_running','Innodb_have_atomic_builtins','Rpl_status','Ssl_session_cache_mode','Ssl_cipher','Ssl_cipher_list','Ssl_version']
		for iter in gstatus:
		  if iter not in nslist: 
		   float (gstatus[iter])

		return gstatus

	def global_variables(self):
		sql = "show global variables"
		variables = self.cursor.execute(sql)
		gvariables = dict(self.cursor.fetchall())
		return gvariables
	
	def total_memory(self):
		gvar = self.global_variables()
		mem1 = float(gvar['key_buffer_size']) + float(gvar['query_cache_size']) + float(gvar['tmp_table_size']) + float(gvar['innodb_buffer_pool_size']) + float(gvar['innodb_additional_mem_pool_size']) + float(gvar['innodb_log_buffer_size']) + float(gvar['max_connections']) 
		 
		mem2 = float(gvar['sort_buffer_size']) + float(gvar['read_buffer_size']) + float(gvar['read_rnd_buffer_size']) + float(gvar['join_buffer_size']) + float(gvar['thread_stack']) + float(gvar['binlog_cache_size']) 
		
		tmem = (mem1 + mem2)/1024/1024 
		return tmem   

	def keycache_hitrate(self):
		gstat = self.global_status()
		nozero = float(gstat['Key_read_requests'])
		if (nozero == 0):
		 return 'Key_read_requests is 0' 
		else:
		 kchitr = (1-gstat['Key_reads']/gstat['Key_read_requests'])*100
		 return kchitr
	
	def percent_full_table_scans(self):
		gstat = self.global_status()
		nozero = gstat['Handler_read_rnd_next']
                if (nozero == 0):
                 return 'handler_read_rnd_next is 0'
                else:
                 hand1 = gstat['Handler_read_rnd_next'] + gstat['Handler_read_rnd']
		 hand2 = gstat['Handler_read_rnd_next'] + gstat['Handler_read_rnd'] + gstat['Handler_read_first'] + gstat['Handler_read_next'] + gstat['Handler_read_key'] + gstat['Handler_read_prev']
		 perfull = (1-(float(hand1)/float(hand2)))*100
                 return perfull


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

	def innodb_buffer_pool_hitrate(self):
		gstat = self.global_status()
		bphitr = (1-float(gstat['Innodb_buffer_pool_reads'])/float(gstat['Innodb_buffer_pool_read_requests']))*100 
		return bphitr
	
	

