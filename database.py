import logging
import pymssql

# project
import monasca_agent.collector.checks as checks

class DatabseStatsMSSQL(checks.AgentCheck):
    
    def __init__(self, name, init_config, agent_config):
        super(DatabseStatsMSSQL, self).__init__(name, init_config, agent_config)
        self.prev_stats = {}

    def check(self, instance):

    	host = instance.get('host', '')
    	user = instance.get('user', '')
    	password = instance.get('password', '')
    	database = instance.get('database', '')

    	conn 	= pymssql.connect(host, user, password, database)
    	cursor	= conn.cursor(as_dict=True)

    	#unique name
    	name = host + database

    	# det dbid from name - should check that database actually exists
    	cursor.execute('SELECT dbid FROM sys.sysaltfiles WHERE name=%s', database)		
    	dbid 	= int(cursor.fetchone()['dbid'])		# result should only ever be a single row

    	# get file stats
    	# fn_virtualfilestats(<database_id>, <file_id>)
    	# file_id = 1 for data file, 2 for log file
    	cursor.execute('SELECT * FROM fn_virtualfilestats(%d, 1)', dbid)

    	row = cursor.fetchone()

    	if name not in self.prev_stats:
    		self.prev_stats[name] = row
    		return

   	#set dimensions
	dimensions = self._set_dimensions({"host": host, "database": database}, instance)

	#post differences
	self.gauge('database.num_reads', row['NumberReads'] - self.prev_stats[name]['NumberReads'],  dimensions)
	self.gauge('database.bytes_read', row['BytesRead'] - self.prev_stats[name]['BytesRead'],  dimensions)
	self.gauge('database.io_stall_read', row['IoStallReadMS'] - self.prev_stats[name]['IoStallReadMS'],  dimensions)
	self.gauge('database.num_writes', row['NumberWrites'] - self.prev_stats[name]['NumberWrites'],  dimensions)
	self.gauge('database.bytes_written', row['BytesWritten'] - self.prev_stats[name]['BytesWritten'],  dimensions)
	self.gauge('database.io_stall_writes', row['IoStallWriteMS'] - self.prev_stats[name]['IoStallWriteMS'],  dimensions)
	self.gauge('database.io_stall', row['IoStallMS'] - self.prev_stats[name]['IoStallMS'],  dimensions)
	self.gauge('database.bytes_on_disk', row['BytesOnDisk'] - self.prev_stats[name]['BytesOnDisk'],  dimensions)    	

    	self.prev_stats[name] = row

    	conn.close()

