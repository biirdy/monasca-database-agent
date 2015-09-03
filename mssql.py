import pymssql
import time

conn 	= pymssql.connect("192.168.1.109", "WIN-TV8J9RAD7T4\Administrator", "admin_1", "SDCTUEst_VLC")
cursor 	= conn.cursor(as_dict=True)

i = 0

while True:
	cursor.execute('SELECT * FROM fn_virtualfilestats(NULL, NULL) WHERE DbId = 7 AND FileId = 1')

	if i == 0:
		prev = cursor.fetchone()
		i = 1
		continue

	row 		= cursor.fetchone()

	times 		= row['TimeStamp'] - prev['TimeStamp']
	num_reads 	= row['NumberReads'] - prev['NumberReads']
	bytes_read	= row['BytesRead'] - prev['BytesRead']
	io_stall_read 	= row['IoStallReadMS'] - prev['IoStallReadMS']
	num_writes	= row['NumberWrites'] - prev['NumberWrites']
	bytes_written	= row['BytesWritten'] - prev['BytesWritten']
	io_stall_write	= row['IoStallWriteMS'] - prev['IoStallWriteMS']
	io_stall	= row['IoStallMS'] - prev['IoStallMS']
	bytes_on_disk	= row['BytesOnDisk'] - prev['BytesOnDisk']

	print times, num_reads, bytes_read, io_stall_read, num_writes, bytes_written, io_stall_write, io_stall, bytes_on_disk

	prev = row

	time.sleep(1)

conn.close()

print 'Closed'
