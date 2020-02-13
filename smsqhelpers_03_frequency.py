# --- HOWTO

# smsqhelpers_03_frequency.py is a helpers script to create aggregated spatial indicators on frequency of your data

# usage:
# smsqhelpers_03_frequency.py run

# --- used modules

import sys

import psycopg2
from psycopg2.extras import RealDictCursor, wait_select

from datetime import datetime  # stat

# --- help

if len(sys.argv) < 2 or sys.argv[1] in (None ,'help', '-help', '-h', '-H'):
	print('# --- HOWTO')
	print('\n# smsqhelpers_03_frequency.py is a helpers script to create aggregated spatial indicators on frequency of your data')
	print('\n# usage:')
	print('# smsqhelpers_03_frequency.py <iteration>')
	print('\nE. g.:')
	print('# smsqhelpers_03_frequency.py leer')
	print('Or:')
	print('# smsqhelpers_03_frequency.py iter1')

# --- init

else:
	# --- connection to database

	connection = 'host=localhost port=5432 dbname=smsq user=postgres password=postgres'
	# pathname
	path = "C:/CSL/mapz/trackanalysis/"

	# args via console
	iter = sys.argv[1]

	# --- startmessage

	print('\n\n\n%%%%%%%%%%%')
	print('\nSmartSquare-Helpers-Script')
	print('\n%%%%%%%%%%%')
	print("\n\n\nYou're going to [...]\n\n")

# --- main

	# -- > -- get data / get table information

	table = ['bh', 'do', 'kv', 'kd']  # the four camera abbrevs

	for i in range(0, len(table)):

		t = table[i] + '_' + iter
		
		q = 'SELECT DISTINCT(SUBSTRING(timestamp FROM 0 FOR 9)) AS times, COUNT(*) FROM '
		q += '(SELECT DISTINCT(timestamp) FROM ' + t + '_raw ORDER BY timestamp) '
		q += 'AS sub GROUP BY times;'

		# connect to database
		with psycopg2.connect(
			connection,
			cursor_factory=RealDictCursor
		) as dbconn:
			dbconn.autocommit = True  # so we do not need to conn.commit() every time... ;)
			
			# the wait_select callback can handle a Ctrl-C correctly (TODO: Deprecated...)
			# wait_select(dbconn)

			with dbconn.cursor() as cursor:
				cursor.execute(q)
				
				result = cursor.fetchall()
				cursor.close()

		# -- > -- work with the result

		# print process
		print('processing camera #' + str(i+1) + ' of ' + str(len(table)) + ': ' + table[i])

		select = ''

		for j in range(0, len(result)):
			for k in range(0, result[j]['count']):
				alias = 'd' + str(j+1) + 't' + str(k+1)
				select += alias + '.count AS ' + alias + ', '

		select = select[:-2]  # erase last comma
		select += ' '  # add space

		leftjoin = ''

		for j in range(0, len(result)):
			for k in range(0, result[j]['count']):
				leftjoin += 'LEFT JOIN (SELECT gid, count FROM ' + table[i] + '_' + iter +'_raw '
				
				if k == 0:
					leftjoin += "WHERE timestamp LIKE '" + result[j]['times'] + "_08%') "
				if k == 1:
					leftjoin += "WHERE timestamp LIKE '" + result[j]['times'] + "_11%') "
				if k == 2:
					leftjoin += "WHERE timestamp LIKE '" + result[j]['times'] + "_13%') "
				if k == 3:
					leftjoin += "WHERE timestamp LIKE '" + result[j]['times'] + "_15%') "
				
				alias = 'd' + str(j+1) + 't' + str(k+1)
				
				leftjoin += 'AS ' + alias + ' ON hex.gid = ' + alias + '.gid '
		
		# close sql query
		leftjoin = leftjoin[:-1]
		leftjoin += ';'

		# -- > -- create query

		# query = ''
		query = 'CREATE TABLE trxhex_freq_' + table[i] + '_' + iter + ' AS '
		query += 'SELECT DISTINCT(hex.gid), ' + select
		query += 'FROM ' + table[i] + '_' + iter + '_raw AS hex '
		query += leftjoin

		# -- > -- execute query
		
		# connect to database (again)
		with psycopg2.connect(
			connection,
			cursor_factory=RealDictCursor
		) as dbconn:
			dbconn.autocommit = True  # so we do not need to conn.commit() every time... ;)
			
			# the wait_select callback can handle a Ctrl-C correctly (TODO: Deprecated...)
			# wait_select(dbconn)

			with dbconn.cursor() as cursor:
				cursor.execute(query)
				cursor.close()

	print('\n\nDone.\n\n')