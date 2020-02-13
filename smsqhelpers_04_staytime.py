# --- HOWTO

# smsqhelpers_04_staytime.py is a helpers script to create aggregated spatial indicators on stay time of your data

# usage:
# smsqhelpers_04_frequency.py run

# --- used modules

import sys

import psycopg2
from psycopg2.extras import RealDictCursor, wait_select

from datetime import datetime  # stat

# --- help

if len(sys.argv) < 2 or sys.argv[1] in (None ,'help', '-help', '-h', '-H'):
	print('# --- HOWTO')
	print('\n# smsqhelpers_04_staytime.py is a helpers script to create aggregated spatial indicators on stay time of your data')
	print('\n# usage:')
	print('# smsqhelpers_04_frequency.py <iteration>')
	print('\nE. g.:')
	print('# smsqhelpers_04_frequency.py leer')
	print('Or:')
	print('# smsqhelpers_04_frequency.py iter1')

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

	# table = ['bh', 'do', 'kv', 'kd']  # the four camera abbrevs
	table = [{'short': 'bh', 'long': 'backhus'},
		{'short': 'do', 'long': 'designOffices'},
		{'short': 'kv', 'long': 'kirchvorplatz'},
		{'short': 'kd', 'long': 'kreuzungDomplatz'}]  # the four camera abbrevs

	for i in range(0, len(table)):
		# pre
		t = table[i]['short'] + '_' + iter
		
		if table[i]['short'] == 'bh':
			side = 1
		else:
			side = 2
		
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
		print('processing camera #' + str(i+1) + ' of ' + str(len(table)) + ': ' + table[i]['long'])
		
		select = ''

		for j in range(0, len(result)):
			for k in range(0, result[j]['count']):
				alias = 't' + str(j+1) + '_' + str(k+1)
				
				count = alias + '_count, '
				mean = alias + '_mean, '
				median = alias + '_median, '
				
				select += count + mean + median
		
		select = select[:-2]  # erase last comma
		select += ' '  # add space
		
		leftjoin = ''
		
		for j in range(0, len(result)):
			for k in range(0, result[j]['count']):
				alias = 't' + str(j+1) + '_' + str(k+1)
				leftjoin += 'LEFT JOIN (SELECT hx.gid, COUNT(*) AS ' + alias + '_count, '
				leftjoin += 'AVG(m_s) AS ' + alias + '_mean, '
				leftjoin += 'PERCENTILE_CONT(.5) WITHIN GROUP (ORDER BY m_s) AS ' + alias + '_median '
				leftjoin += 'FROM trx_hexagon AS hx, segments '
				leftjoin += 'WHERE segments.geom IS NOT NULL AND '
				leftjoin += 'ST_INTERSECTS(hx.geom, segments.geom) AND '
				
				if k == 0:
					leftjoin += "timestamp LIKE '" + result[j]['times'] + "_08%' "
				if k == 1:
					leftjoin += "timestamp LIKE '" + result[j]['times'] + "_11%' "
				if k == 2:
					leftjoin += "timestamp LIKE '" + result[j]['times'] + "_13%' "
				if k == 3:
					leftjoin += "timestamp LIKE '" + result[j]['times'] + "_15%' "
				
				leftjoin += 'GROUP BY hx.gid, camname, timestamp '
				leftjoin += 'ORDER BY hx.gid) AS ' + 'sq_' + alias[1:] + ' '
				leftjoin += 'ON hex.gid = ' + 'sq_' + alias[1:] + '.gid '
		
		# temporary table
		with_hex_spec = 'WITH trx_hexagon_' + table[i]['long'] + ' AS ('
		with_hex_spec += 'SELECT hexhex.gid, hexhex.geom '
		with_hex_spec += 'FROM trx_hexagon AS hexhex, trx_' + iter + ' AS tx '
		with_hex_spec += 'WHERE sideLen = ' + str(side) + ' AND '
		with_hex_spec += "camname = '" + table[i]['long'] + "' AND "
		with_hex_spec += 'objtype = 1 AND '
		with_hex_spec += 'ST_INTERSECTS(hexhex.geom, tx.geom) '
		with_hex_spec += 'GROUP BY hexhex.gid, hexhex.geom), '
		
		# temporary table
		# with_seg = 'WITH segments AS (' 
		with_seg = 'segments AS ('
		with_seg += 'SELECT gid, camname, timestamp, (pt).path[2] -1 AS path, '
		with_seg += 'ST_MAKELINE(LAG((pt).geom, 1, NULL) OVER ('
		with_seg += 'PARTITION BY gid ORDER BY gid, (pt).path), (pt).geom) AS geom, '
		with_seg += 'ST_LENGTH(ST_MAKELINE(LAG((pt).geom, 1, NULL) OVER ('
		with_seg += 'PARTITION BY gid ORDER BY gid, (pt).path), (pt).geom)) AS m_s '
		with_seg += 'FROM (SELECT gid, camname, timestamp, ST_DUMPPOINTS(geom) AS pt '
		with_seg += 'FROM trx_' + iter + " WHERE camname = '" + table[i]['long'] + "' "
		# with_seg += "AND timestamp LIKE '" + result[j]['times'] + "%'"
		with_seg += 'AND objtype = 1) AS dumps) '
		
		# regular query (with temporary table)
		query = ''
		
		query += 'DROP TABLE IF EXISTS trxhex_stay_' + t + ';'
		
		query += '\nCREATE TABLE trxhex_stay_' + t + ' AS '
		query += with_hex_spec + with_seg
		query += 'SELECT hex.gid, ' + select + ' '
		query += 'FROM trx_hexagon_' + table[i]['long'] + ' AS hex ' + leftjoin
		query = query[:-1] + ';'

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