-- DROP STATEMENTS

DROP TABLE IF EXISTS bh_iter1_raw;
DROP TABLE IF EXISTS do_iter1_raw;
DROP TABLE IF EXISTS kv_iter1_raw;
DROP TABLE IF EXISTS kd_iter1_raw;

-- CREATE STATEMENTS

-- > -- sideLen = 1

CREATE TABLE bh_iter1_raw AS 
SELECT hex.gid, camname, timestamp, COUNT(*) FROM trx_hexagon AS hex, trx_iter1 AS trx WHERE sideLen = 1 AND camname = 'backhus' AND objtype = 1 AND ST_INTERSECTS(hex.geom, trx.geom) GROUP BY hex.gid, camname, timestamp ORDER BY camname, hex.gid;

-- > -- sideLen = 2

CREATE TABLE do_iter1_raw AS 
SELECT hex.gid, camname, timestamp, COUNT(*) FROM trx_hexagon AS hex, trx_iter1 AS trx WHERE sideLen = 2 AND camname = 'designOffices' AND objtype = 1 AND ST_INTERSECTS(hex.geom, trx.geom) GROUP BY hex.gid, camname, timestamp ORDER BY camname, hex.gid;

CREATE TABLE kv_iter1_raw AS 
SELECT hex.gid, camname, timestamp, COUNT(*) FROM trx_hexagon AS hex, trx_iter1 AS trx WHERE sideLen = 2 AND camname = 'kirchvorplatz' AND objtype = 1 AND ST_INTERSECTS(hex.geom, trx.geom) GROUP BY hex.gid, camname, timestamp ORDER BY camname, hex.gid;

CREATE TABLE kd_iter1_raw AS 
SELECT hex.gid, camname, timestamp, COUNT(*) FROM trx_hexagon AS hex, trx_iter1 AS trx WHERE sideLen = 2 AND camname = 'kreuzungDomplatz' AND objtype = 1 AND ST_INTERSECTS(hex.geom, trx.geom) GROUP BY hex.gid, camname, timestamp ORDER BY camname, hex.gid;

