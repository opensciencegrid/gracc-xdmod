
"""
Module for interacting with Gratia.

Connects to the database,
Queries the database,
Summarizes resulting queries.
"""

import logging
import math

import transaction

import MySQLdb

GRATIA_QUERY = \
"""
SELECT
  JUR.dbid as dbid,
  ResourceType,
  ReportableVOName,
  JobName,
  LocalUserId,
  ProjectName,
  NodeCount,
  Processors,
  StartTime,
  EndTime,
  WallDuration 
FROM
  JobUsageRecord JUR
JOIN
  JobUsageRecord_Meta JURM ON JUR.dbid = JURM.dbid 
WHERE
  JUR.dbid > %(last_successful_id)s AND
  ProbeName REGEXP %(probename)s AND
  ProjectName REGEXP '^TG-'  
ORDER BY JUR.dbid ASC
LIMIT 1000
""" 

log = logging.getLogger("gratia_xdmod")


def _add_if_exists(cp, attribute, info):
    """
    If section.attribute exists in the config file, add its value into a
    dictionary.

    @param cp: ConfigParser object representing our config
    @param attribute: Attribute in section.
    @param info: Dictionary that we add data into.
    """
    try:
        info[attribute] = cp.get("gratia", attribute)
    except:
        pass

def query_gratia(cp, last_dbid):
    conn = None
    info = {}
    _add_if_exists(cp, "user", info)
    _add_if_exists(cp, "passwd", info)
    _add_if_exists(cp, "db", info)
    _add_if_exists(cp, "host", info)
    _add_if_exists(cp, "port", info)
    if 'port' in info:
        info['port'] = int(info['port'])
    try:
        conn = MySQLdb.connect(**info)
        log.debug("Successfully connected to database ...")
    except:
        log.error("Failed to connect to database; and the reason is:"+ str(conn))
        raise Exception("Failed to connect to database")
    curs = conn.cursor()

    data = {}
    data['probename'] = cp.get("gratia", "probe")
    data['last_successful_id'] = last_dbid

    results = []
    curs.execute(GRATIA_QUERY, data)
    for row in curs.fetchall():
        info = {}
        info['dbid'] = row[0]
        info['resource_type'] = row[1]
        info['vo_name'] = row[2]
        info['job_id'] = row[3]
        info['user'] = row[4]
        info['project_name'] = row[5]
        info['node_count'] = row[6]
        info['processors'] = row[7]
        info['start_time'] = row[8]
        info['end_time'] = row[9]
        info['wall_duration'] = row[10]

        # cleaning of the data
        if info['processors'] is None:
            info['processors'] = 1
        info['start_time_str'] = info['start_time'].strftime("%Y-%m-%d %H:%M:%S") 
        info['end_time_str'] = info['end_time'].strftime("%Y-%m-%d %H:%M:%S") 
        info['wall_duration'] = int(math.floor(info['wall_duration']))
        info['charge'] = info['wall_duration'] / 60.0 / 60.0

        results.append(info)

    curs.close()
    conn.close()

    return results

