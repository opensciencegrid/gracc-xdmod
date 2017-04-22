
"""
Module for interacting with XDMoD
"""

import os
import sys
import pwd
import errno
import logging
from datetime import datetime, timedelta
from dateutil import parser

import MySQLdb

log = logging.getLogger("gracc_xdmod")

xdmod_conn = None
xdmod_curs = None


def _add_if_exists(cp, attribute, info):
    """
    If section.attribute exists in the config file, add its value into a
    dictionary.

    @param cp: ConfigParser object representing our config
    @param attribute: Attribute in section.
    @param info: Dictionary that we add data into.
    """
    try:
        info[attribute] = cp.get("xdmod", attribute)
    except:
        pass


def _connect(cp):

    global xdmod_conn
    global xdmod_curs
    if xdmod_conn is not None:
        return

    info = {}
    _add_if_exists(cp, "user", info)
    _add_if_exists(cp, "passwd", info)
    _add_if_exists(cp, "db", info)
    _add_if_exists(cp, "host", info)
    _add_if_exists(cp, "port", info)
    if 'port' in info:
        info['port'] = int(info['port'])
    try:
        xdmod_conn = MySQLdb.connect(**info)
        log.debug("Successfully connected to database ...")
    except:
        log.error("Failed to connect to database; and the reason is: "+ str(xdmod_conn))
        raise Exception("Failed to connect to database")
    xdmod_curs = xdmod_conn.cursor()


def get_last_dbid(cp):
    _connect(cp)

    query = "SELECT MAX(job_id) FROM raw_jobs_test"
    xdmod_curs.execute(query)
    row = xdmod_curs.fetchone()
    last_dbid = row[0]

    if not isinstance(last_dbid, (int)):
        last_dbid = 0

    return last_dbid


def add(cp, data):
    '''
    Adds a job into the XDMoD database
    '''
    _connect(cp)

    # job_id is no longer availalbe from ES, so just increment the last one
    last_dbid = get_last_dbid(cp)
    data['job_id'] = last_dbid + 1

    log.debug("Inserting new job with id %d" %(data['job_id']))

    # prevent data trunctaion warnings
    data['charge'] = "%.2f" % (data['charge'])
   
    query = """INSERT INTO raw_jobs_test 
( job_id, 
  local_jobid, 
  local_charge, 
  start_time, 
  end_time, 
  wallduration, 
  nodecount, 
  processors, 
  allocation_name, 
  username )
VALUES( %(job_id)s, 
        %(job_name)s, 
        %(charge)s, 
        %(start_time_str)s, 
        %(end_time_str)s, 
        %(wall_duration)s, 
        %(node_count)s, 
        %(processors)s, 
        %(project_name)s, 
        %(user)s )
"""

    xdmod_curs.execute(query, data)
    xdmod_conn.commit()
    
    return True



