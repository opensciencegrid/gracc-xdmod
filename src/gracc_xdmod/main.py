
"""
Library for synchronizing gracc job accounting with XDMoD.

This library will connect to the gracc database, query for
job records, do a little bit of data modificaiton and insert
the record in the XDMoD database.

"""

import os
import time
import random
import logging
import optparse
import configparser

import xdmod
import locking
from State import State
from OSGElasticSearch import OSGElasticSearch

log = None


def parse_opts():

    parser = optparse.OptionParser(conflict_handler="resolve")
    parser.add_option("-c", "--config", dest="config",
                      help="Location of the configuration file.",
                      default="/etc/gracc-xdmod/gracc-xdmod.cfg")
    parser.add_option("-v", "--verbose", dest="verbose",
                      default=False, action="store_true",
                      help="Increase verbosity.")
    parser.add_option("-s", "--cron", dest="cron",
                      type="int", default=0,
                      help = "Called from cron; cron interval (adds a random sleep)")
    
    opts, args = parser.parse_args()

    if not os.path.exists(opts.config):
        raise Exception("Configuration file, %s, does not exist." % \
            opts.config)

    return opts, args


def config_logging(cp, opts):
    global log
    log = logging.getLogger("gracc_xdmod")

    # log to the console
    # no stream is specified, so sys.stderr will be used for logging output
    console_handler = logging.StreamHandler()

    # default log level - make logger/console match
    # Logging messages which are less severe than logging.WARNING will be ignored
    log.setLevel(logging.WARNING)
    console_handler.setLevel(logging.WARNING)

    if opts.verbose: 
        log.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)

    # formatter
    formatter = logging.Formatter("[%(process)d] %(asctime)s %(levelname)7s:  %(message)s")
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)
    log.debug("Logger has been configured")


#def dbid_to_epochms(dbid):
#    """
#    When switching from Gratia to GRACC, we had to switch from matching ids
#    in the two databases to match CreateTime to dbid in xdmod. This switch
#    over happed at epoch 1492783000000 (milliseconds), and at that time,
#    the dbid was 2684684774
#    """
#    return 1492783000000 + (dbid - 2684684774)
#
#
#def epochms_to_dbid(ms):
#    """
#    When switching from Gratia to GRACC, we had to switch from matching ids
#    in the two databases to match CreateTime to dbid in xdmod. This switch
#    over happed at epoch 1492783000000 (milliseconds), and at that time,
#    the dbid was 2684684774
#    """
#    return 2684684774 + (ms - 1492783000000)
#

def main():
    opts, args = parse_opts()
    cp = configparser.ConfigParser()
    cp.read(opts.config)
    config_logging(cp, opts)

    if opts.cron > 0:
        random_sleep = random.randint(1, opts.cron)
        log.info("gracc-xdmod called from cron; sleeping for %d seconds." % \
            random_sleep)
        time.sleep(random_sleep)

    lockfile = cp.get("main", "lockfile")
    locking.exclusive_lock(lockfile)
  
    state = State(cp)
    last_date = state.get_date()

    log.debug("Starting from last date %s" %(last_date))

    #jobs = gratia.query_gracc(cp, last_dbid)
    q = OSGElasticSearch(cp)
    jobs = q.query(last_date)

    for job in jobs["data"]:
        log.debug("Processing job: %s" % str(job))
        
        if not xdmod.add(cp, job):
            log.fatal("Fatal error inserting thew job in the XDMoD database - exiting!")
            return 1
    
        # update the state
        state.update_date(job['@received'])

    return 0

