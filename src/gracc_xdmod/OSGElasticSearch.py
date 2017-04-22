'''
Builds an ES query based on the given config file section

@author: rynge
'''

import logging
import time
from datetime import datetime
from calendar import timegm
import sys
import re
import math

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from pprint import pprint, pformat

import urllib3
urllib3.disable_warnings()

log = logging.getLogger("gracc_xdmod")


class OSGElasticSearch(object):
    '''
    Builds an ES query based on the given config file section
    '''

    def __init__(self, conf):
        '''
        
        :param conf:
        :param state:
        '''
        
        self.conf = conf


        
    def query(self, start_time):
        '''
        Queries ES for the data configured in the conf_section of the config file
        
        :param conf:
        :param conf_section:
        '''
        
        # only do this months index - small problem when crossing month
        # boundaries, but the data ignored is minor
        # example: gracc.osg.raw-2016.11
        now = datetime.utcnow()
        index = 'gracc.osg.raw-' + now.strftime("%Y.%m")
        
        log.info("Querying for data after " + start_time)

        wildcardProbeNameq = 'condor:xd-login.opensciencegrid.org'
            
        wildcardProjectName = 'TG-*'
            
        # Elasticsearch query and aggregations
        s = Search(using=self._establish_client(), index=index) \
                .query("wildcard", ProbeName=wildcardProbeNameq) \
                .filter("wildcard", ProjectName=wildcardProjectName) \
                .filter("range", ** {'@received': {'gt': start_time}}) \
                .filter("term", ResourceType="Payload")
    
                #.filter("range", @received={"gt": start_time}) \

        # is this needed with the buckets below?
        s.sort('CreateTime')

        # only do a subset for now
        s = s[0:10000]

        log.debug(pformat(s.to_dict()))

        # FIXME: improve error handling
        try:
            response = s.execute()
            if not response.success():
                raise
        except Exception as e:
            print(e, "Error accessing Elasticsearch")
            sys.exit(1)

        log.debug(pformat(response.to_dict()))
        
        # build our return data structure - it is a dict with some metadata, and then the actual data
        info = {}
        info["max_date_value"] = 0
        info["max_date_str"] = ""
        info["data"] = []

        for hit in response:
            data = {}
            data["@received"] = hit["@received"]
            data["job_name"] = hit["JobName"]
            data["project_name"] = hit["ProjectName"]
            data["user"] = hit["LocalUserId"]
            data["node_count"] = int(1)
            data["processors"] = int(hit["Processors"])
            data["start_time_str"] = self._clean_date(hit["StartTime"])
            data["end_time_str"] = self._clean_date(hit["EndTime"])
            data['wall_duration'] = int(math.floor(hit["WallDuration"]))
            data['charge'] = data['wall_duration'] / 60.0 / 60.0
            
            info["data"].append(data)
        
        return info
    
        
    def _establish_client(self):
        '''
        Initialize and return the elasticsearch client
        '''
        client = Elasticsearch([self.conf.get("es", "url")],
                               use_ssl=self.conf.getboolean("es", "use_ssl"),
                               verify_certs=False,
                               timeout=self.conf.getint("es", "timeout"))
        return client
        
    
    def _clean_date(self, date):
        d = re.sub("T", " ", date)
        d = re.sub("(\.[0-9]+)*Z", "", d)
        return d

