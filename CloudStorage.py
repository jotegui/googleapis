__author__ = '@jotegui'

import json
import time
import logging
import urllib2
from urllib import urlencode

from GoogleAPI import GoogleAPI, ConfigError
from apiclient import errors


class CloudStorage(GoogleAPI):
    """Wrapper class for the most common CloudStorage functions."""
    
    def __init__(self, config):
        """Initialize the service."""
        
        try:
            self._BUCKET_NAME = config['bucket_name']
        except KeyError:
            raise ConfigError("Missing configuration element: bucket_name")
        
        # Initialize GoogleAPI instance and create storage service
        GoogleAPI.__init__(self, 'storage')
        self.methods = self.get_methods()
        
        return
    
    
    def get_methods(self):
        """Print the currently available methods."""
        s = ''
        for i in dir(self):
            if not i[0].isupper() and i[0] != '_' and i != 'service':
                s += i
                s += '\t{0}'.format(getattr(self, i).__doc__)
                s += "\n"
        return s
        
        
    def list_bucket(self, prefix=None):
        """Return the content of a bucket. Use the optional prefix to indicate folder."""
        while True:
            try:
                resp = self.service.objects().list(bucket=self._BUCKET_NAME, prefix=prefix).execute()
                return resp
            except errors.HttpError as e:
                if json.loads(e.content)['error']['code'] == 500: # Internal Error. Wait 3 secs and retry
                    time.sleep(3)
                else:
                    raise(e)
    
    
    def get_object(self, path):
        """Get metadata on a particular object."""
        resp = self.service.objects().get(bucket=self._BUCKET_NAME, object=path).execute()
        return resp
    
    
    def delete_object(self, path):
        """Delete a particular object. Warning! Deletions are permanent."""
        resp = self.service.objects().delete(bucket=self._BUCKET_NAME, object=path).execute()
        return resp
    
    
    def prepare_merge(self, folder):
        """Prepare the body of the request for merging a folder into a single file."""
        
        req_bodies = []
        
        req_body = {
            "kind": "storage#composeRequest",
            "sourceObjects":[],
            "destination": {"contentType": "application/octet-stream"}
        }
        
        objs = self.list_bucket(prefix=folder)
        
        cont = 0
        for obj in objs['items']:
            req_body['sourceObjects'].append({"generation":obj["generation"], "name":obj["name"]})
            
            cont += 1
            if cont == 32:
                req_bodies.append(req_body)
                req_body = {
                    "kind": "storage#composeRequest",
                    "sourceObjects":[],
                    "destination": {"contentType": "application/octet-stream"}
                }
                cont = 0
        if cont > 0:
            req_bodies.append(req_body)
        
        return req_bodies
    
    
    def _merge_resource(self, folder):
        """VertNet specific: Perform the merging of a single resource into one or more 32-piece lumps."""
        
        req_bodies = self.prepare_merge(folder)
        
        cont = 0
        for req_body in req_bodies:
            
            if len(req_body['sourceObjects']) > 1: # if there is more than one object, merge them
                destinationObject = "/".join(["resource_dumps", "{0}_{1}".format(folder.split("/")[2], cont)])
                req = self.service.objects().compose(destinationBucket=self._BUCKET_NAME, destinationObject=destinationObject, body=req_body)
                resp = req.execute()
            
            elif len(req_body['sourceObjects']) == 1: # if there is only one object, copy it
                sourceObject=req_body['sourceObjects'][0]['name']
                destinationObject='resource_dumps/{0}'.format(folder.split("/")[2])
                resp = self.copy_object_within_bucket(sourceObject, destinationObject)
            cont += 1
        
        return resp
    
    
    def _get_prefixes(self):
        """VertNet specific: Extract location of last indexed resources."""
        api_url = "https://vertnet.cartodb.com/api/v2/sql"
        q = "select harvestfolder from resource_staging where ipt is true and networks like '%VertNet%'"
        url = '?'.join([api_url, urlencode({'q':q})])
        d = ['/'.join(x['harvestfolder'].split('/')[1:]) for x in json.loads(urllib2.urlopen(url).read())['rows']]
        return d
    
    
    def delete_bucket(self, folder):
        """Delete content of particular bucket."""
        objs = self.list_bucket(prefix=folder)['items']
        for obj in objs:
            self.delete_object(obj['name'])
        return
    
    
    def _clean_dumps(self):
        """VertNet specific: Delete the content of the resource_dumps folder."""
        folder = "resource_dumps"
        self.delete_bucket(folder)
        return
    
    
    def _build_resources(self):
        """VertNet specific: Merge the shards of each resource into one or two lump files in resource_dumps folder."""
        prefixes = self.get_prefixes()
        for prefix in prefixes:
            self.merge_resource(prefix)
        return
    
    def _build_uri_list(self):
        """VertNet specific: Build a list of the lump files' URIs for BigQuery client."""
        uri_list = []
        objs = self.list_bucket("resource_dumps")
        for obj in objs['items']:
            uri = "/".join(["gs:/", self._BUCKET_NAME, obj['name']])
            uri_list.append(uri)
        return uri_list
    
    
    def _build_resource_uri_list(self, prefix):
        """VertNet specific: Build a list containing the URIs of the shards of a single resource."""
        uri_list = []
        for i in self.list_bucket(prefix)['items']:
            uri = '/'.join(["gs:/", self._BUCKET_NAME, i['name']])
            uri_list.append(uri)
        return uri_list
    
    
    def compose(self, req_body, destinationObject):
        """Given a request body and the name of a new file, merge all files in the request body into the new file."""
        req = self.service.objects().compose(destinationBucket=self._BUCKET_NAME, destinationObject=destinationObject, body=req_body)
        resp = req.execute()
        return resp
    
    
    def locate_resource(self, resource_name):
        """Given the name of a resource, return the full path to it."""
        objs = self.list_bucket()['items']
        for obj in objs:
            if resource_name in obj['name']:
                logging.info(obj['name'])
        return
    
    
    def copy_object_within_bucket(self, sourceObject, destinationObject):
        """Make a copy of an object with a different name or path, but inside the same bucket."""
        copy_body = {
            "kind": "storage#object",
            "contentType": "application/octet-stream",
            "contentLanguage": "en"
        }
        req = self.service.objects().copy(
            sourceBucket=self._BUCKET_NAME,
            sourceObject=sourceObject,
            destinationBucket=self._BUCKET_NAME,
            destinationObject=destinationObject,
            body=copy_body
        )
        resp = req.execute()
        return resp
