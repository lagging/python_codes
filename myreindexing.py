#!/usr/bin/env python

from elasticsearch import Elasticsearch
from argparse import ArgumentParser, FileType
import json
import requests
from time import sleep

class ElasticSearch():
    def __init__(self, url):
        self.url = url

    def request(self, method, path, data=None):
        return (requests.request(
                method, 'http://%s/%s' % (self.url, path),
                data=data,
                headers={'Content-type': 'application/json'}).json())

    def post(self, path, data):
        return self.request('post', path, data)

    def get(self, path, data=None):
        return self.request('get', path, data)

    def scan_and_scroll(self, index):
        response = self.get('%s/_search?search_type=scan&scroll=1m' % index,
                            data=json.dumps({"query": {"match_all": {}},
                                             "size": 100}))
        while True:
            response = self.get('_search/scroll?scroll=1m',
                                data=response['_scroll_id'])
            if len(response['hits']['hits']) == 0:
                return
            yield response['hits']['hits']

    def set_mapping(self, index, mappings):
        return self.post(index, data=json.dumps(mappings))

    def count(self, index):
        response = self.get('%s/_search' % index)
        return response['hits']['total'] if 'hits' in response else 0

    def bulk_insert(self, index, bulk):
        return self.post('_bulk',
                         data=''.join(
                         json.dumps({'create': {'_index': index,
                                                '_type': line['_type']}}) +
                         "\n" +
                         json.dumps(line['_source']) + "\n" for line in bulk))

    def drop(self, index):
        return self.request('delete', index)

    def alias(self, index, to):
        return self.request('put', '%s/_alias/%s' % (index, to))
  
    def change(self, index, temporary_index, alias):
        return self.request('post', '/_aliases',data=json.dumps(json.loads('{"actions": [{ "remove": { "index": "%s", "alias": "%s" }},{ "add": { "index": "%s", "alias": "%s" }}]}'%(index,alias,temporary_index,alias) )))
	

def reindex(elasticsearch, mapping_file, index,alias):
    es = ElasticSearch(elasticsearch)
    es.alias(index,alias)
    mapping_text = mapping_file.read()
    temporary_index = index + '-tmp-'
    for i in range(100):
        try_temporary_index = temporary_index + str(i)
        print "Setting mapping to %s" % try_temporary_index
        response = es.set_mapping(try_temporary_index,json.loads(mapping_text))
        if 'acknowledged' in response and response['acknowledged']:
            temporary_index = try_temporary_index
            break
    if temporary_index is None:
        print "Can't find a temporary index to work with."
        return False

    old_index_count = es.count(index)

    for bulk in es.scan_and_scroll(index):
        es.bulk_insert(temporary_index, bulk)
    print "\nDone inserting data\n"

    for i in range(100):
        new_index_count = es.count(temporary_index)
        if new_index_count == old_index_count:
            print "OK, same number of rows in both index."
            break
        print ("Not the same number of rows in old and new...waiting... "
               "(old=%d, new=%d)"% (old_index_count, new_index_count))
        if i > 10:
	    print "aborting not equal rows in both index"
            return
        sleep(1)

    #print "Deleting %s" % index
    #es.drop(index)
    #print "Aliasing %s to %s" % (temporary_index, index)
    #es.alias(temporary_index, index)
    es.change(index,temporary_index,alias)



if __name__ == '__main__':
    parser = ArgumentParser(
        description="reindex the given index, but only if you stoped"
        "writing to it (will fail if you're writing)...check your alias too")
    parser.add_argument('--index', help='index name to reindex')
    parser.add_argument('--elasticsearch', help='ES host address')
    parser.add_argument('--mapping',
                        help='Mapping file, starts with {"mappings"...',
                        type=FileType('r'))
    parser.add_argument('--alias', help='alias name')
    args = parser.parse_args()
    reindex(args.elasticsearch, args.mapping, args.index,args.alias)
