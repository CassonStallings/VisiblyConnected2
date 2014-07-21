"""
Name:       CrunchbaseApi.py
Purpose:    Class with tools to pull data from crunchbase through the V1.0 API
            and store it in MongoDB as JSON documents. MongoDB may be running
            on AWS. The class is set up to get all data, not data related to specific
            queries. This is primarily used by crunchbase_to_mongo.py. It can be used
            separately, but the mongoDB is integrated a little too well.

            Connection pooling is used to allow multiple simultaneous requests. The
            frequency of requests is limited to 10 per second per the Crunchbase API.

Requires:   If AWS is used the AMAZON_ACCESS_KEY_ID and AMAZON_SECRET_ACCESS_KEY are needed.
            Crunchbase API is required and should be be stored in an environmental variable,
            CRUNCHBASE_API_KEY, if not passed in.
Author:     Casson Stallings, CassonStallings@gmail.com
Created:    3/12/2014
Copyright:  Casson Stallings (c) 2014
Licence:    Apache License, Version 2.0
"""


import os, json, cPickle
import time
from time import sleep
from simplejson.decoder import JSONDecodeError
from concurrent import futures
import requests
from boto.s3.key import Key
from boto.s3.connection import S3Connection

class CrunchbaseApi():
    """
    Implements some of the Crunchbase 1.0 API. Designed to store results in MongoDB.

    Types, entity type, or list type are used throughout. They are the 5 types of data
    from Crunchbase that this class can retrieve. The types are:

    financial-organizations
    people
    companies
    products
    service-providers
    """
    entity_types = ['financial-organizations', 'people', 'companies', 'products', 'service-providers']
    single_entity_types = ['financial-organization', 'person', 'company', 'product', 'service-provider']
    singular_entities = {'financial-organizations': 'financial-organization', 'people': 'person',
                           'companies': 'company', 'products': 'product', 'service-providers': 'service-provider'}

    def __init__(self, mongo_uri='', api_key=None, aws_id=None, aws_key=None, open_log_file=None):
        """Initialize a CrunchbaseApi object using the crunchbase API key.

        :param str mongo_uri: URI to mongoDB instance being used, looks to environment var if not supplied
        :param str api_key: Crunchbase API key
        :param str aws_id: AMAZON_ACCESS_KEY_ID if AWS is used
        :param str aws_key: AMAZON_SECRET_ACCESS_KEY is AWS is used
        :param file open_log_file: file object for output (not the name) needs to be open.
        :rtype: CrunchbaseApi
        """

        self.base = 'http://legacy-api.crunchbase.com/v/1/'
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = os.environ['CRUNCHBASE_API_KEY']
        if aws_id:
            self.aws_id = aws_id
        if aws_key:
            self.aws_key = aws_key
        if open_log_file:
            self.open_log_file = open_log_file
        if mongo_uri:
            self.mongo_uri = mongo_uri
        else:
            self.mongo_uri ='mongodb://localhost:27017'
        self.sleep_time = 0.0
        self.sleep_time_if_problems = 5.0

    def get_entity_types(self):
        """
        :returns: list of 5 entity types
        :rtype : list
        """
        return self.entity_types

    def get_single_entity_type(self, entity_type):
        """
        :returns: singular entity types
        :rtype : str
        """
        return self.singular_entities[entity_type]

    def put_entity_list_in_s3(self, key_name='', bucket_name='crunchbase_data',  entity_type='financial-organizations'):
        """Call get_entity_list, store results as JSON in S3 bucket, and return list dicts of permalinks

        The JSON object stored is equivalent to a list of dicts, with each dict using an individual
        permalink as its key, and the contents providing minimal information, i.e., the name.

        :param str bucket_name: name of S3 bucket in which to entity list
        :param str key_name: the key, or file, name within the bucket
        :param str entity_type: an entity type.
        :return: a list of dicts, each representing one permalink
        :rtype list[dict]:
        """

        if not bucket_name:
            bucket_name = 'crunchbase_data'
        if not key_name:
            key_name = entity_type + '_list.json'
        conn = S3Connection(self.aws_id, self.aws_key)
        bucket = conn.create_bucket(bucket_name)
        k = bucket.new_key(key_name)

        print 'calling get with entity_type', entity_type
        entity_list = self.get_entity_list(entity_type)
        a_list = list()
        for d in entity_list:
            a_list.append(d['permalink'])
        k.set_contents_from_string(json.dumps(a_list))
        return entity_list

    def get_entity_list_from_s3(self, key_name='', bucket_name='crunchbase_data',  entity_type='financial-organizations'):
        """Get permalinks of one type from s3 and return as a list of dicts.

        :param str entity_type: an entity type
        :param str bucket_name: AWS bucket name
        :param str key_name: key or file name within bucket
        :return: list of dicts
        :rtype list[dict]:
        """

        if not bucket_name:
            bucket_name = 'crunchbase_data'
        if not key_name:
            key_name = entity_type + '_list.json'
        conn = S3Connection(self.aws_id, self.aws_key)
        bucket = conn.get_bucket(bucket_name)
        key = bucket.get_key(key_name)
        if not key:
            print 'The key {} was not found in bucket {}.'.format(key_name, bucket_name)
            return None
        permalink_list = json.loads(key.get_contents_as_string())
        return permalink_list

    def cycle_through_permalinks(self, entity_type, permalink_list, mongo_collection):
        """Currently unused
        Cycles through list of permalinks, calls call_api which stores results in MongoDB.

        :param str entity_type: an entity type
        :param list[str] permalink_list: a list of permalinks
        :param mongo_collection:
        :rtype None:
        """

        t0 = i = 0
        for i in xrange(0, len(permalink_list), 10):
            duration = time.time() - t0
            if duration < 1:
                sleep(1.1 - duration)
                print 'sleeping for', 1.1 - duration
            t0 = time.time()
            out_str = 'Starting Iter, time for last 10: ' + entity_type + ' ' + str(i) + ' ' + str(duration) + ' Seconds'
            print out_str
            if self.open_log_file:
                self.open_log_file.writelines(['\n',out_str])
                self.open_log_file.flush()
            links = permalink_list[i:i+10]
            tl0 = time.time()
            for link in links:
                link_dur = time.time() - tl0
                if link_dur < 1:
                    sleep(1-link_dur)
                #print 'cycle through permalinks, link', link, link_dur
                link_entity_tuples = (entity_type, mongo_collection, link)
                self.api_call(link_entity_tuples)

        def api_call(self, link_type_tuple):
            """currently unused
            Makes individual calls to Crunchbase requesting a specific record, saves in MongoDB.

            :param 3-tuple link_type_tuple: entity_type, mongo_collection, permalink
            :return None:
            """

            entity_type, mongo_collection, permalink = link_type_tuple
            a = 'http://api.crunchbase.com/v/1/' + entity_type + '/' + permalink + '.js?api_key=' + self.api_key
            try:
                temp = requests.get(a)
                if temp.status_code != 200:
                    sleep(3)
                    temp = requests.get(a)
                d = temp.json()
                d['_id'] = permalink
                mongo_collection.save(d)

            except JSONDecodeError as de:
                print 'Decode Error on ', temp.status_code
                print '   URL: ', a
                print '   Error args:', de.args
                if self.open_log_file:
                    self.open_log_file.writelines(['\ndecode error: ' + permalink])
                sleep(3)

            except ValueError as ev:
                print 'Expected Value Error, Get Status_code', temp.status_code, 'Retry after sleep'
                print '   Error args:', ev.args
                sleep(3)
                temp = requests.get(a)
                d = temp.json()
                d['_id'] = permalink
                mongo_collection.save(d)
                if temp.status_code == 200:
                    print 'Success for ', a, '\n'

            except BaseException as exp:
                print 'Error in api_call, Text:', exp, link_type_tuple

    def get_page_store_in_mongo(self, type, collection, permalink, timeout):
        """Makes a single call to Crunchbase, stores results in MongoDB collection.

        :param str type: an entity type
        :param MongoDB Collection collection: the collection for storage
        :param str permalink: specific permalink to be requested
        :param int timeout: connection timeout (second)
        :return:
        """
        a = 'http://api.crunchbase.com/v/1/' + type + '/' + permalink + '.js?api_key=' + self.api_key
        temp = requests.get(a, timeout=timeout)
        d = temp.json()
        d['_id'] = permalink
        collection.save(d)
        return d

    def get_page_store_in_dict(self, type, permalink, the_dict, timeout):
        """Makes a single call to Crunchbase, stores results in a file.

        :param str type: an entity type
        :param MongoDB Collection collection: the collection for storage
        :param str permalink: specific permalink to be requested
        :param int timeout: connection timeout (second)
        :return:
        """
        url = self.base + type + '/' + permalink + '.js?api_key=' + self.api_key
        result = requests.get(url, timeout=timeout)
        d = result.json()
        #obj = json.loads(d)
        the_dict[permalink] = d

    def cycle(self, entity_type, permalink_list, file_name='', mongo_collection=''):
        """Cycle through permalink list, making 10 simultaneous calls to Crunchbase via get_page_store_in_mongo.

        :param str type: an entity type
        :param list[str] permalink_list: list of permalinks
        :param MongoDB Collection mongo_collection: collection where result is stored
        return None:
        """

        dict_of_data = dict()
        # if file_name:
        #     open_file = open(file_name, 'a')

        t0 = i = 0
        for i in xrange(0, len(permalink_list), 10):
            duration = time.time() - t0
            if duration < 1:
                sleep(1.1 - duration)
                print 'sleeping for', 1.1 - duration
            t0 = time.time()
            out_str = 'Starting Iter, time for last 10: ' + entity_type + ' ' + str(i) + ' ' + str(duration) + ' Seconds'
            print out_str
            permalinks = permalink_list[i:i+10]
            tl0 = time.time()

            with futures.ThreadPoolExecutor(max_workers=10) as executor:
                if mongo_collection:
                    make_api_call = dict((executor.submit(self.get_page_store_in_mongo, type=entity_type,
                        collection=mongo_collection, permalink=permalink,
                        timeout=30), permalink)
                        for permalink in permalinks)

                    for i, future in enumerate(futures.as_completed(make_api_call)):
                        json_dict = make_api_call[future]
                        if future.exception() is not None:
                            print('%r generated an exception: %s' % ("ddd", future.exception()))


                else:
                    make_api_call = dict(
                        (executor.submit(
                            self.get_page_store_in_dict, type=entity_type, permalink=permalink,
                            the_dict=dict_of_data, timeout=30),
                            permalink)
                        for permalink in permalinks)

                    for i, future in enumerate(futures.as_completed(make_api_call)):
                        ret = make_api_call[future]
                        #dict_of_data[adict['permalink']] = adict
                        if future.exception() is not None:
                            print('%r Futures generated an exception: %s' % (i, future.exception()))
        return dict_of_data

    def store_webpages(self, key_dict_lst, entity_type, save_bucket):
        """Currently unused
        Given list of permalinks (in dicts), download web pages from crunchbase, and save in s3."""

        # Set up bucket for save
        print 'store_webpages'
        conn = S3Connection(self.aws_id, self.aws_key)
        bucket = conn.get_bucket(save_bucket)
        #k = Key(bucket)

        cnt = 0
        print 'Starting loop to get and store pages:', len(key_dict_lst.keys())
        for cnt, d in enumerate(key_dict_lst):
            print 'looping in enumerate, cnt, d\n      ', cnt, d, 'key=', d['permakey']
            permakey = d['permakey']
            webpage_as_json = self.get_entity(permakey, entity_type=entity_type)
            k = permakey
            cnt += 1
            k.set_contents_from_string(webpage_as_json)
            if cnt > 5:
                break
            print 'store_webpage', cnt
        return cnt

    def get_entity(self, permakey, entity_type='company'):
        """Currently unused
         get type from crunchbase and returns a dictionary."""
        params = {'api_key': self.api_key}  ###, 'Entity': type, 'Name': permalink}
        url = self.base + entity_type + '/' + permakey + '.js'
        time.sleep(self.sleep_time)  # Wait for a bit so we are not hitting it too fast..
        try:
            r = requests.get(url, params=params)
            if r.status_code >= 400:
                print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
                return None
        except:
            print 'Waiting 5 seconds before continuing.', url, params
            time.sleep(self.sleep_time_if_problems)
            return None
        try:
            result = json.loads(r.content)
        except:
            print '\nERROR in get_entity loading json\n', r.content
            result = None

        return result

    def get_entity_list(self, entity_type):
        """Gets complete list of entity type from crunchbase and returns a list of dictionaries.

        :param str entity_type: an entity type
        :return: JSON object representing  permalink list
        :rtype list[dict]:
        """
        url = self.base + entity_type + '.js'
        print 'url', url
        params = {'api_key': self.api_key}
        r = requests.get(url, params=params)

        if r.status_code >= 400:
            print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
            print '   URL attempted:', r.url
            return None
        # r.content: list of dict-like
        return json.loads(r.content)

    def get_entity_list_and_pickle(self, entity_type, pickle_file):
        """
        Gets complete list of an entity type from crunchbase, saves a pickle,
        and returns it as a list.

        :param str entity_type: an entity type (e.g. people, companies, financial-organizations)
        :param str pickle_file: file to save pickled list
        :rtype list[str]: permalinks
        """
        # url = self.base + entity_type + '.js'
        # params = {'api_key': self.api_key}
        # r = requests.get(url, params=params)
        # if r.status_code >= 400:
        #     print '\nERROR {} while retrieving page {}'.format(r.status_code, url)
        #     print '   URL attempted:', r.url
        #     return

        d = self.get_entity_list(entity_type)
        entity_list = list()
        for adict in d:
            entity_list.append(adict['permalink'])
        cPickle.dump(entity_list, open(pickle_file, 'wb'))
        return entity_list

    def get_pickled_entity_list(self, pickle_file):
        """
        Gets a complete entity list from a pickle file and returns as list.

        :param str pickle_file: file where pickle is stored
        :rtype list[str]: permalinks
        """
        return cPickle.load(open(pickle_file, 'rb'))
