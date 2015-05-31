# coding:utf-8
"""
scrapy-mongodb - MongoDB pipeline for Scrapy

Homepage: https://github.com/sebdah/scrapy-mongodb
Author: Sebastian Dahlgren <sebastian.dahlgren@gmail.com>
License: Apache License 2.0 <http://www.apache.org/licenses/LICENSE-2.0.html>

Copyright 2013 Sebastian Dahlgren

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import datetime

from pymongo import errors
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference

from scrapy import log
from scrapy.contrib.exporter import BaseItemExporter

VERSION = '0.9.0'


def not_set(string):
    """ Check if a string is None or ''

    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline(BaseItemExporter):
    """ MongoDB pipeline class """
    # Default options
    config = {
        'uri': 'mongodb://localhost:27017',
        'fsync': False,
        'write_concern': 0,
        'database': 'scrapy-mongodb',
        'replica_set': None,
        'collection': {
            'default': {
                'name': 'items',
                'unique_key': None,
                'buffer': None,
                'append_timestamp': False,
                'stop_on_duplicate': 0
            }
        }
    }

    # Item buffer
    current_item = {}
    item_buffer = {}

    # Duplicate key occurence count
    duplicate_key_count = {}

    def load_spider(self, spider):
        self.crawler = spider.crawler
        self.settings = spider.settings

    def open_spider(self, spider):
        self.load_spider(spider)

        # Configure the connection
        self.configure()

        if self.config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                self.config['uri'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the collection
        database = connection[self.config['database']]
        self.collection = {}
        self.stop_on_duplicate = {}
        self.buffer_settings = {}
        for itype, collection in self.config['collection'].items():
            self.collection[itype] = database[collection['name']]
            if collection['buffer']:
                self.buffer_settings[itype] = collection['buffer']
            else:
                self.buffer_settings[itype] = 0
            self.item_buffer[itype] = []
            self.current_item[itype] = 0 

            # Ensure unique index
            if collection['unique_key']:
                self.collection[itype].ensure_index(collection['unique_key'], unique=True)
                log.msg('uEnsuring index for key {0}'.format(
                collection['unique_key']))

            # Get the duplicate on key option
            if collection['stop_on_duplicate']:
                tmpValue = collection['stop_on_duplicate']
                if tmpValue < 0:
                    log.msg(
                        (
                            u'Negative values are not allowed for'
                            u' MONGODB_STOP_ON_DUPLICATE option.'
                        ),
                        level=log.ERROR
                    )
                    raise SyntaxError(
                        (
                            'Negative values are not allowed for'
                            ' MONGODB_STOP_ON_DUPLICATE option.'
                        )
                    )
                self.stop_on_duplicate[itype] = collection['stop_on_duplicate']
            else:
                self.stop_on_duplicate[itype] = 0
          
            log.msg(u'Connected to MongoDB {0}, using "{1}/{2}"'.format(
                self.config['uri'],
                self.config['database'],
                collection['name']))

    def configure(self):
        """ Configure the MongoDB connection """
        # Handle deprecated configuration
        if not not_set(self.settings['MONGODB_HOST']):
            log.msg(
                u'DeprecationWarning: MONGODB_HOST is deprecated',
                level=log.WARNING)
            mongodb_host = self.settings['MONGODB_HOST']

            if not not_set(self.settings['MONGODB_PORT']):
                log.msg(
                    u'DeprecationWarning: MONGODB_PORT is deprecated',
                    level=log.WARNING)
                self.config['uri'] = 'mongodb://{0}:{1:i}'.format(
                    mongodb_host,
                    self.settings['MONGODB_PORT'])
            else:
                self.config['uri'] = 'mongodb://{0}:27017'.format(mongodb_host)

        if not not_set(self.settings['MONGODB_REPLICA_SET']):
            if not not_set(self.settings['MONGODB_REPLICA_SET_HOSTS']):
                log.msg(
                    (
                        u'DeprecationWarning: '
                        u'MONGODB_REPLICA_SET_HOSTS is deprecated'
                    ),
                    level=log.WARNING)
                self.config['uri'] = 'mongodb://{0}'.format(
                    self.settings['MONGODB_REPLICA_SET_HOSTS'])

        # Set all regular options
        options = [
            ('uri', 'MONGODB_URI'),
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
        ]

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

    def process_item(self, item, spider):
        """ Process the item and add it to MongoDB

        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        itype = type(item).__name__
        item = dict(self._get_serialized_fields(item))
        if not self.config['collection'][itype]:
            itype = 'default'

        collection = self.config['collection'][itype]

        if self.buffer_settings[itype]:
            self.current_item[itype] += 1

            if collection['append_timestamp']:
                item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}

            self.item_buffer[itype].append(item)

            if self.current_item[itype] == collection['buffer']:
                self.current_item[itype] = 0
                return self.insert_item(self.item_buffer[itype], spider, itype)

            else:
                return item

        return self.insert_item(item, spider, itype)

    def close_spider(self, spider):
        """ Method called when the spider is closed

        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """
        for itype, item_buffer in self.item_buffer.values:
            if item_buffer:
                self.insert_item(item_buffer, spider, itype)

    def insert_item(self, item, spider, itype):
        """ Process the item and add it to MongoDB

        :type item: (Item object) or [(Item object)]
        :param item: The item(s) to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        collection = self.config['collection'][itype]

        if not isinstance(item, list):
            item = dict(item)

            if collecgtion['append_timestamp']:
                item['scrapy-mongodb'] = {'ts': datetime.datetime.utcnow()}

        if collection['unique_key'] is None:
            try:
                self.collection[itype].insert(item, continue_on_error=True)
                log.msg(
                    u'Stored item(s) in MongoDB {0}/{1}'.format(
                        self.config['database'], collection['name']),
                    level=log.DEBUG,
                    spider=spider)
            except errors.DuplicateKeyError:
                log.msg(u'Duplicate key found', level=log.DEBUG)
                if (self.stop_on_duplicate[itype] > 0):
                    self.duplicate_key_count[itype] += 1
                    if (self.duplicate_key_count[itype] >= self.stop_on_duplicate[itype]):
                        self.crawler.engine.close_spider(
                            spider,
                            'Number of duplicate key insertion exceeded'
                        )
                pass

        else:
            key = {}
            if isinstance(collection['unique_key'], list):
                for k in dict(collection['unique_key']).keys():
                    key[k] = item[k]
            else:
                key[collection['unique_key']] = item[collection['unique_key']]

            self.collection[itype].update(key, item, upsert=True)

            log.msg(
                u'Stored item(s) in MongoDB {0}/{1}'.format(
                    self.config['database'], collection['name']),
                level=log.DEBUG,
                spider=spider)

        return item
