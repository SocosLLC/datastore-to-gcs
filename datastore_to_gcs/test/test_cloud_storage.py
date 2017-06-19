# -*- coding: utf-8 -*-
#
# test_cloud_storage.py
#
# Copyright 2016 Socos LLC
#

import datetime
import itertools
import os
import random
import unittest
from pprint import pprint

import dateutil.parser

import gae_stubs
gae_stubs.init()

import cloudstorage

import datastore_to_gcs.cloud_storage as gcs
from datastore_to_gcs import util


QUICK = os.getenv('QUICK') == '1'


class FixtureModel(util.CommonEqualityMixin):

    def __init__(self, id, email, foofloat, dt):
        self.id = id
        self.email = email
        self.foofloat = foofloat
        self.dt = dt


class TestGCSClient(unittest.TestCase):

    bucket = 'test-bucket'
    debug = False
    dict_fixtures = []
    model_fixtures = []

    def setUp(self):
        for i in xrange(5):
            self.dict_fixtures.append({u'id': unicode(i),
                                       u'email': u'%d@example.com' % i,
                                       u'foofloat': random.random(),
                                       u'nested': {u'inner': i},
                                       u'dingo': 1000 + i % 50})
            self.model_fixtures.append(FixtureModel(id=unicode(i),
                                                    email='%d@example.com' % i,
                                                    foofloat=random.random(),
                                                    dt=datetime.date(year=2015, month=1, day=1)))

    def tearDown(self):
        for item in gcs.list_objects(self.bucket):
            cloudstorage.delete(util.parse_cloud_storage_path(self.bucket, item))

    def upload_json(self):
        filename = 'test-2015-08-14T18:46:04.json'
        directory = 'sms_logs/'
        path_base = os.path.join(self.bucket, directory)
        print path_base
        bucket, object_path = gcs.upload_data(self.dict_fixtures, path_base, filename)
        return object_path

    def upload_model_json(self):
        filename = 'test-2015-09-14T20:46:05.json'
        directory = 'sms_logs/'
        path_base = os.path.join(self.bucket, directory)
        print path_base
        bucket, object_path = gcs.upload_data(self.model_fixtures, path_base, filename)
        return object_path

    def test_list_empty_bucket(self):
        self.assertEqual(len(gcs.list_objects(self.bucket)), 0)

    def test_list_dir(self):
        # Add two files in dir, one not in dir
        test_dir = 'td/'
        gcs.upload_data(self.dict_fixtures[0:10], self.bucket, test_dir + 'foo.json')
        gcs.upload_data(self.dict_fixtures[10:20], self.bucket, test_dir + 'bar.json')
        gcs.upload_data(self.dict_fixtures[10:20], self.bucket, 'baz.json')
        # Check that the two files are listed
        listed = gcs.list_objects(self.bucket, test_dir)
        self.assertEqual(len(listed), 2)
        self.assertIn('foo.json', listed)
        self.assertIn('bar.json', listed)

    @unittest.skipIf(QUICK, 'QUICK is set')
    def test_list_over_1000(self):
        test_dir = 'td/'
        for i in range(1010):
            gcs.upload_data(self.dict_fixtures[0:10], self.bucket, test_dir + str(i) + '.json')
        listed = gcs.list_objects(self.bucket, test_dir)
        self.assertEqual(len(listed), 1010)

    def test_upload(self):
        json_name = self.upload_json()
        objects = gcs.list_objects(self.bucket)
        self.assertIn(json_name, objects)

    def test_model_upload(self):
        json_name = self.upload_model_json()
        objects = gcs.list_objects(self.bucket)
        self.assertIn(json_name, objects)

    def test_download_object(self):
        filename = self.upload_json()
        downloaded_object = gcs.download_object(self.bucket, filename)
        # Check that it's a DataDict
        print "DOWNLOADED"
        pprint(downloaded_object)
        self.assertIsInstance(downloaded_object[0], util.DataDict)
        print "\nFIXTURE"
        data_dict_fixture = [util.DataDict(fixture) for fixture in self.dict_fixtures]
        pprint(data_dict_fixture)
        self.assertTrue(downloaded_object == data_dict_fixture)

    def test_download_object_model(self):
        filename = self.upload_model_json()
        downloaded_object = gcs.download_object(self.bucket, filename, object_class=FixtureModel)
        # Check that it's a FixtureModel
        self.assertIsInstance(downloaded_object[0], FixtureModel)
        # Fudge the dates
        for d in downloaded_object:
            d.dt = dateutil.parser.parse(d.dt).date()
        print "DOWNLOADED"
        pprint([d.__dict__ for d in downloaded_object])
        print "\nFIXTURE"
        pprint([f.__dict__ for f in self.model_fixtures])
        self.assertTrue(all([d.__dict__ == f.__dict__ for (d, f) in
                             itertools.izip(downloaded_object, self.model_fixtures)]))

    def test_download_object_filter_fields(self):
        filename = self.upload_json()
        fields = ['id', 'nested.inner']
        downloaded_object = gcs.download_object(self.bucket, filename, fields)
        self.assertEqual(len(downloaded_object), len(self.dict_fixtures))
        self.assertIsInstance(downloaded_object[0]['id'], unicode)
        self.assertIsInstance(downloaded_object[0]['nested']['inner'], int)
        first_fixture_filtered = util.DataDict(
            {'id': self.dict_fixtures[0]['id'],
             'nested': {'inner': self.dict_fixtures[0]['nested']['inner']}})
        self.assertDictEqual(downloaded_object[0], first_fixture_filtered)

    def test_empty_json_upload(self):
        item = []
        name = 'empty.json'
        try:
            gcs.upload_data(item, self.bucket, name)
        except Exception, e:
            self.fail('Should not throw exception {}'.format(e))


if __name__ == '__main__':
    unittest.main()
