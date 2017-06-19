# test_datastore_to_gcs.py
#
# Copyright 2015 Socos LLC
#

import datetime
import itertools
import logging
from pprint import pformat, pprint
import unittest

import gae_stubs
gae_stubs.init()

from google.appengine.ext import ndb

import datastore_to_gcs
import datastore_to_gcs.cloud_storage as gcs


LOG = logging.getLogger(__name__)


class TestModel(datastore_to_gcs.BaseModel):
    email = ndb.StringProperty(required=True, indexed=True)


class TestDatastoreToGCS(unittest.TestCase):

    fixtures = []
    object_name = 'users.json'
    bucket = 'test-bucket'

    def setUp(self):
        gae_stubs.init()
        for i in xrange(40):
            self.fixtures.append(TestModel(id=1000+i,
                                           email='%d@example.com' % (1000+i)))

    def tearDown(self):
        gae_stubs.close()

    def run_datastore_to_gcs_dump(self, since=None):
        datastore_to_gcs.dump(TestModel, self.bucket, self.object_name, since=since)

    def run_datastore_to_gcs_update(self):
        datastore_to_gcs.update(TestModel, self.bucket, self.object_name)

    def initial_dump(self):
        ndb.put_multi(self.fixtures[0:10])
        self.run_datastore_to_gcs_dump()

    def test_initial_dump(self):
        self.initial_dump()
        # Assert that the transferred object exists
        self.assertIn(self.object_name, gcs.list_objects(self.bucket))
        transferred_items = gcs.download_object(self.bucket, self.object_name)
        # Assert that it has the right number of items
        self.assertEqual(len(transferred_items), 10)
        # Assert that field names are right
        self.assertTrue('email' in transferred_items[4])
        self.assertTrue('id' in transferred_items[9])
        self.assertTrue('last_modified' in transferred_items[0])

    def test_dump_log(self):
        log_dir = 'log_dir/'
        ndb.put_multi(self.fixtures[0:10])
        self.assertEqual(TestModel.query().count(), 10)
        datastore_to_gcs.dump_log(TestModel, self.bucket, log_dir)
        log_files = gcs.list_objects(self.bucket, log_dir)
        self.assertEqual(len(log_files), 1)
        isoformat_regex = '\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.json'
        self.assertRegexpMatches(log_files[0], isoformat_regex)
        log_files = gcs.list_objects(self.bucket, log_dir)
        print log_files
        self.assertEqual(len(log_files), 1)
        log_items = list(itertools.chain(
            *[gcs.download_object(self.bucket, log_dir + lf)
            for lf in log_files]))
        print log_items
        self.assertEqual(len(log_items), 10)

    def test_empty_dump(self):
        try:
            self.run_datastore_to_gcs_dump()
        except Exception, e:
            LOG.exception(e)
            self.fail('Failed with exception {}'.format(e))

    def test_empty_dump_since(self):
        since = datetime.datetime.now() - datetime.timedelta(days=3)
        try:
            self.run_datastore_to_gcs_dump(since=since)
        except Exception, e:
            LOG.exception(e)
            self.fail('Failed with exception {}'.format(e))

    def test_empty_update(self):
        try:
            self.run_datastore_to_gcs_update()
        except Exception, e:
            self.fail('Failed with exception {}'.format(e))

    def test_nothing_new_to_update(self):
        self.initial_dump()
        initial_result = gcs.download_object(self.bucket, self.object_name)
        self.assertIn(self.object_name, gcs.list_objects(self.bucket))
        # Don't change anything but run update -- should leave file as is
        self.run_datastore_to_gcs_update()
        updated_result = gcs.download_object(self.bucket, self.object_name)
        self.assertTrue(initial_result == updated_result,
                        msg='{}\ndoes not match{}'.format(pformat(updated_result),
                                                          pformat(initial_result)))

    def add_new_items(self):
        ndb.put_multi(self.fixtures[10:15])
        self.run_datastore_to_gcs_update()

    def test_add_new_items(self):
        self.initial_dump()  # Adds 10 items
        self.add_new_items()  # Adds 5 items
        transferred_items = gcs.download_object(self.bucket, self.object_name)
        pprint(transferred_items)
        self.assertEqual(len(transferred_items), 15)

    def test_modify_existing(self):
        self.initial_dump()
        new_email = 'changed@example.com'
        orig_results = TestModel.query(TestModel.email == self.fixtures[6].email).fetch()
        self.assertEqual(len(orig_results), 1)
        orig = orig_results[0]
        print 'ORIG:'
        print orig
        orig.email = new_email
        orig.put()
        self.run_datastore_to_gcs_update()
        transferred_items = gcs.download_object(self.bucket, self.object_name)
        new_results = [e for e in transferred_items if e['id'] == orig.key.id()]
        self.assertEqual(1, len(new_results))
        modified = new_results[0]
        print 'MODIFIED:'
        print modified
        self.assertEqual(modified['email'], new_email)
