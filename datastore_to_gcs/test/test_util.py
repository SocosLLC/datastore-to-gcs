# -*- coding: utf-8 -*-
# test_util.py
#
# Copyright 2015 Socos LLC
#

from datetime import datetime, date
import json
import os
from pprint import pprint
import sys
import unittest

import cumberbatch

import gae_stubs
gae_stubs.init()

from datastore_to_gcs import util


class TestUtil(unittest.TestCase):

    def test_parse_cloud_storage_path(self):
        base1 = '/bucket'
        rest1 = 'foo/bar'
        file_path1 = util.parse_cloud_storage_path(base1, rest1)
        self.assertEquals(file_path1, '/bucket/foo/bar')

        base2 = '/bucket/foo'
        rest2 = 'bar/baz'
        file_path2 = util.parse_cloud_storage_path(base2, rest2)
        self.assertEquals(file_path2, '/bucket/foo/bar/baz')

        base3 = 'bucket/foo/bar'
        rest3 = '/baz'
        file_path3 = util.parse_cloud_storage_path(base3, rest3)
        self.assertEquals(file_path3, '/bucket/foo/bar/baz')

        base4 = 'bucket/foo/'
        rest4 = '/bar/baz'
        file_path4 = util.parse_cloud_storage_path(base4, rest4)
        self.assertEquals(file_path4, '/bucket/foo/bar/baz')

    def test_parse_cloud_storage_path_split(self):
        base1 = '/bucket'
        rest1 = 'foo/bar'
        bucket1, path1 = util.parse_cloud_storage_path_split(base1, rest1)
        self.assertEquals(bucket1, 'bucket')
        self.assertEquals(path1, 'foo/bar')

        base2 = '/bucket/foo'
        rest2 = 'bar/baz'
        bucket2, path2 = util.parse_cloud_storage_path_split(base2, rest2)
        self.assertEquals(bucket2, 'bucket')
        self.assertEquals(path2, 'foo/bar/baz')

        base3 = 'bucket/foo/bar'
        rest3 = '/baz'
        bucket3, path3 = util.parse_cloud_storage_path_split(base3, rest3)
        self.assertEquals(bucket3, 'bucket')
        self.assertEquals(path3, 'foo/bar/baz')

        base4 = 'bucket/foo/'
        rest4 = '/bar/baz'
        bucket4, path4 = util.parse_cloud_storage_path_split(base4, rest4)
        self.assertEquals(bucket4, 'bucket')
        self.assertEquals(path4, 'foo/bar/baz')

    def test_serializable(self):

        class Thingo(object):
            foo = 0

            def __init__(self, i):
                self.bar = i

        unserializable = {
            'id': 'asdfghqwer1234',
            'subdict': {'id': '1234568',
                        'datetime': datetime(2000, 1, 1)},
            'datelist': [date(2014, 1, 1), date(2015, 1, 1)],
            'things': (Thingo(i) for i in range(5)),
            'tuple': (u'\U0001f509',)}
        output = util.serializable(unserializable)
        expected_output = {
            'id': 'asdfghqwer1234',
            'subdict': {'id': '1234568',
                        'datetime': '2000-01-01T00:00:00'},
            'datelist': ['2014-01-01', '2015-01-01'],
            'things': [{'bar': i} for i in range(5)],
            'tuple': [u'\U0001f509']}

        self.assertEqual(output, expected_output)

    def test_serializable_gae_keys(self):
        from google.appengine.ext import ndb
        input = {
            'id': 'asdfghqwer1234',
            'subdict': {'id': '1234568',
                        'key': ndb.Key('TestModel', '1')},
            'keylist': [ndb.Key('TestModel', '2'), ndb.Key('TestModel', '3')]}
        output = util.serializable(input)
        expected_output = {
            'id': 'asdfghqwer1234',
            'subdict': {'id': '1234568',
                        'key': '1'},
            'keylist': ['2', '3']}

        self.assertEqual(output, expected_output)
