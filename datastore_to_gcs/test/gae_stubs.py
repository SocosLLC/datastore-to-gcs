# gae_stub.py
#
# Copyright 2015 Socos LLC
#
"""
Makes the Datastore stub available to clients running outside of GAE.

Example:
    >>> import unittest
    ... import gae_stub
    ... # init up here to make appengine includes available
    ... gae_stub.init()
    ... from google.appengine.ext import ndb
    ...
    ... class TestStuff(unittest.TestCase):
    ...     def setUp(self):
    ...         gae_stub.init()  # re-init here to clear caches
    ...     def tearDown(self):
    ...         gae_stub.close()  # close to deactivate testbed
    ...     def test_things(self):
    ...         # Tests that use GAE! Woohoo!
    ...         self.assertIsNotNone(ndb.get_context())  # or whatever
"""

import logging
import os
import sys

_testbed = None

# Reduce verbosity of GAE stubs
handlers = logging.getLogger('').handlers
if handlers:
    handlers[0].setLevel(logging.INFO)


def gae_path():
    env_var_path = os.getenv('GAE_SDK_PATH')
    if env_var_path:
        return env_var_path

    default = '/usr/local/google_appengine'
    if os.path.isdir(default):
        return default

    raise Exception('GAE not found at /usr/local/google_appengine and GAE_SDK_PATH not defined.')


def init():
    global _testbed
    # Make sure the correct 'google' module is in sys.modules
    if 'google' in sys.modules:
        del sys.modules['google']
    sys.path.insert(0, gae_path())
    # Make sure remote_api_stub deps are in path
    sys.path.append(os.path.join(gae_path(), 'lib', 'yaml', 'lib'))
    sys.path.append(os.path.join(gae_path(), 'lib', 'fancy_urllib'))

    from google.appengine.ext import ndb
    from google.appengine.ext import testbed

    # First, create an instance of the Testbed class.
    _testbed = testbed.Testbed()
    # Then activate the testbed, which prepares the service stubs for use.
    _testbed.activate()
    # Next, declare which service stubs you want to use.
    _testbed.init_app_identity_stub()
    _testbed.init_blobstore_stub()
    _testbed.init_datastore_v3_stub()
    _testbed.init_memcache_stub()
    _testbed.init_urlfetch_stub()
    # Clear ndb's in-context cache between tests.
    # This prevents data from leaking between tests.
    # Alternatively, you could disable caching by
    # using ndb.get_context().set_cache_policy(False)
    ndb.get_context().clear_cache()


def close():
    global _testbed
    _testbed.deactivate()
