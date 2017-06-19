datastore-to-gcs
================

Transfers data from Google Cloud Datastore to Google Cloud Storage.
Intended to be used from Google App Engine.


Usage
-----

::

    >>> import datastore_to_gcs
    >>> from google.appengine.ext import ndb
    >>>
    >>> class Foo(datastore_to_gcs.BaseModel):
    ...     email = ndb.StringProperty()
    ...
    >>> f = Foo(email='a@b.com')
    >>> f.put()
    Key('Foo', 1)
    >>> datastore_to_gcs.update(Foo, 'bucket-name', 'foo.json')
    >>> f2 = Foo(email='b@b.com')
    >>> f2.put()
    Key('Foo', 2)
    >>> import datastore_to_gcs.cloud_storage as gcs
    >>> gcs.download_object('bucket-name', 'foo.json')
    [{u'last_modified': u'2017-06-19T18:02:01.332908', u'email': u'a@b.com', u'id': 1}]
    >>> datastore_to_gcs.update(Foo, 'bucket-name', 'foo.json')
    >>> gcs.download_object('bucket-name', 'foo.json')
    [{u'last_modified': u'2017-06-19T18:02:01.332908', u'email': u'a@b.com', u'id': 1}, {u'last_modified': u'2017-06-19T18:03:09.342067', u'email': u'b@b.com', u'id': 2}]
    >>>
    >>>
    >>> class Message(datastore_to_gcs.BaseModel):
    ...     text = ndb.StringProperty()
    ...
    >>> m1 = Message(text='hello 1')
    >>> m1.put()
    Key('Message', 3)
    >>> datastore_to_gcs.dump_log(Message, 'bucket-name', 'messages/')
    '2017-06-19T18:05:07.json'
    >>> m2 = Message(text='hello 2')
    >>> m2.put()
    Key('Message', 4)
    >>> m3 = Message(text='hello 3')
    >>> m3.put()
    Key('Message', 5)
    >>> datastore_to_gcs.dump_log(Message, 'bucket-name', 'messages/')
    '2017-06-19T18:06:02.json'
    >>> gcs.download_object('bucket-name', 'messages/2017-06-19T18:05:07.json')
    [{u'text': u'hello 1', u'last_modified': u'2017-06-19T18:04:33.558426', u'id': 3}]
    >>> gcs.download_object('bucket-name', 'messages/2017-06-19T18:06:02.json')
    [{u'text': u'hello 1', u'last_modified': u'2017-06-19T18:04:33.558426', u'id': 3}, {u'text': u'hello 2', u'last_modified': u'2017-06-19T18:05:35.084417', u'id': 4}, {u'text': u'hello 3', u'last_modified': u'2017-06-19T18:05:50.859952', u'id': 5}]



Contributing
------------

Pull requests welcome!
https://github.com/SocosLLC/datastore-to-gcs


About Socos
-----------

Socos LLC is the company behind `Muse <https://muse.socoslearning.com>`_.

Muse brings your child's everyday experiences to life and supports
their development through research-based activities. Muse gives you
insights into your child's development and shows you what you can do
each day to support their growth.

