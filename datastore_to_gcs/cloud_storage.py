# cloud_storage.py
#
# Copyright 2015 Socos LLC
#
"""
This is Google Cloud Storage client for Google App Engine. It's a wrapper
around cloudstorage.
"""

# Imports ######################################################################

from __future__ import print_function
import collections
import json
import logging
from pprint import pformat

import cloudstorage

from datastore_to_gcs import util


LOG = logging.getLogger(__name__)


# Interface ####################################################################

def list_objects(path_base, rest_of_path=''):
    """
    Lists the objects at the specified path in GCS.

    Parameters
    ----------
    path_base : str
    rest_of_path : str

    Returns
    -------
    list[str]
        A list of filenames

    """
    path_to_dir = util.parse_cloud_storage_path(path_base, rest_of_path)
    path_to_dir = path_to_dir[:-1] if path_to_dir[-1] == '/' else path_to_dir
    items = cloudstorage.listbucket(path_to_dir)
    relative_paths = (i.filename[len(path_to_dir) + 1:] for i in items if not i.is_dir)
    # @bistenes: Can't tell you why, but the above line also produces an
    #   entry for the directory despite the is_dir check, but *only in
    #   production*. Couldn't repro in testing or staging. The relative
    #   path from the directory to itself, as computed by the line above,
    #   is of course ''. Hence the following return, which filters out
    #   those entries.
    return [r for r in relative_paths if r != '']


def exists(path_base, rest_of_path=''):
    objs = list_objects(path_base)
    return rest_of_path in objs


def download_object(path_base, rest_of_path, needed_fields=(), object_class=util.DataDict):
    """
    Loads the data at file_path into memory, interpreting it as lines of JSON

    Parameters
    ----------
    path_base : str
        The bucket or directory of the object to be downloaded
    rest_of_path : str
        The path of the object to be downloaded
    needed_fields : list[str] | tuple[str]
        The fields to filter each object down to. Nested fields are supported
        using '.' as separator, i.e. in `{'foo': {'bar': 'baz'}}`, the `bar`
        field can be filtered for by passing `needed_fields=['foo.bar']`
    object_class : type
        A class to wrap each line of data in

    Returns
    -------
    list[object_class]
        Representing the data in the downloaded object
    """
    gcs_file = cloudstorage.open(util.parse_cloud_storage_path(path_base, rest_of_path))
    item_dicts = _objects_from_file(gcs_file, needed_fields)
    gcs_file.close()
    return _parse_result(item_dicts, object_class)


def upload_data(input_list, path_base, rest_of_path):
    """
    Uploads input_list to a file at the given path.

    Each item of input_list gets passed through util.serializable
    and then serialized as JSON. Each item gets one line in the file, producing
    a file for which each line is a valid JSON object.

    Parameters
    ----------
    input_list : list[object]
        Non-empty iterable of objects to be serialized
    path_base : str
        The first part of the path, including the bucket name
    rest_of_path : str
        The rest of the path, including the filename

    Returns
    -------
    The (bucket, object) path of the uploaded object, or None if failed.
    """
    assert isinstance(input_list, collections.Iterable)
    file_path = util.parse_cloud_storage_path(path_base, rest_of_path)
    # Abort if input_list is empty
    if not any(True for _ in input_list):
        LOG.warn('Refusing to upload empty file to ' + file_path)
        return None
    # Carry out the upload
    gcs_file = cloudstorage.open(file_path, mode='w', content_type='text/plain')
    for item in input_list:
        ser_item = util.serializable(item)
        json.dump(ser_item, gcs_file, encoding='utf-8')
        gcs_file.write(_encode('\n'))
    gcs_file.close()
    return util.parse_cloud_storage_path_split(path_base, rest_of_path)


def log_name(utc_dt):
    dt_string = utc_dt.replace(microsecond=0).isoformat()
    return dt_string + '.json'


# Implementation ###############################################################

def _encode(raw):
    return raw.encode('utf-8')


def _decode(utf8):
    return utf8.decode('utf-8')


def _objects_from_file(file_object, needed_fields):
    file_object.seek(0)
    items = []
    for linenum, line in enumerate(file_object):
        try:
            items.append(json.loads(line))
        except ValueError as e:
            raise ValueError('Error at line %d: %s' % (linenum, e))
    if len(needed_fields) > 0:
        for i in range(len(items)):
            items[i] = util.filter_paths(items[i], [p.split('.') for p in needed_fields])
    return items


def _parse_result(item_dicts, object_class):
    result = []
    for item_dict in item_dicts:
        try:
            result.append(object_class(**item_dict))
        except TypeError, e:
            LOG.error('TypeError on\n{}'.format(pformat(item_dict, indent=4)))
            raise e
    return result

