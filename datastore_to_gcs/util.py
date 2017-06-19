from datetime import date, datetime
import enum
from toolz.dicttoolz import get_in, assoc_in

from google.appengine.ext.ndb import Key


def filter_paths(item, paths):
    filtered_item = {}
    for key_path in paths:
        value = get_in(key_path, item)
        filtered_item = assoc_in(filtered_item, key_path, value)
    return filtered_item


def parse_cloud_storage_path(base, rest):
    if base[0] != '/':
        base = '/' + base
    if base[-1] == '/':
        base = base[:-1]
    if rest and rest[0] == '/':
        rest = rest[1:]
    return '/'.join((base, rest)) if rest else base


def parse_cloud_storage_path_split(base, rest):
    if base[0] == '/':
        base = base[1:]
    if base[-1] == '/':
        base = base[0:-1]
    base_parts = base.split('/')
    bucket = base_parts[0]
    object_name = ''
    if len(base_parts) > 1:
        object_name = '/'.join(base_parts[1:]) + '/'
    if rest[0] == '/':
        rest = rest[1:]
    object_name += rest
    return bucket, object_name


def serializable(item):
    """Converts item into an equivalent JSON serializable object.

    Parameters
    ----------
    item : T

    Returns
    -------
    dict
    """
    # Okay, go through the actual recursive serialization
    if isinstance(item, dict):
        result = {}
        for key, val in item.items():
            result[key] = serializable(val)
    elif isinstance(item, (list, tuple)) or hasattr(item, '__iter__'):
        result = []
        for i in item:
            result.append(serializable(i))
    elif isinstance(item, datetime) or isinstance(item, date):
        result = item.isoformat()
    elif isinstance(item, Key):
        result = item.id()
    elif isinstance(item, enum.Enum):
        result = item.name
    elif hasattr(item, 'serializable') and callable(item.serializable):
        result = item.serializable()
    elif hasattr(item, '__dict__'):
        # Note that this doesn't include class attributes
        result = serializable(item.__dict__)
    else:
        result = item

    return result


class DataDict(dict):
    """ A wrapper for dict which prints the object in case of a KeyError. """

    def __getitem__(self, key):
        try:
            val = dict.__getitem__(self, key)
        except KeyError:
            raise KeyError('{0} not found in {1}'.format(key, self.keys()))

        return val

    def __getattr__(self, key):
        try:
            val = dict.__getitem__(self, key)
            return val
        except KeyError:
            raise AttributeError('{0} not found in {1}'.format(key, self.keys()))


class CommonEqualityMixin(object):

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

