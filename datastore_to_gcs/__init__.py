# __init__.py
#
# Copyright 2015 Socos LLC
#

import cloud_storage, util


# Imports / Setup ##############################################################

# Core
import datetime
import dateutil.parser
import logging
import os
from pprint import pprint, pformat

from google.appengine.ext import ndb


LOG = logging.getLogger(__name__)


# NDB Base Model ###############################################################

class BaseModel(ndb.Model):
    """ Inherit from this model to support datastore_to_gcs transfers """

    last_modified = ndb.DateTimeProperty(required=True, auto_now=True)

    def serializable(self):
        obj = self.to_dict()
        obj = util.serializable(obj)
        if self.key:
            obj['id'] = self.key.id()
        return obj


# Interface ####################################################################

def dump(model, gcs_bucket, gcs_object, since=None):
    if since:
        new_items_iter = model.query(model.last_modified > since).iter()
    else:
        new_items_iter = model.query().iter()
    new_items = [i.serializable() for i in new_items_iter]
    if len(new_items) > 0:
        LOG.info("Uploading {n} items in {m} to {o}".format(n=len(new_items),
                                                            m=model._get_kind(),
                                                            o=gcs_object))
        cloud_storage.upload_data(new_items, gcs_bucket, gcs_object)
    else:
        LOG.info("No items in {m} to transfer to {o}".format(m=model._get_kind(),
                                                             o=gcs_object))


def dump_log(model, gcs_bucket, gcs_log_directory, since=None):
    """ Creates a timestamped file in GCS containing new entities

    The timestamp of the previous log file is used to determine what date
    to pull entities from.

    Parameters
    ----------
    model : ndb.Model
    gcs_bucket : str
    gcs_object : str
    """
    log_name = cloud_storage.log_name(datetime.datetime.utcnow())
    gcs_object = os.path.join(gcs_log_directory, log_name)
    dump(model, gcs_bucket, gcs_object, since)
    return log_name


def update(model, gcs_bucket, gcs_object):
    """Updates the given GCS object with new data from the given model.

    Uses last_modified to determine the date to get items from. Bases the
    identity of entities in the GCS object on their 'id' field -- existing
    entities for which new data is found will be replaced.

    Parameters
    ----------
    model : ndb.Model
    gcs_bucket : str
    gcs_object : str
    """
    # If file doesn't exist, just dump
    if not cloud_storage.exists(gcs_bucket, gcs_object):
        LOG.info('No object to update, calling dump(...)')
        return dump(model, gcs_bucket, gcs_object)

    # Get preexisting items
    transferred_items = cloud_storage.download_object(gcs_bucket, gcs_object)
    LOG.info('{} items exist'.format(len(transferred_items)))

    # Find the most recently modified one
    last_date = datetime.datetime(1, 1, 1)
    for item in transferred_items:
        modified_date = dateutil.parser.parse(item['last_modified'])
        if modified_date > last_date:
            last_date = modified_date

    # Get all items modified after that date
    LOG.info('Last date on record: {}'.format(last_date.isoformat()))
    new_items_iter = model.query(model.last_modified > last_date).iter()
    new_items = tuple(item.serializable() for item in new_items_iter)
    new_items_by_id = {i['id']: i for i in new_items}

    if new_items:  # Found new items -- update existing items GCS
        items_by_id = {i['id']: i for i in transferred_items}
        items_by_id.update(new_items_by_id)
        items = items_by_id.values()
        LOG.info("Updating {n} items in {m} to {o}".format(n=len(new_items),
                                                           m=model._get_kind(),
                                                           o=gcs_object))
        cloud_storage.upload_data(items, gcs_bucket, gcs_object)
    else:  # Nothing to update with.
        LOG.info("No new items in {m} to append to {o}".format(m=model._get_kind(),
                                                               o=gcs_object))


# Private Methods ##############################################################

def _transfer_log(gcs_bucket, model, target):
    last_log = _last_log_datetime(gcs_bucket, target)
    LOG.debug('Last {} log found: {}'.format(model.__name__, last_log))
    res = dump_log(model,
                   gcs_bucket,
                   target,
                   since=last_log)
    if res:
        log_name = res
        LOG.debug('Saved {} log: {}'.format(model.__name__, log_name))


def _last_log_datetime(gcs_bucket, gcs_dir):
    """ Finds the last date for files under /gcs_bucket/gcs_dir

    Files must be named as ISO formatted datetime strings with the .json extension.

    :return: datetime.datetime
    :raises: ValueError if a file in the directory has a noncompliant name
    """
    items_in_dir = cloud_storage.list_objects(gcs_bucket, gcs_dir)
    LOG.debug('Directory contains: {}'.format(items_in_dir))
    if len(items_in_dir) == 0:
        return None  # since=None just defaults to get all entries from datastore.

    # item[:-5] removes .json extension
    isostrings = [item[:-5] for item in items_in_dir]
    try:
        dts = [dateutil.parser.parse(item) for item in isostrings]
        return max(dts)
    except ValueError, e:
        LOG.error(u"Couldn't parse date in {}".format(pformat(isostrings)))
        raise e

