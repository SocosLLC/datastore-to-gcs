#!/bin/sh

TESTARGS=${@:-"datastore_to_gcs/test/"}

./env/bin/nosetests ${TESTARGS}

