#!/usr/bin/env bash

virtualenv env
./env/bin/easy_install -U pip
./env/bin/pip install --upgrade -r requirements.txt

