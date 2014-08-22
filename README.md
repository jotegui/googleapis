googleapis
==========

Python wrapper classes for accessing different Google Cloud APIs (https://developers.google.com/api-client-library/)

Installation
------------

Install the Google API Client for Python via pip or easy_install

` $ pip install --upgrade google-api-python-client

` $ easy_install --upgrade google-api-python-client

This module (the whole folder) should be available to Python by adding the parent folder to the PYTHONPATH environment variable.

Requirements
------------
client_secrets.json
cred

How it works
------------

Simply import the class module for the specific service you will use, and the credentials

' from googleapis import BigQuery
' from cred import bq_cred


