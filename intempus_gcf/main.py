import functions_framework
import sys
import json
import os
import requests
from datetime import datetime
import time
import logging

def getSchema():
    return {
        "schema": {
            "employee": {
                "primary_key": "id"
            },
            "contract": {
                "primary_key": "id"
            },
            "case": {
                "primary_key": "id"
            },
            "customer": {
                "primary_key": "id"
            },
            "company": {
                "primary_key": "id"
            },
            "work_report": {
                "primary_key": "id"
            }
        }
    }

# Define helper function to flatten nested dictionaries and lists
def flatten_data(y):
    nested_dict = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name)
                i += 1
        else:
            nested_dict[name[:-1]] = x

    flatten(y)
    return nested_dict

### Authentication
base_domain = 'https://intempus.dk'
api_path = '/web/v1/'

API_KEY = os.environ.get('API_BEARER_TOKEN')
USERNAME = os.environ.get('ACCOUNT_ID')

headers = {
    'content-type': 'application/json',
    'Authorization': 'ApiKey {user}:{apikey}'.format(user=USERNAME, apikey=API_KEY),
}

@functions_framework.http
def handler(req):
    ### State handlers
    if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
        return {"message": "Not authorized"}, 403

    path = req.full_path[1:-1]
    if path == "schema":
        return getSchema()

    body = json.loads(req.data)

    if body["state"] is None:
        body["state"] = {}
        state = {}
    else:
        state = body["state"]

    ## Handle different tables
    output = ""
    if body["name"] == "employee":
        output = 'employee'
    if body["name"] == "contract":
        output = 'contract'
    if body["name"] == "case":
        output = 'case'
    if body["name"] == "customer":
        output = 'customer'
    if body["name"] == "company":
        output = 'company'
    if body["name"] == "work_report":
        output = 'work_report'

    ### API call
    # Set the limit parameter
    limit = 5000

    next_url = state.get('next_url')
    if next_url:
        # Construct the full URL using the base domain and the relative path provided in 'next_url'
        url = f"{base_domain}{next_url}"
    else:
        # Initial URL construction
        url = f"{base_domain}{api_path}{output}/?pagination_type=cursor&limit={limit}"

    logging.info(f"Querying URL: {url}")  # Log the URL

    response = requests.get(url, headers=headers)
    data = json.loads(response.text)

    rows = []
    for row in data['objects']:
        dataRow = dict(row)

        # Flatten all remaining dictionary objects
        d_objects = [k for k in dataRow.keys() if type(dataRow[k]) is dict]
        for d in d_objects:
            dataRow = flatten_data(dataRow)
        rows.append(dataRow)

    next_url = data['meta']['next']  # Directly use the URL provided by the API
    has_more = bool(next_url)

    return {
        "insert": rows,
        "state": {"next_url": next_url if has_more else None},
        "hasMore": has_more
    }
