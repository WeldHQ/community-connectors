import functions_framework
import sys
import json
import os
import requests
from datetime import datetime
import time

def getSchema():
    return {
    "schema": {
            "time_entries" : {
                "primary_key": "id"
            }, 
            "projects" : {
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
base_url = 'https://api.harvestapp.com/v2/'
payload={}

headers = {
      'Harvest-Account-ID': os.environ.get('ACCOUNT_ID'),
      'Authorization': 'Bearer '+ os.environ.get('API_BEARER_TOKEN'),
      'User-Agent': 'Harvest API', 
      'content-type':'application/json'
    }

@functions_framework.http
def handler(req):
    ### State handlers
    ##if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
    ##   return { "message": "Not authorized" }, 403

    path = req.full_path[1:-1]
    if path == "schema":
        return getSchema()

    body = json.loads(req.data)

    if(body["state"] is None):
        body["state"] = {}
        state={}
    else:   
        state=body["state"]

    if(state.get('started')):
        started = state.get('started')
    else: 
        started = datetime.now().strftime("%Y-%m-%d")## datetime.date.today().strftime("%Y-%m-%d")

    ## Handle different tables 
    output = ""
    if body["name"] == "time_entries":
      output = 'time_entries'
    if body["name"] == "projects":
      output = 'projects'
    
    ### API call 
    today=datetime.now().strftime("%Y-%m-%d")
    start='2022-03-14'

    current_page = 1

    if(state.get("next_page")):
      current_page = state.get("next_page")

    url= base_url+output+"?page=" + format(current_page)
    response = requests.get(url, headers=headers)
    
    data = json.loads(response.text)

    rows = []
    for row in data[output]:
        dataRow = dict(row)

        # Flatten all remaining dictionary objects
        d_objects = [k for k in dataRow.keys() if type(dataRow[k]) is dict]
        for d in d_objects:
            dataRow = flatten_data(dataRow)
        rows.append(dataRow)

    ## Handle pagination
    next_page = current_page + 1
    has_more = len(rows)>0
    return { 
            "insert": rows,
            "state": {"next_page": next_page if has_more else None}, 
            "hasMore": has_more, 
        }