import os
import requests
import json
import datetime
import pytz
import urllib
import urllib.parse
import time

def getSchema():
    return {
        "schema": {
            "leads": {
                "primary_key": "id",
                "fields": [
                  {"name": "id", "type": ["null", {
                      "type": "string", "logicalType": "stringify-string"}], "default": None},

                ]
            },
            "persons": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "organizations": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "deals": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "notes": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "products": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            },
            "pipelines": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]


            },
            "activities": {
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": ["null", {
                        "type": "string", "logicalType": "stringify-string"}], "default": None},
                ]
            }

        }
    }

# Define helper function to run incremental

def should_run_incremental(table_name):
    match table_name:
        case "notes":
            return True
        case other:
            return False

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

# URL and API 

url_base = "https://domain.pipedrive.com/api/v1/"
api =  "&api_token=" + os.environ.get('API_BEARER_TOKEN')

#  HTTP call
def lambda_handler(event, context):
    #Make sure request is coming from Weld
    #try:
     #   authHeader = event["headers"]["authorization"]
   # except KeyError:
   #     authHeader = None
    
  #  if authHeader is None or authHeader.split(" ")[1] != os.environ.get('WELD_API_KEY'):
   #     return {
     #       'statusCode': 401,
     #       'body': 'Unauthorized'
     #   }

    path = event["rawPath"][1:]
    if path == "schema":
      return {
          'statusCode': 200,
          'body': json.dumps(getSchema())
        }
    body = json.loads(event["body"])
    state = body["state"]

 # Init API call params

    offset = 0
    limit = 500

    if (state.get("next_start")):
        offset = state.get("next_start")

    url_params = "?limit=" + str(limit) + "&start=" + str(offset)
    fields_url = None

 # Table to return
    output = ""
    if body["name"] == "leads":
        output = 'leads'
    if body["name"] == "persons":
        fields_url = 'personFields'
        output = 'persons'
    if body["name"] == "organizations":
        output = 'organizations'
    if body["name"] == "deals":
        fields_url = 'dealFields'
        output = 'deals'
    if body["name"] == "notes":
        output = 'notes'
    if body["name"] == "products":
        output = 'products'
    if body["name"] == "pipelines":
        output = 'pipelines'
    if body["name"] == "activities":
        output = 'activities'


    url_with_page = url_base + output + url_params + "&" + api

    state = body["state"]

     # If running incremental

    if (state.get('started')):
        started = state.get('started')
    else:
        started = datetime.date.today().strftime("%Y-%m-%d")

    
    pointer = None
    if should_run_incremental(output):
        if (state.get("pointer")):
            pointer = state.get("pointer")
        else:
            pointer = "1970-01-01"

    print(pointer)


    # 3.3 Check ratelimit

    if (state.get("ratelimit")):
        ratelimit_remaining = int(state.get("ratelimit").get("remaining"))
        ratelimit_reset = int(state.get("ratelimit").get("reset"))
    else:
        ratelimit_remaining = 1000
        ratelimit_reset = 100

    if (ratelimit_remaining < 5):
        current_time = time.time()
        wait_time = ratelimit_reset-current_time
        print("Ratelimit hit, waiting")
        print(current_time)
        print(ratelimit_reset)
        print(wait_time)
        time.sleep(wait_time)
    else:
        time.sleep(0.5)

    # request the Pipedrive API

    response = requests.get(url_with_page)
    if (response.status_code > 299):
        response.raise_for_status()
    data = json.loads(response.text)

    # rate limiting
    new_ratelimit_remaining = response.headers["X-Ratelimit-Remaining"]
    new_ratelimit_reset = response.headers["X-Ratelimit-Reset"]
    new_ratelimit = {
        "remaining": new_ratelimit_remaining,
        "reset": new_ratelimit_reset
    }

     # join to fields data 
    fields_data = None

    if (fields_url):
        fields_response = requests.get(url_base+fields_url+"?"+api)
        fields_data = json.loads(fields_response.text)["data"]
        print(fields_data)

    #  Format data to Weld format
    
    rows = []
    for row in data["data"]:
        dataRow = dict(row)


        d_objects = [k for k in dataRow.keys() if type(dataRow[k]) is dict]
        for d in d_objects:
            dataRow = flatten_data(dataRow)

        if (fields_data):
            new_data = dict(dataRow)
            keys = dataRow.keys()
            for field_key in keys:
                if (len(field_key) > 39):
                    field_data = [
                        d for d in fields_data if d['key'] == field_key][0]
                    if (field_data):
                        field_name = field_data["name"]
                        new_data[field_name] = dataRow[field_key]
                        del new_data[field_key]
            rows.append(new_data)

        else:
            rows.append(dataRow)

   # Handle pagination
    pagination_data = data["additional_data"]["pagination"]
    limit = pagination_data["limit"]
    hasmore = pagination_data["more_items_in_collection"]
    next_start = pagination_data["start"] + limit

    save_state = hasmore or (should_run_incremental(output) and not hasmore)

    return {
        "insert": rows,
        "state": {
            "limit": limit,
            "next_start": next_start if hasmore else 0,
            "ratelimit": new_ratelimit,
            "pointer": pointer if hasmore else started,
            "sync_started": started if hasmore else None
        },
        "hasMore": hasmore
    }