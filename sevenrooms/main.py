import functions_framework
import os
import requests
import json
import datetime
import pytz
import urllib
import urllib.parse
import time
from datetime import date

def getSchema():
    return {
        "schema": {
            "reservations" : {
                "primary_key" : "id"
            },
            "waitlist" : {
                "primary_key" : "id"
            },
              "venues" : {
                "id" : "string"
            }
        }   
    }

# Get access token from SevenRooms API
url_auth = "https://api.sevenrooms.com/2_4/auth"


payload = {
  "client_id": os.environ.get('CLIENT_ID'),
  "client_secret": os.environ.get('CLIENT_SECRET')}

headers = {"Content-Type": "application/x-www-form-urlencoded"}

response = requests.post(url_auth, data=payload, headers=headers)

data = response.json()
print(data)
access_token = data['data']['token']

# API URL
url =  "https://api.sevenrooms.com/2_4/"


def should_run_incremental(table_name):
  match table_name:
    case "reservations/export":
      return True
    case "waitlist":
      return True
    case other:
      return False

def use_venue_ids(table_name):
  match table_name:
      case "venues":
        return False
      case other:
        return True
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
# HTTP call
@functions_framework.http
def handler(req):
    # If path is schema, return the schema to Weld
    path = req.full_path[1:-1]
    if path == "schema":
        return getSchema()

    body = json.loads(req.data)


    output = ""
    if body["name"] == "reservations":
        output = 'reservations/export'
    if body["name"] == "waitlist":
        output = 'waitlist'
    if body["name"] == "venues":
        output = 'venues'



    # URL
    url_with_page = url + output 
    print(url_with_page)

    # Call the API with the access token
    headers = {
        "Authorization": f"{access_token}"
    }

    body = json.loads(req.data)
    state = {}
    if (body.get("state")):
        state = body.get("state")

    rows = []
    cursor = None
    limit = 50 # Set the initial limit to 50

    if (state.get("cursor")):
        cursor = state.get("cursor")


    # If running incremental

    if(state.get('started')):
      started = state.get('started')
    else: 
      started = datetime.date.today().strftime("%Y-%m-%d")

    pointer = None
    if should_run_incremental(output):
      if(state.get("pointer")):
        pointer = state.get("pointer")
      else:
       pointer = "1970-01-01"
      

    print(pointer)

    venue_ids = []

    if(state.get("venue_ids")):
      venue_ids=state.get("venue_ids", [])
    else:
      venue_ids=[
      ]

    query = {
      
      "limit": limit,
      "cursor": cursor,
    }

    if(use_venue_ids(output)):
      query["venue_id"] = venue_ids[0] # Always take the first venue_id. This list will shrink during the run, always popping the first venue_id when it is done

    if(should_run_incremental(output)):
      query["from_date"]=pointer
      query["to_date"]=started
    
    # Add exponential backoff
    retry_count = 0
    response = None
    while retry_count < 5:
      url_with_page = url + output
      print(url_with_page)
      response = requests.get(url_with_page, headers=headers, verify=True, params=query)
      if response.status_code == 200:
        break
      else:
        retry_count += 1
        time.sleep(2 ** retry_count)

    if response is None:
       raise ValueError("Response was not set")
    
    if response.status_code > 299:
      return response.text, response.status_code
    
    data = json.loads(response.text)
    # Format data to Weld format
    if body["name"] == "reservations":
      rows.extend(data["data"]["results"])
      cursor = data["data"]["cursor"]
      limit = data["data"]["limit"]
    if body["name"] == "venues":
      rows.extend(data["data"]["results"])
      cursor = data["data"]["cursor"]
      limit = data["data"]["limit"]
    if body["name"] == "waitlist":
      rows.extend(data["data"][0]["waitlist_entries"])
      cursor = data["data"][0]["cursor"]
      limit = data["data"][0]["limit"]
  
    print(len(rows))
    has_more = len(rows) >= 50
    print(has_more)

    # If we are using venue ids and there is no more data, check if this was the last one
    # If last one, we finish
    # If there are more venue_ids we reset has_more to true and remove the first venue_id from the list
    if not has_more and len(venue_ids)>1 and use_venue_ids(output):
      venue_ids.pop(0)
      cursor = None
      has_more=True
    
    save_state = has_more or (should_run_incremental(output) and not has_more)
    
    return {
      "insert": rows,
      "state": {
        "limit": limit,
        "cursor": cursor if has_more else None,
        "pointer": pointer if has_more else started,
        "sync_started": started if has_more else None,
        "venue_ids": venue_ids if (has_more and use_venue_ids(output)) else None
      } if save_state else None,
      "hasMore": has_more, 
    }


