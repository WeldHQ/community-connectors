import functions_framework
import os
import base64
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
            "shifts" : {
                "primary_key" : "id"
            },
            "events" : {
                "primary_key" : "id"
            },
            "users" : {
                "primary_key" : "id"
            },
            "absence" : {
                "primary_key" : "id"
            },
            
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

# Set the API endpoint and credentials
endpoint_auth = "https://api.rotaready.com/oauth/token"
realm = os.environ.get('REALM')
client_key = os.environ.get('CLIENT_KEY')
client_secret = os.environ.get('CLIENT_SECRET')

def _get_access_token():
    # Encode the credentials
    credentials = f"{realm}:{client_key}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    # Set the request headers
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    # Set the request payload
    data = {
        "grant_type": "client_credentials"
    }
    # Send the request
    response = requests.post(endpoint_auth, headers=headers, data=data)

    # Get the access token from the response
    if response.status_code == 200:
        access_token = response.json()["access_token"]
        print(f"Access Token: {access_token}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

    return access_token


# HTTP call
@functions_framework.http
def handler(req):
    # If path is schema, return the schema to Weld
    path = req.full_path[1:-1]
    
    if path == "schema":
        return getSchema()

    body = json.loads(req.data)


    endpoint_attendance = "https://api.rotaready.com/attendance/events/paginated"
    endpoint_users = f"https://api.rotaready.com/staff/account/paginated"
    endpoint_shifts = "https://api.rotaready.com/rota/shift/paginated"
    endpoint_absence = "https://api.rotaready.com/absence/paginated"

    attendance_limit = 100
    users_limit = 50
    shifts_limit = 100
    absence_limit = 100

    
    start = "2023-06-12"
    start_date_max = datetime.datetime.now(pytz.timezone('Europe/London')).date() + datetime.timedelta(days=14)
    start_date_max = start_date_max.strftime("%Y-%m-%d") 
    page = 1 

    params_shifts = {
            "startDateMin": "2023-06-12",
            "startDateMax": start_date_max,
            "limit": shifts_limit,
            "page": page
        
        }
    
    params_attendance = {
                "start": start,
                "limit": attendance_limit,
                "page": page
            }
    
    params_users = {
                "limit": users_limit,
                "page": page
            }
    
    params_absence = {
                "limit": absence_limit,
                "page": page
            }
    
    params = {}
    endpoint = ""
    if body["name"] == "shifts":
        endpoint = endpoint_shifts
        limit = shifts_limit
        params = params_shifts
    if body["name"] == "events":
        endpoint = endpoint_attendance
        limit = attendance_limit
        params = params_attendance
    if body["name"] == "users":
        endpoint = endpoint_users
        limit = users_limit
        params = params_users
    if body["name"] == "absence":
        endpoint = endpoint_absence
        limit = absence_limit
        params = params_absence

    headers = {
        "Authorization": f"Bearer {_get_access_token()}",
        "Content-Type": "application/json"
    }
    
    body = json.loads(req.data)
    state = {}
    if (body.get("state")):
        state = body.get("state")
    
    if(state.get("next_page")):
      page = state.get("next_page")


            # Send the request
    # adjust the page number
    params["page"] = page
    response = requests.get(endpoint, headers=headers, params=params)

            # Process the response
    if response.status_code == 200:
                df = response.json()
              
    else:
            print(f"Error: {response.status_code} - {response.text}")     
           
    # Return JSON response
            return df
    
    response_data = response.json()   
    # get name of table
    table_name = body["name"]    
    shifts = response_data.get(table_name, [])
    data = json.loads(response.text)

        #3.5 Format data to Weld format
    rows = []
    for row in shifts:
      dataRow = dict(row)
      
        # Flatten all remaining dictionary objects
      d_objects = [k for k in dataRow.keys() if type(dataRow[k]) is dict]
      for d in d_objects:
          dataRow = flatten_data(dataRow)
      rows.append(dataRow)

  
    print(len(data))
    print(len(shifts))
    print(page)
    next_page = page + 1
    has_more = len(shifts) >= limit 
 


    return {
      "insert": rows,
      "state": {
        "limit": limit,
        "page": page,
        "next_page": next_page
      }, 
      "hasMore": has_more
    }
