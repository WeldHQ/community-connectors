import os
import json
import datetime
import urllib
import urllib.parse
import time
import requests
from datetime import timedelta

def getSchema():
  return {
    "schema": {
      "accounts" : {

        "primary_key": "id",
      },
      "people" : {

        "primary_key": "id",
      }
      
    }
  }
  
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

url =  "https://api.salesloft.com/v2/"
headers = {"Authorization": "Bearer " + os.environ.get("API_BEARER_TOKEN")}

def lambda_handler(event, context):
    path = event["rawPath"][1:]
    if path == "schema":
      return {
          'statusCode': 200,
          'body': json.dumps(getSchema())
        }

    body = json.loads(event["body"])

    if(body["state"] is None):
      body["state"] = {}
     
    print('Body')
    print(body)


    state = body["state"]
    ratelimit_remaining = ""
    ratelimit_cost = ""
    

    output = ""
    if body["name"] == "accounts":
      output = 'accounts'
    if body["name"] == "people":
      output = 'people'
   
    current_page = 1

    if(state.get("next_page")):
      current_page = state.get("next_page")

     # URL 
    
    url_with_page = url + output  + "/?page=" + format(current_page)
    print(url_with_page)

    #3.3 Check ratelimit
    if(state.get("ratelimit")):
      ratelimit_remaining = int(state.get("ratelimit").get("remaining"))
      ratelimit_cost = int(state.get("ratelimit").get("cost"))

      print("Rate limit info")
      print(ratelimit_cost)
      print(ratelimit_remaining)

    if(ratelimit_remaining < ratelimit_cost):
        time.sleep(60)
    else:
        time.sleep(0.5)


    #3.4 Call the API
    response = requests.get(url_with_page, headers=headers)

    new_ratelimit = {
      "remaining": response.headers["x-ratelimit-remaining-minute"],
      "cost": response.headers.get("x-ratelimit-endpoint-cost", 1),
    }

    if(response.status_code>299):
      if(response.status_code == 429):
        if(retries==3):
          return response.text, response.status_code
        # setting rate limit to values that will make the code wait
        state['ratelimit'] = {
           "remaining":0,
           "cost":1
        }
        body['state'] = state

        data_str = json.dumps(body)

        setattr(req, "data", data_str)
        return handler(req, retries+1)
      return response.text, response.status_code
    data = json.loads(response.text)

    #3.5 Format data to Weld format
    rows = []
    data_row = data["data"]
    for row in data_row:
      dataRow = dict(row)
      
        # Flatten all remaining dictionary objects
      d_objects = [k for k in dataRow.keys() if type(dataRow[k]) is dict]
      for d in d_objects:
          dataRow = flatten_data(dataRow)
 
      rows.append(dataRow)


    
    #save_state = has_more 

    #Handle pagination
    pagination_data = data["metadata"]["paging"]
    per_page = pagination_data["per_page"]
    current_page = pagination_data["current_page"]
    next_page = pagination_data["next_page"]
    prev_page = pagination_data["prev_page"]

    has_more = next_page is not None
   


    return {
        "insert": rows,
        "state": {
          "next_page":next_page if has_more else None,
          "ratelimit": new_ratelimit,
        } ,
        "hasMore" : has_more
        
    }
