import requests
import json
import time
import os
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
from oauthlib.oauth2 import BackendApplicationClient
import functions_framework
import datetime
import pytz

CLIENT_ID = "XXX"
CLIENT_SECRET = os.environ.get('ZDIRECT_CLIENT_SECRET', 'ZDIRECT_CLIENT_SECRET environment variable is not set.')
MERCHANT_ID = "XXX"

def getSchema():
  """
  Returns Schema for Weld Integration
  """
  return {
    "schema": {
      "order" : {"primary_key" : "id"},
      "order_item": {"primary_key": "id"},
      "order_line": {"primary_key": "id"}
    }
  }

def request_api_key(client_id, client_secret):
  """
  Function for requesting new Bearer token from Zalando zDirect API.
  Returns access token, as well as expiry datetime for that token in order to minimize the number of requests to the Auth API
  """
  args = {'grant_type': 'client_credentials', 
          'Content-Type': 'application/x-www-form-urlencoded'}
  # Create auth client and fetch bearer token
  auth = HTTPBasicAuth(client_id, client_secret)
  client = BackendApplicationClient(client_id=client_id)
  oauth = OAuth2Session(client=client)
  token = oauth.fetch_token(token_url='https://api.merchants.zalando.com/auth/token', auth=auth, kwargs=args)

  access_token = token['access_token']
  expiry_dt = datetime.datetime.fromtimestamp(token['expires_at'])
  return access_token, expiry_dt

def request_orders(url, payload, headers, included=False):
  """
  Function for requesting orders from a given URL.
  Returns all orders for that URL, and returns next_url in case there are multiple pages for the given period.
  """
  print(f"Requesting data from URL: {url}")
  response = requests.get(url=url, headers=headers, params=payload)

  while response.status_code == 429:
    print("Sent too many requests, reading response header and waiting before retrying.")
    retry_secs = int(response.headers.get('Retry-After', 9))
    print(f'Sleeping {retry_secs + 1}')
    time.sleep(retry_secs+1)
    response = requests.get(url=url, headers=headers, params=payload)

  data = json.loads(response.text)
  # Return order data or order_line/items if included set to True
  row_list = data['included'] if included else data['data']

  # Check for a link to the next page
  if 'next' in data['links']:
    next_url = data['links']['next']
  else:
    next_url = None

  return row_list, next_url

@functions_framework.http
def handler(req):
    # make sure request is coming from Weld
    if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
        return { "message": "Not authorized" }, 403

    # if path is schema, return the schema to Weld
    path = req.full_path[1:-1]
    if path == "schema":
      return getSchema()
    
    # Get data for correct granularity/endpoint
    request_json = req.get_json()
    name = request_json["name"]
    request_state = request_json["state"]

    print(f"{request_json=}")

    # Check if last Bearer Token is already expired (or is within 5 mins of expiring)
    if request_state.get("key_expiry") and request_state.get("last_api_key"):
      # check if last api key has already expired and request new one if so
      expiry_threshold = datetime.datetime.now() + datetime.timedelta(minutes=5)
      last_expiry_time = datetime.datetime.fromisoformat(request_state.get("key_expiry"))
      if expiry_threshold > last_expiry_time:
        print("Requesting new API Token as last one expired.")
        api_key, expiry_dt = request_api_key(CLIENT_ID, CLIENT_SECRET)
        expiry_time = expiry_dt.isoformat('T')
      else:
        print("Reusing last API Token.")
        api_key = request_state.get("last_api_key")
        expiry_time = request_state.get("key_expiry")
    else:
      print("Requesting new API Token.")
      api_key, expiry_dt = request_api_key(CLIENT_ID, CLIENT_SECRET)
      expiry_time = expiry_dt.isoformat('T')

    # Process previous state
    if request_state.get("orders_last_updated_after"):
      # Define order request parameters, using incremental sync cursor where applicable
      o_start_time = request_state.get("orders_last_updated_after")
    else:
      # Sync from 2022-01-01 if no last_updated_after given
      o_start_time = datetime.datetime(2022, 1, 1, 0, 0, 0, tzinfo=pytz.UTC).isoformat('T')

    current_time = datetime.datetime.now(pytz.UTC).isoformat('T')

    # Use current time or 7 days from start date as last_updated_time for next trigger
    o_end_time = min([
        datetime.datetime.fromisoformat(o_start_time) + datetime.timedelta(days=7),
        datetime.datetime.fromisoformat(current_time)
    ]).isoformat('T')
    insert_lst = []

    if name == 'order':
      payload_include = 'orders'
    elif name == 'order_item':
      payload_include = 'order_items'
    elif name == 'order_line':
      payload_include = 'order_items.order_lines'
    else:
      raise KeyError('Endpoint not defined.')
      
    include_flag = False if name=='order' else True

    # Create payload and request params for first request
    url = f"https://api.merchants.zalando.com/merchants/{MERCHANT_ID}/orders"
    headers = {"Accept": "application/vnd.api+json", "Authorization": f"Bearer {api_key}"}
    payload = {'last_updated_after': o_start_time, 
                'last_updated_before': o_end_time, 
                'include': payload_include,
                'page[size]': 250}

    # Request zDirect Orders API
    rows, next_url = request_orders(url=url, payload=payload, headers=headers, included=include_flag)
    insert_lst.extend(rows)
    while next_url:
      rows, next_url = request_orders(url=next_url, payload={}, headers=headers, included=include_flag)
      insert_lst.extend(rows)

    return_hasmore = o_end_time < current_time

    # Create new state for next run
    return_state = {"orders_last_updated_after": o_end_time,
                    "last_api_key": api_key, 
                    "key_expiry": expiry_time}

    print(f"Return the following to Weld: {o_end_time=}, insert_len={len(insert_lst)}, {return_hasmore=}")
  
    return json.dumps({
        "insert": insert_lst,
        "state": return_state,
        "hasMore": return_hasmore
    })