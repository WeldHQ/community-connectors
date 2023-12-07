import requests
import json
import time
import os
import functions_framework
from datetime import datetime, timedelta

AUTH_CREDS = ""
AUTH_HEADER = {"Authorization": f"Basic {AUTH_CREDS}"}
AUTH_URL = "https://login.bol.com/token?grant_type=client_credentials"
base_url = "https://api.bol.com/retailer/"


def getSchema():
    """
    Returns Schema for Weld Integration
    """
    return {
        "schema": {
            "order_items": {"primary_key": "order_id"},
            "returns": {"primary_key": "return_id"},
            "inventory": {"primary_key": "inv_id"}
        }
    }


def request_bearer_token(auth_url, auth_header):
    """
        Function for requesting new Bearer token from Bol API.
        Returns access token, as well as expiry datetime for that token in order to minimize the number of requests to the Auth API
    """
    auth_req = requests.post(url=auth_url, headers=auth_header)
    response = auth_req.json()
    expiry_dt = datetime.now() + timedelta(seconds=response['expires_in'])
    bearer_token = response['access_token']
    print(f"Requested new bearer token with expiry timestamp: {expiry_dt}.")
    return bearer_token, expiry_dt


def avoid_rate_limit(headers):
    """
      Function for checking if rate limit will be reached given a responses header.
      If limit will be reached, pauses the script until it is reset.
    """
    rate_limit = int(headers['x-ratelimit-remaining'])
    rate_reset = int(headers['x-ratelimit-reset'])
    # Wait for rate to reset when only one request left
    if rate_limit < 2:
        print(f"Sleeping {rate_reset+1} seconds to avoid reaching rate limit.")
        time.sleep(rate_reset+1)


def request_order_items(last_updated_date, bearer_token, expiry_dt):
    """
    Function for requesting order items updated after a specified date.
    Includes Pagination and credential handling. 
    """
    # Set initial parameters/headers
    order_url = f"{base_url}orders"
    pcounter = 1
    data_returned = True

    api_header = {"Authorization": f"Bearer {bearer_token}",
                  "Accept": "application/vnd.retailer.v9+json"}

    order_params = {'fulfilment-method': 'ALL',
                    'status': 'ALL',
                    'latest-change-date': last_updated_date,
                    'page': pcounter}

    order_items = []
    while data_returned:
        # Check if Bearer token has expired, request new one if it has
        if expiry_dt <= datetime.now() + timedelta(seconds=30):
            print(
                f"Requesting new bearer token as old one expires at {expiry_dt}.")
            bearer_token, expiry_dt = request_bearer_token(
                AUTH_URL, AUTH_HEADER)
            api_header["Authorization"] = f"Bearer {bearer_token}"

        # Request orders
        order_params['page'] = pcounter
        print(f"Requesting orders for page {pcounter}.")
        o_response = requests.get(
            url=order_url, params=order_params, headers=api_header)

        # Check if data in response
        # Set try_next_page to False if not
        o_json = o_response.json()
        data_returned = False if len(o_json) == 0 else True

        if data_returned and 'orders' in o_json:
            # Increase page counter by 1
            pcounter += 1

            # Get orders and create request for each orderId
            o = o_json['orders']
            o_ids = [oi['orderId'] for oi in o]
            print(f"Requesting data for {len(o_ids)} order items.")

            # Check rate limit and pause if needed
            avoid_rate_limit(o_response.headers)

            for oi in o_ids:
                # Construct order item url and request data
                orderitem_url = f'{order_url}/{oi}'
                oi_response = requests.get(
                    url=orderitem_url, headers=api_header)
                oi_json = oi_response.json()

                # Append to return list
                order_items.append(oi_json)

                # Check rate limit and pause if needed
                avoid_rate_limit(oi_response.headers)
        else:
            print(f"No data on page {pcounter}. Stopping.")
            data_returned = False
            avoid_rate_limit(o_response.headers)

    if order_items:
        for oi in order_items:
            oi['order_id'] = oi.pop('orderId')
            
    return order_items


def request_returns(bearer_token, expiry_dt):
    returns_url = f"{base_url}returns"
    pcounter = 1
    data_returned = True
    api_header = {"Authorization": f"Bearer {bearer_token}",
                  "Accept": "application/vnd.retailer.v9+json"}

    returns_params = {'page': pcounter,
                      'handled': True,
                      'fulfilment-method': 'FBB'}

    returns = []
    while data_returned:
        if expiry_dt <= datetime.now() + timedelta(seconds=30):
            print(
                f"Requesting new bearer token as old one expires at {expiry_dt}.")
            bearer_token, expiry_dt = request_bearer_token(
                AUTH_URL, AUTH_HEADER)
            api_header["Authorization"] = f"Bearer {bearer_token}"

        # Request returns
        returns_params['page'] = pcounter
        print(f"Requesting returns for page {pcounter}.")
        r_response = requests.get(
            url=returns_url, params=returns_params, headers=api_header)
        r_json = r_response.json()
        data_returned = False if len(r_json) == 0 else True

        if data_returned and 'returns' in r_json:
            r = r_json['returns']
            returns.extend(r)
            pcounter += 1
        avoid_rate_limit(r_response.headers)
    else:
        print(f"No data on page {pcounter}. Stopping.")
        data_returned = False
        avoid_rate_limit(r_response.headers)

    # Rename the returnId to snake case to avoid issues with Weld connector
    if returns:
        for re in returns:
            re['return_id'] = re.pop('returnId')

    return returns


def request_inventory(bearer_token, expiry_dt):
    """
      Function to request current inventory snapshot for all eans.
      Adds unique ean_date id to allow patching of inventory values if run multiple times a day.
    """
    inventory_url = f"{base_url}inventory"

    pcounter = 1
    data_returned = True
    api_header = {"Authorization": f"Bearer {bearer_token}",
                  "Accept": "application/vnd.retailer.v9+json"}

    inventory_params = {'page': pcounter}

    inventory = []
    while data_returned:
        if expiry_dt <= datetime.now() + timedelta(seconds=30):
            print(
                f"Requesting new bearer token as old one expires at {expiry_dt}.")
            bearer_token, expiry_dt = request_bearer_token(
                AUTH_URL, AUTH_HEADER)
            api_header["Authorization"] = f"Bearer {bearer_token}"

        # Request inventory
        inventory_params['page'] = pcounter
        print(f"Requesting inventory for page {pcounter}.")
        i_response = requests.get(
            url=inventory_url, params=inventory_params, headers=api_header)
        i_json = i_response.json()
        data_returned = False if len(i_json) == 0 else True

        if data_returned and 'inventory' in i_json:
            i = i_json['inventory']
            inventory.extend(i)
            pcounter += 1
        avoid_rate_limit(i_response.headers)
    else:
        print(f"No data on page {pcounter}. Stopping.")
        data_returned = False
        avoid_rate_limit(i_response.headers)

    # Add snapshot date and primary key snapshotDate_EAN to each record if return list is not empty
    if len(inventory) > 0:
        today = datetime.now().strftime('%Y-%m-%d')
        for i in inventory:
            i['snapshot_date'] = today
            i['inv_id'] = f"{today}_{i['ean']}"
    return inventory


@functions_framework.http
def handler(req):
    # make sure request is coming from Weld
    if req.headers["authorization"].split(" ")[1] != os.environ.get('WELD_API_KEY'):
        return {"message": "Not authorized"}, 403

    # if path is schema, return the schema to Weld
    path = req.full_path[1:-1]
    if path == "schema":
        return getSchema()

    # Get data for correct granularity/endpoint
    request_json = req.get_json()  # silent=True
    name = request_json["name"]

    # print(f"{request_json=}")
    print(f"{request_json=}")

    # Check if last Bearer Token is already expired (or is within 5 mins of expiring)
    if "key_expiry" in request_json["state"] and "last_api_key" in request_json["state"]:
        # check if last api key has already expired and request new one if so
        expiry_threshold = datetime.now() + timedelta(minutes=5)
        expiry_dt = datetime.fromisoformat(
            request_json["state"]["key_expiry"])
        
        if expiry_threshold > expiry_dt:
            print("Requesting new API Token as last one expired.")
            bearer_token, expiry_dt = request_bearer_token(AUTH_URL, AUTH_HEADER)
        else:
            print("Reusing last API Token.")
            bearer_token = request_json["state"]["last_api_key"]
    else:
        print("Requesting new API Token.")
        bearer_token, expiry_dt = request_bearer_token(AUTH_URL, AUTH_HEADER)

    # Process previous state
    if "last_updated_date" in request_json["state"]:
        # Define order request parameters, using incremental sync cursor where applicable
        start_time = request_json["state"]["last_updated_date"]
        start_date = datetime.strptime(start_time, "%Y-%m-%d").date()
    else:
        # Sync from three months ago (max for orders) if no last_updated_date given
        start_time = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        start_date = datetime.strptime(start_time, "%Y-%m-%d").date()

    if name == 'order_items':
        # Request order items from Bol Orders API
        print(f"Requesting order_items that have been updated after {start_time}.")
        insert_lst = request_order_items(start_time, bearer_token, expiry_dt)
        print("Requested all order_items.")
        # Set last_updated_date for the next run
        # Take the minimum of either the last run's date incremented by one day, or current date for the next run
        next_day = start_date+timedelta(days=1)
        current_day = datetime.today().date()
        end_date = min(next_day, datetime.now().date())
        end_time = end_date.strftime("%Y-%m-%d")
        # Should send another request if last_updated_date was not today.
        return_hasmore = True if end_date < current_day else False

    elif name == "returns":
        # Set current date as date_state to avoid syncs twice a day
        end_date = datetime.today().date()
        end_time = end_date.strftime("%Y-%m-%d")
        return_hasmore = False
        if end_date > start_date:
          # Request most recent inventory snapshot for all items
          insert_lst = request_returns(bearer_token, expiry_dt)
          print("Requested all returns.")
        else: 
          # Return empty list if inventory has already been synced that day  
          print("Returns data has been requested today already.")
          insert_lst = []

    elif name == "inventory":
        # Set current date as date_state to avoid syncs twice a day
        end_date = datetime.today().date()
        end_time = end_date.strftime("%Y-%m-%d")
        return_hasmore = False
        if end_date > start_date:  
          # Request most recent inventory snapshot for all items
          insert_lst = request_inventory(bearer_token, expiry_dt)
          print("Requested inventory data.")
        else: 
          # Return empty list if inventory has already been synced that day  
          print("Inventory data has been requested today already.")
          insert_lst = []

    else:
        insert_lst = []

    # Create new state for next run
    return_state = {"last_updated_date": end_time,
                    "last_api_key": bearer_token,
                    "key_expiry": expiry_dt.isoformat('T')}

    print(
        f"Return the following to Weld: {end_time=}, insert_len={len(insert_lst)}, {return_hasmore=}")

    return json.dumps({
        "insert": insert_lst,
        "state": return_state,
        "hasMore": return_hasmore
    })
