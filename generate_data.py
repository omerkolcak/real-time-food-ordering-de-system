import requests
import google.generativeai as genai
import json
import math
import time
from datetime import datetime, timedelta
import random
from utils import extract_json_dict_from_response, transform_price, fetch_records_from_database, insert_orders, insert_order_items

def generate_user_data(user_id=0, address_id=0):
    """
    This function randomly generates user data using Random User api.
    Returns the user data and corresponding address data
    """
    URL = "https://randomuser.me/api/?nat=us"
    response = requests.get(URL).json()['results'][0]

    user_data = {
        "user_id": user_id,
        "address_id": address_id,
        "user_fullname": f"{response['name']['first']} {response['name']['last']}",
        "date_of_birth": response['dob']['date'],
        "gender": response['gender'],
        "nationality": response['nat'],
        "registration_number": response['login']['username'],
        "email": response['email'],
        "phone_number": response['phone'],
        "cell_number": response['cell'],
        "picture": response['picture']['large']
    }

    address_info = f"{response['location']['street']['number']} {response['location']['street']['name']}, {response['location']['city']}\
{response['location']['state']}, {response['location']['postcode']}, {response['location']['country']}"
    
    address_data = {
        "address_id": address_id,
        "address_info": address_info,
        "longitude": None,
        "latitude": None
    }

    return user_data, address_data

def generate_restaurant_names(number_of_restaurants=500,google_api_key=None):
    """
    This function randomly generates restaurant names.
    """

    # configure genai and define gemini model
    genai.configure(api_key=google_api_key)
    model = genai.GenerativeModel("gemini-pro")

    restaurant_names = []

    n_times = math.ceil(number_of_restaurants / 100)

    prompt = "can you provide me only 100 different restaurant names do not include any other information"
    for n in range(n_times):
        response = model.generate_content(prompt).text

        restaurant_names.extend([name.split(".")[1].strip() for name in response.split("\n")])

    # remove duplicates
    restaurant_names = list(set(restaurant_names))
    print(f"{len(restaurant_names)} different restaurant names are generated.")

    with open('data/restaurant_names.json', 'w') as f:
        json.dump({'restaurant_names':restaurant_names}, f)

    return restaurant_names


def generate_restaurant_data(restaurant_name,restaurant_id=0,address_id=0,google_api_key=None):
    """
    This function randomly generates restaurant data using Google Gemini. 
    """
    assert google_api_key is not None

    genai.configure(api_key=google_api_key)

    model = genai.GenerativeModel("gemini-pro")
    
    text_prompt = f"can you generate a restaurant named {restaurant_name} located in LA as a json dictionary containing the keys as restaurant_name,address, \
longitude, latitude, phone, website, foods and price, drinks and price, desserts and price have more than 10 foods, more than 10 drinks \
and more than 5 desserts, and do not generate google maps, please use only integers and floats for prices."

    response_processed = False
    
    while not response_processed:
        try:
            response = model.generate_content(text_prompt)
            response = response.text
            # extract the json dictionary from gemini response
            response = extract_json_dict_from_response(response)

            restaurant_dict = json.loads(response)
            response_processed = True
        except:
            print("Error occured while processing response, try again.")
            time.sleep(3)

    address_data = {
        "address_id": address_id,
        "address_info": restaurant_dict['address'],
        "longitude": restaurant_dict['longitude'],
        "latitude": restaurant_dict['latitude']
    }
    foods_data = transform_price(restaurant_dict['foods'])
    beverages_data = transform_price(restaurant_dict['drinks'])
    desserts_data = transform_price(restaurant_dict['desserts'])
    
    del restaurant_dict['address'], restaurant_dict['longitude'], restaurant_dict['latitude'], restaurant_dict['foods'], \
        restaurant_dict['drinks'], restaurant_dict['desserts']

    restaurant_dict['restaurant_id'] = restaurant_id

    return restaurant_dict,address_data,foods_data,beverages_data,desserts_data

def generate_random_timestamp(date=None,lower_limit=0):
    """
    Function generate random timestamp.
    """
    random_second = random.randint(lower_limit,24*60*60)
    return date + timedelta(seconds=random_second)

def generate_order(cur,user=None,restaurant=None,ORDER_ID=0,order_datetime=None):
    """
    This function generates a random order.
    """
    # fetch restaurant specific items
    restaurant_id = restaurant['restaurant_id']
    where_clause = f"WHERE fk_restaurant = {restaurant_id}"
    
    menu_items = fetch_records_from_database(cur, table_name="menu_items",filter=where_clause)

    # randomly generate order item count
    number_of_items = random.randint(1,10)
    # item_id quantity mapping
    item_dict = {}
    total_cost = 0
    for _ in range(number_of_items):
        item = random.choice(menu_items)
        if item['item_id'] in item_dict:
            item_dict[item['item_id']] += 1
        else:
            item_dict[item['item_id']] = 1

        total_cost += item['price']

    # set order date as now
    order_datetime = generate_random_timestamp(order_datetime)
    order_datetime = str(order_datetime)
    # choose randomly customer rating
    customer_rating = random.choice([1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5])

    order = {
        'order_id': ORDER_ID, 'fk_restaurant': restaurant_id, 'fk_user': user['user_id'], 'order_datetime': order_datetime,
        'total_cost': round(total_cost,2), 'cust_restaurant_rating': customer_rating
    }
    
    # insert order items into database
    order_items = []
    for item in item_dict:
        order_item = {'fk_menu_item': item, 'fk_order': ORDER_ID, 'item_quantity': item_dict[item]}
        order_items.append(order_item)

    return order, order_items



