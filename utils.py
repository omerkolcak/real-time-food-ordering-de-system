import re
from kafka import KafkaConsumer
import json

MENU_ITEM_ID = 9963

def create_database_tables(conn,cur):
    """
    This function creates postgres db tables.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS address (
            address_id INTEGER PRIMARY KEY,
            address_info VARCHAR(255),
            longitude FLOAT,
            latitude FLOAT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            fk_address INTEGER,
            user_fullname VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS restaurants (
            restaurant_id INTEGER PRIMARY KEY,
            fk_address INTEGER,
            restaurant_name VARCHAR(255),
            phone_number VARCHAR(255),
            website VARCHAR(255)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS menu_items (
            item_id INTEGER PRIMARY KEY,
            fk_restaurant INTEGER,
            item_name VARCHAR(255),
            item_category VARCHAR(255),
            price FLOAT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            fk_restaurant INTEGER,
            fk_user INTEGER,
            order_datetime TIMESTAMP,
            total_cost FLOAT,
            cust_restaurant_rating FLOAT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_item (
            order_item_id INTEGER PRIMARY KEY,
            fk_menu_item INTEGER,
            fk_order INTEGER,
            item_quantity INTEGER
        )
    """)
    conn.commit()

def insert_address(conn,cur,address):
    """
    Function to insert address data into database.
    """
    cur.execute("""
        INSERT INTO address (address_id, address_info, longitude, latitude)
        VALUES (%s, %s, %s, %s)
    """,
    (address['address_id'], address['address_info'], address['longitude'], address['latitude'])
    )
    conn.commit()

    return conn

def insert_users(conn,cur,user_data):
    """
    Function to insert address data into database.
    """
    cur.execute("""
        INSERT INTO users (user_id,fk_address,user_fullname,date_of_birth,gender,nationality,registration_number,email,phone_number,cell_number,picture )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (user_data['user_id'],user_data['address_id'],user_data['user_fullname'],user_data['date_of_birth'],user_data['gender'],user_data['nationality'],user_data['registration_number'],user_data['email'],user_data['phone_number'],user_data['cell_number'],user_data['picture'])
    )
    conn.commit()

    return conn

def insert_restaurants(conn,cur,restaurant_data,address_id):
    """
    Function to insert restaurant data into database.
    """
    cur.execute("""
        INSERT INTO restaurants (restaurant_id, fk_address, restaurant_name, phone_number, website)
        VALUES (%s, %s, %s, %s, %s)
    """,
    (restaurant_data['restaurant_id'], address_id, restaurant_data['restaurant_name'], restaurant_data['phone'], restaurant_data['website'])
    )
    conn.commit()

    return conn

def insert_menu_items(conn,cur,items,category,restaurant_id):
    """
    Function to insert menu items into database.
    """
    for item in items:
        global MENU_ITEM_ID
        cur.execute("""
            INSERT INTO menu_items (item_id, fk_restaurant, item_name, item_category, price)
            VALUES (%s, %s, %s, %s, %s)
        """,
        (MENU_ITEM_ID, restaurant_id, item['name'], category, item['price'])
        )
        MENU_ITEM_ID += 1
    conn.commit()

    return conn

def insert_orders(conn,cur,order):
    """
    Function to insert order into database.
    """
    cur.execute("""
        INSERT INTO orders (order_id, fk_restaurant, fk_user, order_datetime, total_cost, cust_restaurant_rating)
        VALUES (%s, %s, %s, %s, %s, %s)
    """,
    (order['order_id'], order['fk_restaurant'], order['fk_user'], order['order_datetime'], order['total_cost'], order['cust_restaurant_rating'])
    )
    conn.commit()

    return conn

def insert_order_items(conn,cur,order_item):
    """
    Function to insert order items into database.
    """
    cur.execute("""
        INSERT INTO order_item (order_item_id, fk_menu_item, fk_order, item_quantity)
        VALUES (%s, %s, %s, %s) 
    """,
    (order_item['order_item_id'],order_item['fk_menu_item'],order_item['fk_order'],order_item['item_quantity'])
    )
    conn.commit()

    return conn

def fetch_records_from_database(cur,table_name="restaurants",filter=""):
    """
    This function fetches all the data from given table
    """
    query = f"\
        SELECT row_to_json(t)\
        FROM (\
            SELECT * FROM {table_name} {filter}\
        ) t;\
    "

    records = cur.execute(query)
    records = cur.fetchall()
    records = [record[0] for record in records]

    if len(records) == 0:
        raise Exception(f"{table_name} is empty!!!")
    
    return records


def transform_price(items):
    """
    Sometimes Gemini gives price values as not integers or floats, transform them.
    """
    ## edge cases --> $ signs,FREE, 12$-20$ 
    if type(items[0]['price']) != str:
        return items
    else:
        for item in items:
            try:
                price = item['price'].replace("$","")

                if price.lower() == "free": 
                    price = 0
                elif "-" in price:
                    price_range = price.split("-")
                    price = (float(price_range[0]) + float(price_range[1])) / 2

                item['price'] = float(price)
            except:
                break

    return items

def extract_json_dict_from_response(text):
    """
    This function extracts the json dict from Gemini response
    """
    # Find the index of the first occurrence of '{'
    start_index = text.find('{')
    if start_index == -1:
        return None  # Return None if '{' is not found
    
    # Find the index of the last occurrence of '}'
    end_index = text.rfind('}')
    if end_index == -1:
        return None  # Return None if '}' is not found
    
    # Extract the substring between the first '{' and the last '}'
    substring = text[start_index: end_index + 1]
    
    # and finally clear the spaces from response
    substring = re.sub(' +', ' ', substring)
    
    return substring

# Function to create a Kafka consumer
def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

# Function to fetch data from Kafka
def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        print(message)
        for sub_message in message:
            print(sub_message.value)
            data.append(sub_message.value)
    return data