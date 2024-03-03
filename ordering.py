import psycopg2
from utils import fetch_records_from_database, insert_orders, insert_order_items
from generate_data import generate_order
import random
import json
import time
import sys
from datetime import datetime, timedelta

from confluent_kafka import SerializingProducer 

ORDER_ID, ORDER_MENU_ITEM_ID = 0,0

def create_single_order(conn,restaurants,users,date):
    """
    Function to randomly create single order.
    """
    user = random.choice(users)
    restaurant = random.choice(restaurants)
    # restaurant = restaurants[0]

    # randomly generate order for choosen user and restaurant
    global ORDER_ID
    order, order_items = generate_order(cur,user,restaurant,ORDER_ID,date)
    # insert order into database
    conn = insert_orders(conn,cur,order)
    # insert order items into database
    for item in order_items:
        global ORDER_MENU_ITEM_ID
        
        item['order_item_id'] = ORDER_MENU_ITEM_ID
        conn = insert_order_items(conn,cur,item)

        ORDER_MENU_ITEM_ID += 1
    
    ORDER_ID += 1

    order['restaurant_name'] = restaurant['restaurant_name']

    return conn, order, order_items

def simulate_food_ordering_system(conn,cur,start_date=None,end_date=None):
    """
    Function to simulate food ordering system in a given time period.
    """
    assert end_date > start_date

    # fetch all the restaurant and users from the database.
    restaurants = fetch_records_from_database(cur,'restaurants')
    users = fetch_records_from_database(cur,'users')

    current_date = start_date
    # define kafka producer
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

    while current_date <= end_date:
        # first choose randomly how many orders in that day
        n_orders = random.randint(1000,5000)
        for _ in range(n_orders):
            conn, order, order_items = create_single_order(conn,restaurants,users,current_date)
            #print(order)

            # produce orders
            producer.produce('orders_topic', key=str(order['order_id']), value=json.dumps(order))
            producer.flush() 
            # produce order items
            for item in order_items:
                producer.produce('order_items_topic', key=str(item['order_item_id']), value=json.dumps(item))
                producer.flush()

            time.sleep(1)

        # update current date.
        current_date += timedelta(days=1)


if __name__ == "__main__":
    # connect to the postgres database
    conn = psycopg2.connect("host=localhost dbname=food-orders-db user=postgres password=postgres")
    cur = conn.cursor()

    start_date, end_date = datetime.strptime(sys.argv[1],'%m/%d/%Y'), datetime.strptime(sys.argv[2], '%m/%d/%Y')
    simulate_food_ordering_system(conn,cur,start_date, end_date)
    