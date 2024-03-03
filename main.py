import psycopg2
import os
from dotenv import load_dotenv
from generate_data import generate_user_data, generate_restaurant_names, generate_restaurant_data
from utils import create_database_tables, insert_address, insert_users, insert_restaurants, insert_menu_items
import time

NUMBER_OF_USERS, NUMBER_OF_RESTAURANTS = 10000, 500
ADDRESS_ID = 0

if __name__ == "__main__":
    # connect to the postgres database
    conn = psycopg2.connect("host=localhost dbname=food-orders-db user=postgres password=postgres")
    cur = conn.cursor()
    # create the tables
    create_database_tables(conn,cur)
    # generate user data and insert it into database
    for user_id in range(344,NUMBER_OF_USERS):
        user_data, address_data = generate_user_data(user_id,ADDRESS_ID)
        conn = insert_address(conn,cur,address_data)
        conn = insert_users(conn,cur,user_data)
        ADDRESS_ID += 1
        print(user_id)
    
    # load google api key
    load_dotenv()
    GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

    # first generate restaurant names
    restaurant_names = generate_restaurant_names(NUMBER_OF_RESTAURANTS,GOOGLE_API_KEY)

    # generate restaurant data and insert it into database
    for i,name in enumerate(restaurant_names):
        restaurant_data,address_data,foods_data,beverages_data,desserts_data = generate_restaurant_data(name,i,ADDRESS_ID,GOOGLE_API_KEY)
        conn = insert_address(conn,cur,address_data)
        conn = insert_restaurants(conn,cur,restaurant_data,address_data['address_id'])
        conn = insert_menu_items(conn,cur,foods_data,"food",restaurant_data['restaurant_id'])
        conn = insert_menu_items(conn,cur,beverages_data,"beverage",restaurant_data['restaurant_id'])
        conn = insert_menu_items(conn,cur,desserts_data,"dessert",restaurant_data['restaurant_id'])
        
        print(restaurant_data,address_data,foods_data,beverages_data,desserts_data)
        ADDRESS_ID += 1
        
        time.sleep(1)

