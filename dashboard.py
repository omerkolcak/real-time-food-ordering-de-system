import streamlit as st
from utils import fetch_records_from_database, create_kafka_consumer, fetch_data_from_kafka
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

# @st.cache_data
def fetch_menu_items():
    """
    Function to get menu items in order to join with the dataframe that is streamed by spark. 
    """
    conn = psycopg2.connect("host=localhost dbname=food-orders-db user=postgres password=postgres")
    cur = conn.cursor()

    menu_items = fetch_records_from_database(cur,table_name="menu_items")

    return pd.DataFrame(menu_items)

def update_data():
    """
    Function to update the data in a given interval using kafka consumer.
    """
    menu_items = fetch_menu_items()
    # show the restaurant revenues and ratings 
    restaurant_consumer = create_kafka_consumer("restaurant_stats")
    data = fetch_data_from_kafka(restaurant_consumer)

    restaurant_stats = pd.DataFrame(data)
    restaurant_stats = restaurant_stats.drop_duplicates(subset=['restaurant_name'], keep='last')

    restaurant_stats = restaurant_stats.sort_values('total_revenue',ascending=False)

    # show the most popular foods ordered by users
    menu_item_consumer = create_kafka_consumer("menu_item_stats")
    data = fetch_data_from_kafka(menu_item_consumer)

    menu_item_stats = pd.DataFrame(data)
    menu_item_stats = menu_item_stats.drop_duplicates(subset=['fk_menu_item'], keep='last')

    # restaurant_stats = pd.DataFrame([['The Flavor Retreat', 11515.0, 3.077465], 
    #                                  ['The Tastetastic Extravaganza', 35929.0, 3.11], 
    #                                  ['The Palate Sanctuary', 16949.0, 3.04]], columns=['restaurant_name','total_revenue','avg_rate'])

    # restaurant_stats = restaurant_stats.sort_values('total_revenue',ascending=False)

    # menu_item_stats = pd.DataFrame([[1,33], [2,25], [3,11]],columns=['fk_menu_item','item_quantity'])

    # menu_item_stats = menu_item_stats.merge(menu_items,left_on='fk_menu_item', right_on='item_id')[['item_name','item_quantity','price']]
        
    return restaurant_stats, menu_item_stats

def plot_restaurant_revenues(restaurant_stats):
    """
    Function to plot restaurant revenues and ratings as bar chart.
    """
    # Create a figure and axis object
    fig, ax = plt.subplots(figsize=(10, 6))

    # Set the bar positions
    bar_positions = np.arange(len(restaurant_stats))

    # Plotting Revenue
    ax.barh(bar_positions - 0.2, restaurant_stats['total_revenue'], height=0.4, color='skyblue', label='Revenue')

    # Plotting Ratings
    ax2 = ax.twiny()
    ax2.barh(bar_positions + 0.2, restaurant_stats['avg_rate'], height=0.4, color='salmon', label='Customer Rating')
    ax2.set_xlim(0,5)

    # Set labels and title
    ax.set_xlabel('Revenue')
    ax2.set_xlabel('Customer Rating')
    ax.set_title('Restaurant Revenue and Rating')

    # Add the restaurant names as labels on the y-axis
    ax.set_yticks(bar_positions)
    ax.set_yticklabels(restaurant_stats['restaurant_name'])

    bar, labels = ax.get_legend_handles_labels()
    bar2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(bar + bar2, labels + labels2, loc="upper right")

    return fig

def plot_menu_item_popularity(menu_item_stats):
    """
    This function plots top 10 ordered items.
    """
    pass

def create_dashboard():
    """
    Function to create and display dasboards
    """
    restaurant_stats, menu_item_stats = update_data()

    figure = plot_restaurant_revenues(restaurant_stats)
    st.pyplot(figure)

create_dashboard()