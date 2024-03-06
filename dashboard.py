import streamlit as st
from utils import fetch_records_from_database, create_kafka_consumer, fetch_data_from_kafka
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

@st.cache_data
def fetch_menu_items():
    """
    Function to get menu items in order to join with the dataframe that is streamed by spark. 
    """
    conn = psycopg2.connect("host=localhost dbname=food-orders-db user=postgres password=postgres")
    cur = conn.cursor()

    menu_items = fetch_records_from_database(cur,table_name="menu_items")

    return pd.DataFrame(menu_items)

@st.cache_data
def fetch_full_order_data():
    """
    This function fetches the orders from database along with the user and restaurant data.
    """
    conn = psycopg2.connect("host=localhost dbname=food-orders-db user=postgres password=postgres")
    cur = conn.cursor()

    query = "\
        SELECT u.user_fullname, o.order_datetime, r.restaurant_name, o.total_cost, o.cust_restaurant_rating\
        FROM orders o\
        JOIN restaurants r ON o.fk_restaurant = r.restaurant_id\
        JOIN users u ON o.fk_user = u.user_id;\
    "

    return pd.read_sql_query(query, conn)

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

    restaurant_stats = restaurant_stats.sort_values('total_revenue',ascending=False)[:10]

    # show the most popular foods ordered by users
    menu_item_consumer = create_kafka_consumer("menu_item_stats")
    data = fetch_data_from_kafka(menu_item_consumer)

    menu_item_stats = pd.DataFrame(data)
    menu_item_stats = menu_item_stats.drop_duplicates(subset=['fk_menu_item'], keep='last')

    menu_item_stats = menu_item_stats.merge(menu_items,left_on='fk_menu_item', right_on='item_id')[['item_name','item_quantity','price']]
    menu_item_stats['item_name'] = menu_item_stats['item_name'] + "\n$" + menu_item_stats['price'].astype(str)
    
    menu_item_stats = menu_item_stats.sort_values('item_quantity',ascending=False)[:10]

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
    # Create a figure and axis object
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.barh(menu_item_stats['item_name'],menu_item_stats['item_quantity'], height=0.4, color='salmon', label='Item Quanity')

    ax.set_xlabel('Item Quantity')
    ax.set_title('Top 10 Most Ordered Items')

    return fig

@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    """
    Function to split a dataframe into chunks for pagination
    """
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df

def paginate_table(table_data):
    """
    Function to create paginated table.
    """
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction", options=["⬆️", "⬇️"], horizontal=True
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        )
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)
    
def create_dashboard():
    """
    Function to create and display dasboards
    """
    restaurant_stats, menu_item_stats = update_data()

    st.header('Food Ordering Dashboard', divider='rainbow')
    # plot restaurant revenues
    figure = plot_restaurant_revenues(restaurant_stats)
    st.pyplot(figure)
    
    # plot most popular items
    figure = plot_menu_item_popularity(menu_item_stats)
    st.pyplot(figure)

    # show orders as a table
    orders_data = fetch_full_order_data()
    st.header("Orders List")
    paginate_table(orders_data)

create_dashboard()