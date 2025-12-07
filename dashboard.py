import pandas as pd
import networkx as nx
import yaml
import re
import warnings
import matplotlib.pyplot as plt
import streamlit as st

def clean_price(price_raw):
    s = str(price_raw)
    rate = 1.2 if '€' in s or 'EUR' in s else 1.0
    s = s.replace('¢', '.')
    clean_num_str = re.sub(r'[^\d.]', '', s)
    val = float(clean_num_str)
    return round(val * rate, 2)

def clean_timestamp(ts_raw):
    s = str(ts_raw)
    s = s.replace(';', ' ').replace(',', ' ')
    return s.strip()

def clean_phone(phone):
    return re.sub(r'\D', '', str(phone))

def create_author_tuple(author_str):    
    authors_list = author_str.split(',')
    clean_authors = []
    for a in authors_list:
        clean_authors.append(a.strip())
    clean_authors.sort()
    
    return tuple(clean_authors)

def find_top_5_days_revenue(orders):
    return orders.groupby('date_only')['paid_price'].sum().sort_values(ascending=False).head(5).reset_index()

def find_most_popular_author(orders, books):
    orders_with_authors = orders.merge(
    books[['id', 'author_tuple']], 
    left_on='book_id', 
    right_on='id', 
    how='left')

    author_sold_count = orders_with_authors.groupby('author_tuple')['quantity'].sum().sort_values(ascending=False)
    authors = ", ".join(author_sold_count.head(1).idxmax())
    count = author_sold_count.max()
    return authors, count

def find_unique_users(users): #returns map
    G = nx.Graph()
    G.add_nodes_from(users['id'])

    for col in ['email', 'clean_phone', 'address']:
        groups = users.groupby(col)['id'].apply(list)
        
        for ids in groups:
            for i in range(1, len(ids)):
                G.add_edge(ids[0], ids[i])

    unique_groups = list(nx.connected_components(G))

    user_map = {}
    for component in nx.connected_components(G):
        ids = list(component)
        first_id = str(sorted(ids)[0])
        for id in ids:
            user_map[str(id)] = first_id
    
    return len(unique_groups), user_map

def find_top_spender(orders, users, user_map):
    orders['real_id'] = orders['user_id'].astype(str).map(user_map)
    users['real_id'] = users['id'].astype(str).map(user_map)

    users_total_spent = orders.groupby('real_id')['paid_price'].sum()
    top_spender_real_id = users_total_spent.idxmax()
    total_spent = users_total_spent.max()

    cols = ['id', 'name', 'email', 'clean_phone', 'address']
    profile = users.loc[users['real_id'] == top_spender_real_id, cols].reset_index()
    return profile, total_spent

def load_and_fix_data(folder_name):
    orders = pd.read_parquet(folder_name + '/orders.parquet')
    users = pd.read_csv(folder_name + '/users.csv')
    with open(folder_name + '/books.yaml', 'r') as f:
        books = yaml.safe_load(f)
    books = pd.json_normalize(books)
    newcols = []
    for i in range(len(books.columns)):
        newcols.append(books.columns[i].replace(':', ''))
    books.columns = newcols

    orders['clean_unit_price'] = orders['unit_price'].apply(clean_price)
    orders['paid_price'] = orders['quantity'] * orders['clean_unit_price']

    orders['clean_timestamp'] = orders['timestamp'].apply(clean_timestamp)
    warnings.filterwarnings('ignore', category=FutureWarning)
    orders['date_parsed'] = pd.to_datetime(orders['clean_timestamp'], format='mixed', dayfirst=True)
    orders['date_only'] = orders['date_parsed'].dt.date

    users['clean_phone'] = users['phone'].apply(clean_phone)

    books['author_tuple'] = books['author'].apply(create_author_tuple)

    return orders, users, books


def plot_daily_revenue(orders):
    daily_revenue = orders.groupby('date_only')['paid_price'].sum().sort_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(
        daily_revenue.index,
        daily_revenue.values,
        marker='',
        linestyle='-',
        color='green',
        linewidth=2 
    )
    ax.set_title('Daily Revenue', fontsize=16)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Revenue', fontsize=12)
    ax.grid(True, linestyle='--', alpha=0.6)

    
    st.pyplot(fig)


st.set_page_config(layout="wide", page_title="Dashboard")

tab1, tab2, tab3 = st.tabs(["DATA1", "DATA2", "DATA3"])

def render_tab_content(folder_name):
    orders, users, books = load_and_fix_data(folder_name)

    nunique_users, user_map = find_unique_users(users)
    top_5_days = find_top_5_days_revenue(orders)
    most_pop_author, author_sales_count = find_most_popular_author(orders, books)
    top_spender_profile, total_spent = find_top_spender(orders, users, user_map)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Number of unique users", nunique_users)
    col2.metric("Number of unique sets of authors", books['author_tuple'].nunique())
    #col3.metric("Most popular author", most_pop_author, f"{author_sales_count} books sold")
    col3.metric("Best buyer spent", f"${total_spent:,.2f}")
    
    st.divider()
    
    st.metric("Most popular author", most_pop_author)
    st.write(f"{author_sales_count} books sold")

    st.divider()

    col_left, col_right = st.columns([1,2])
    
    with col_left:
        st.subheader("Top 5 days by revenue")
        st.dataframe(top_5_days, use_container_width=True, hide_index=True)

    with col_right:
        st.subheader("Daily Revenue chart")
        plot_daily_revenue(orders)

    st.divider()

    st.subheader("Best buyer aliases")
    st.dataframe(top_spender_profile, use_container_width=True, hide_index=True)

with tab1:
    render_tab_content("DATA1")
with tab2:
    render_tab_content("DATA2")
with tab3:
    render_tab_content("DATA3")