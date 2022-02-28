import pandas as pd
import numpy as np
import hashlib
from db import *

def create_dataframe(filepath,columns):
    df = pd.read_csv(filepath, header=None)
    df.columns = columns
    return df

def create_order_ids(df):
    order_ids = df.apply(lambda x: abs(int(hashlib.md5((x["time_stamp"] + x["Customer Name"]).encode("utf-8")).hexdigest(), 16) % (10 ** 12)), axis=1)
    df['orderid'] = order_ids
    
    return df

def create_ids(df,col_name_df,id_name,table_name_sql):
    
    data_dicts = [] # each element in the list is a dictionary with the name and id of the branch or payment method
    names_col = df[col_name_df].tolist()
    ids_col = df.apply(lambda x: abs(int(hashlib.md5((x[col_name_df]).encode("utf-8")).hexdigest(), 16) % (10 ** 12)), axis=1).tolist()
    
    df[id_name] = ids_col
    df.drop([col_name_df], axis=1, inplace=True)
    
    for i in range(len(names_col)):
        
        data_dict = {id_name: ids_col[i], col_name_df: names_col[i]}
        
        if data_dict not in data_dicts:
            data_dicts.append(data_dict)
    
    add_to_sql(table_name_sql, col_name_df, data_dicts) # check if there are any entries that are not in the database and add them
    
    return df

def create_order_prods(orders_df):
    
    order_prods_df = orders_df[['orderid', 'Order']]
    order_prods_df['Order'] = order_prods_df['Order'].apply(lambda x: x.split(', '))

    order_prods_df = order_prods_df.explode('Order')
    order_prods_df['quantity'] = np.ones(len(order_prods_df['orderid'])).tolist()

    order_prods_df = order_prods_df.groupby(['orderid', 'Order']).count()['quantity'].reset_index()
    dummy_df = order_prods_df['Order'].str.rpartition(' - ') # split column in last occurrence of separator

    order_prods_df['Order'] = dummy_df[0]
    order_prods_df['price'] = dummy_df[2]

    order_prods_ids = order_prods_df.apply(lambda x: abs(int(hashlib.md5((x['Order']).encode("utf-8")).hexdigest(), 16) % (10 ** 12)), axis=1)
    
    order_prods_df['prodid'] = order_prods_ids
    
    data_dicts = []
    prod_names = order_prods_df['Order'].tolist()
    prices = order_prods_df['price'].tolist()
    
    order_prods_df.drop(['Order'], axis=1, inplace=True)
    
    for i in range(len(order_prods_ids)): 
        
        data_dict = {'prodid': order_prods_ids[i], 'prodname': prod_names[i], 'currentprice': prices[i]}

        if data_dict not in data_dicts:
            data_dicts.append(data_dict)
    
    add_to_sql('products', 'prodname', data_dicts)
    
    return order_prods_df

def add_to_sql(table_name, col_name, data_dicts):
    # create a temp table from the dictionaries in data_dicts and join to permanent SQL table without duplicates
    df = pd.DataFrame(data_dicts)
    pandas_to_sql(df, table_name, col_name, 'join')