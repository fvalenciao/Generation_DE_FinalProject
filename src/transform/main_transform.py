from transform_funcs import *
import datetime

def main_transform(filename):
# CREATE ORDERS DATAFRAME
    columns = ['time_stamp', 'branchname', 'Customer Name', 'Order', 'sum_total', 'paymentmethod', 'Card Number']

    orders_df = create_dataframe(f"/tmp/{filename}",columns)

    # CREATE ORDER IDS AND SET THEM AS THE INDEX OF THE ORDERS DATAFRAME
    orders_df = create_order_ids(orders_df)
    orders_df['time_stamp'] = orders_df['time_stamp'].apply(lambda x: datetime.datetime.strptime(x,'%d/%m/%Y %H:%M'))

    # DROP SENSITIVE INFO
    orders_df.drop(['Customer Name', 'Card Number'], axis=1, inplace=True)

    # CREATE AND POPULATE BRANCH ID COLUMN
    orders_df = create_ids(orders_df, 'branchname','branchid', 'locations', filename)

    # CREATE AND POPULATE PAYMENT METHODS COLUMN
    orders_df = create_ids(orders_df, 'paymentmethod', 'paymentid','payment_method', filename)

    # CREATE AND POPULATE ORDER PRODUCTS DATAFRAME, UPDATE PRODUCTS TABLE
    order_prods_df = create_order_prods(orders_df, filename)

    # REORGANISE COLUMNS
    orders_df = orders_df[['orderid', 'time_stamp', 'branchid', 'paymentid', 'sum_total']]
    order_prods_df = order_prods_df[['orderid', 'prodid', 'quantity', 'price']]
    
    # EXPORT TRANSFORMED DATA TO S3 BUCKET
    pandas_to_s3(orders_df, 'transactions', filename)
    pandas_to_s3(order_prods_df, 'order_products', filename)


