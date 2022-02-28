import boto3
import pandas as pd
from db import *

def main_load(filename, bucket):
    
    tables = ['locations', 'payment_method', 'products', 'transactions', 'order_products']
    col_names = ['branchname','paymentmethod','prodname','','']
    modes = ['join','join','join','add','add']
    
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    
    for i in range(len(tables)):
        
        table = tables[i]
        col_name = col_names[i]
        mode = modes[i]
        

        s3_to_sql(table, col_name, filename, bucket, mode)
    
def create_dataframe(filepath):
    df = pd.read_csv(filepath)
    return df
