import psycopg2
from json import loads
import boto3

# Load environment variables from .env file

client2 = boto3.client('ssm')
response = client2.get_parameter(Name='team1_creds',WithDecryption=True)
creds = loads(response['Parameter']['Value'])

host = creds["host"]
user = creds["user"]
port = creds["port"]
password = creds["password"]
database = "team1_cafe"

def pandas_to_sql(df,table_name, col_name, mode):

    for i in range(5): # retries

        try:
            connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
            connection.autocommit = True
            cursor = connection.cursor()
        
            # Adjust ... according to number of columns
            np_data = df.to_numpy()
            a = ','.join(["%s"]*len(df.columns)) 
            col_names = ','.join(df.columns.tolist())
            args_str = b','.join(cursor.mogrify(f"({a})", x) for x in tuple(map(tuple,np_data)))
            
            if mode == 'add':
                cursor.execute(f"insert into team1_schema.{table_name} ({col_names}) VALUES "+args_str.decode("utf-8"))
            
            else:
                cursor.execute(f"CREATE TEMP TABLE {table_name}_temp AS SELECT * FROM team1_schema.{table_name} LIMIT 0")
                cursor.execute(f"INSERT INTO {table_name}_temp ({col_names}) VALUES "+args_str.decode("utf-8"))
                cursor.execute(f"INSERT INTO team1_schema.{table_name} SELECT * FROM {table_name}_temp WHERE {col_name} NOT IN (SELECT {col_name} FROM team1_schema.{table_name})")
        
            cursor.close()
            connection.close()
            break
            
        except (Exception, psycopg2.Error) as error:
            print(f"Attempt {i}: Failed to insert record into table", error)
            cursor.close()
            connection.close()

# Establish a database connection
def execute_query(statement):
    try:
        connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
        cursor = connection.cursor()
        cursor.execute(statement)
        connection.commit()
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        return rows
    
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)
        cursor.close()
        connection.close()