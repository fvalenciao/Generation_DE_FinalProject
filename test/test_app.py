import pandas.testing # <-- for testing dataframes
import unittest.mock as mock
import pandas as pd
import os
import boto3
from moto import mock_s3

#& Tests file download like our lambda
def download_file(filepath,bucket,key):
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, key, filepath)

@mock_s3()
def test_download_file():
    s3=boto3.client("s3") #fake s3 client
    s3.create_bucket(Bucket="testbucket") #fake s3 bucket
    s3.put_object(Bucket="testbucket",Key="testfile",Body=b"foo-bar") # fake s3 bucket with fake stuff inside
    download_file("testing","testbucket","testfile") #insert our args from the func "download_file(filepath,bucket,key)
    assert os.path.isfile("testing")#< the fake file called test
# so all above commands are "fake", an asserting is "real"


#real func df pd:
# def create_dataframe(filepath,columns):
#     df = pd.read_csv(filepath, header=None)
#     df.columns = columns
#     return df
#! must have above function in different file,then import to here for test below to work
#*mock unittest func using df pd:

from functest import create_dataframe 
#change "functest" to be the lambda file > in src folder (whichever is the most recent)

@mock.patch("functest.pd.read_csv") #filename which we're patching - must be a string
def test_create_dataframe(read_csv_mock:mock.Mock):
    read_csv_mock.return_value=pd.DataFrame({"foo_id": [1, 2, 3]}) #can be anything , see below notes
    results=create_dataframe("test.csv",["bar_id"]) #mock csv, then change column name, then renaming it, bar had ot be in a list because columns
    read_csv_mock.assert_called_once()
    pd.testing.assert_frame_equal(results, pd.DataFrame({"bar_id": [1, 2, 3]}))


#^results=create_dataframe("test.csv",["bar_id"]) = made a new file so we cant mock test patch the entire file as pd

#* Next steps: when mocking other func,, do dummy chesterfield with 2 rows/columns123
#* Next steps: when mocking other func,, do dummy chesterfield with 2 rows/columns

