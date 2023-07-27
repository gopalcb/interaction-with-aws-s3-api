#!/usr/bin/env python
# coding: utf-8

# # AWS S3 API Usage

# * Assume AWS role
# * List AWS S3 buckets
# * Check if a bucket exists
# * Create S3 bucket
# * Delete S3 bucket
# * Create fake data object and upload to S3

# In[ ]:


get_ipython().system('pip install boto3')
get_ipython().system('pip install faker')


# In[1]:


import boto3
from botocore.exceptions import ClientError
import botocore


# In[2]:


aws_access_key = 'XXXXXXXXXXXXXXXXXX'
aws_secret_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXX'
aws_default_region = 'us-east-1'
session_token = ''
target_bucket_name = 'xxxxxxxxxxxx'


# AWS S3 functions using boto client -

# In[5]:


def aws_assume_role():
    try:
        print('aws assuming role')
        sts_client = boto3.client(
            'sts', 
            aws_access_key_id=aws_access_key, 
            aws_secret_access_key=aws_secret_key,
            region_name=aws_default_region
        )
        print('sts boto client created')
        
        account = sts_client.get_caller_identity()['Account']
        role = f'arn:aws:iam::{account}:role/ani_cnf_test_role'
        response = sts_client.assume_role(RoleArn=role, RoleSessionName='xxxxx', DurationSeconds=14400)
        
        session_token = response['Credentials']['SessionToken']
        
        print('aws role assume success')
        return session_token
        
    except ClientError as ex:
        print(f'Error aws role assume - {ex}')
    
    
def aws_list_s3_buckets():
    try:
        print('listing aws s3 buckets')
        zz = aws_assume_role()
        s3_client = boto3.client(
            's3', 
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=zz,
            region_name='us-east-1'
        )
        print('s3 boto client created')
        
        response = s3_client.list_buckets()
        buckets = [bucket["Name"] for bucket in response['Buckets']]
        
        return buckets
        
    except ClientError as ex:
        print(f'Error listing s3 buckets - {ex}')
        return False
    
    
def check_if_s3_bucket_exists(bucket_name):
    try:
        print('checking if s3 bucket exists')
        aws_assume_role()
        s3_client = boto3.resource('s3')
        bucket = s3_client.Bucket(bucket_name)
        
        bucket_exists = False
        if bucket.creation_date:
            print(f'{bucket_name} bucket exists')
            bucket_exists = True
        else:
            print(f'{bucket_name} bucket does not exist')
        
        return bucket_exists
    
    except ClientError as ex:
        print(f'Error in bucket exists check - {ex}')
        
        
def create_aws_s3_bucket(bucket_name, region=None):
    try:
        aws_assume_role()
        
        if region is None:
            print(f'creating s3 bucket in default region')
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            print(f'creating s3 bucket in {region} region')
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        
        print('created bucket successfully')
        return True
    
    except ClientError as ex:
        print(f'Error during creating bucket - {ex}')
        
        
def delete_aws_s3_bucket(bucket_name):
    try:
        aws_assume_role()
        s3_client = boto3.resource('s3')
        bucket = s3_client.Bucket(bucket_name)
        bucket.delete()
        
        print(f'{bucket_name} bucket deleted')
    
    except ClientError as ex:
        print(f'Error during deleting bucket - {ex}')
        


# Tiny data ingestion example -

# In[58]:


from faker import Faker
import json
import time


fake = Faker()

uploaded_files = []


def upload_data_to_s3_bucket(bucket_name, folder, file_name, data):
    try:
        print(f'uploading data to {bucket_name} bucket')
        data_string = json.dumps(data, indent=2, default=str)
        
        aws_assume_role()
        s3_client = boto3.client('s3')

        response = s3_client.put_object(
            Bucket=bucket_name, 
            Key=f'{folder}/{file_name}',
            Body=data_string
        )
        
        code = response['ResponseMetadata']['HTTPStatusCode']
        
        if code == 200:
            print(f'{file_name} uploaded successfully')
        else:
            print(f'failed uploading {file_name}')
            
        print('-'*10)
        
        return True
    
    except ClientError as ex:
        print(f'Data upload failed - {ex}')


def ingest_fake_data_and_upload():
    upload_count = 10
    batch_count = 0
    
    create_aws_s3_bucket(target_bucket_name)
    
    for itr in range(upload_count):
        batch_count = batch_count + 1
        
        data = [fake.profile() for x in range(10)]
        data = {'data': data}
        
        upload_data_to_s3_bucket(target_bucket_name, f'raw_batch_{batch_count}', f'raw_{batch_count}_file.json', data)
        uploaded_files.append(f'raw_{batch_count}_file.json')
        
        time.sleep(2)
        

ingest_fake_data_and_upload()

