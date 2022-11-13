import requests
import zipfile
import io
import pandas as pandas

import json
import os
import boto3

#---------------
# Get API Keys
#---------------
content = open('config.json')
config = json.load(content)
access_key = config['access_key']
secret_access_key = config['secret_access_key']

#---------------
# EXTRACT
#---------------
# We use the ipeds prefix because the data comes from the:
# [I]ntegrated [P]ostsecondary [E]ducation [D]ata [S]ystem
ipeds_locs = 'https://nces.ed.gov/ipeds/datacenter/data/'

## We will get 12-Month Enrollment which has a prefix of EFFY
#-12-month unduplicated headcount by race/ethnicity, gender and level of student

ipeds_fils = 'EFFY{}.zip'
ipeds_dict = 'EFFY{}_Dict.zip'
ipeds_xlsx = 'EFFY{}.xlsx'
years = [2016,2017,2018]

def extract():
    try:
        #A for loop that will loop through the values in years to get the data
        for yr in years:
            print('GETTING FILES FROM {}'.format(yr))
            rdata = requests.get(ipeds_locs + ipeds_fils.format(yr))
            rdict = requests.get(ipeds_locs + ipeds_dict.format(yr))
            rdata_zip = zipfile.ZipFile(io.BytesIO(rdata.content))
            rdict_zip = zipfile.ZipFile(io.BytesIO(rdict.content))
    
            print('Extracting {} files from zip archive:'.format(yr))
            rdata_zip.printdir()
            rdict_zip.printdir()
            rdata_zip.extractall()
            rdict_zip.extractall()

            print('Places {} files into data frame:'.format(yr))
            df = pandas.read_excel(ipeds_xlsx.format(yr))
            load(df, ipeds_xlsx.format(yr))
    except Exception as e:
        print("Data extract error: " + str(e))



#---------------
# LOAD - in S3
#---------------
def load(df, tbl):
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save to s3
        upload_file_bucket = 'hied-delta-lake'
        upload_file_key = 'raw/' + f"/{str(tbl)}"
        filepath =  upload_file_key #+ ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-west-2')
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(df)
            print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))