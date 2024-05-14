# Version: 0.80
# Date: Jan 19, 2022
# ESP: Klaviyo
# Brands:
# Programmer/s: Roldan Roman
#
# Processing of relevant columns from various CSV's
# And consolidating them into one master file
# For addition to BigQuery

from http.client import SWITCHING_PROTOCOLS
from operator import index
import numpy as np
import pandas as pd
import os,sys
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import json
import shutil
import requests
import dropbox
import pathlib
import io
from zipfile import ZipFile,is_zipfile

def hello_gcs(request):
    
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #client_info = request.get_json()
    
    client_id = request.args.get('client_id') # REMOTE DEPLOY
    client_esp = request.args.get('esp')    # REMOTE DEPLOY
    process = request.args.get('process')   # REMOTE DEPLOY
    # client_id = 'tire_buyer'    # LOCAL DEPLOY
    # client_esp = 'Listrak'       # LOCAL DEPLOY
    # process = 'masterlist_update'   # LOCAL DEPLOY
    
    error_result = ''
    

    if process == 'masterlist_update':

        uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/'   # URL LINK OF CLIENT IN GSC
        
        try:
            client_module = __import__(client_id)
            error_result = client_module.update_masterlist(uri, client_id, client_esp)
            
        except Exception as e:
            print(e)
            error_result = error_result + ' ' + str(e)
            try:
                print('Using ESP Module')
                #esp_module = __import__(client_esp)
                #esp_module.update_masterlist(uri, client_id, client_esp)

            except Exception as e: 
                print(e)
                error_result = error_result + ' ' + str(e)
                print('module read failed')

        alert_email(client_id+ ' Masterlist Updated',client_id)

        if error_result == '':
            return(client_id + ' Database Updated')
        else:
            return(error_result)

    elif process == 'masterlist_backup':
        error_result = ''
        error_result = update_bigquery(request)
        
        return(error_result)


def update_bigquery(request):
    
    client_id = request.args.get('client_id')    # REMOTE DEPLOY
    client_esp = request.args.get('esp')       # REMOTE DEPLOY
    #client_id = 'sunday_lawn_care'    # LOCAL DEPLOY
    #client_esp = 'Listrak'    # LOCAL DEPLOY
    error_result = ''
    
    uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/'
    
    try:
        client_module = __import__(client_id)
        error_result = client_module.update_bigquery(uri, client_id, client_esp)
        
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + str(e)
        try:
            print('Using Klaviyo Module')
            esp_module = __import__(client_esp)
            error_result = esp_module.update_bigquery(uri, client_id, client_esp)

        except Exception as e: 
            print(e)
            error_result = error_result + ' ' + str(e)
            print('module read failed')
    alert_email(client_id + ' BigQuery Updated',client_id)
    if error_result == '':
        return(client_id + ' Database Updated')
    else:
        return(error_result)
    
    
def to_dropbox(dataframe,dbx_path,client_name):
    path = dbx_path + client_name + '2.xlsx'
    
    token = os.environ['DBXTOKEN']
    
    dbx = dropbox.Dropbox(token)
    team_namespace_id = dbx.users_get_current_account().root_info.root_namespace_id
    dbx_team = dbx.with_path_root(dropbox.common.PathRoot.namespace_id(team_namespace_id))

    #df_string = dataframe.to_excel(uri + 'Vant Panels.xlsx', sheet_name='Data', index=False)
    #db_bytes = bytes(df_string, 'utf8')

    #df = pd.DataFrame([range(5), list("ABCDE")])

    with io.BytesIO() as stream:
        with pd.ExcelWriter(stream) as writer:
            dataframe.to_excel(writer,sheet_name='Data',index=False)
            writer.save()
        stream.seek(0)
        meta = dbx_team.files_upload(
            stream.getvalue(),
            path=path,
            mode=dropbox.files.WriteMode("overwrite")
        )
    print('success upload to dropbox')

def to_ash_nyc(dataframe,dbx_path,client_name):
    path = dbx_path + client_name + '.xlsx'
    
    token = os.environ['DBXTOKEN']
    
    dbx = dropbox.Dropbox(token)
    team_namespace_id = dbx.users_get_current_account().root_info.root_namespace_id
    dbx_team = dbx.with_path_root(dropbox.common.PathRoot.namespace_id(team_namespace_id))

    #df_string = dataframe.to_excel(uri + 'Vant Panels.xlsx', sheet_name='Data', index=False)
    #db_bytes = bytes(df_string, 'utf8')

    #df = pd.DataFrame([range(5), list("ABCDE")])

    with io.BytesIO() as stream:
        with pd.ExcelWriter(stream) as writer:
            dataframe.to_excel(writer,sheet_name='Data',index=False)
            writer.save()
        stream.seek(0)
        meta = dbx_team.files_upload(
            stream.getvalue(),
            path=path,
            mode=dropbox.files.WriteMode("overwrite")
        )
    print('success upload to dropbox')

def alert_email(alert_message,client_id):
    #alert = requests.get('https://subjectlinepro.com/sendmail?to=rroman@alchemyworx.com&subject='+ client_id + ' alert'+ '&key=c9ee2dfb-d1e6-4762-9984-de659a29f8d8&message='+ alert_message)
    #print('email alert status: ',alert.status_code)
    print('----')


def get_table(client_id,str_cols,int_cols):
    
    p = 'alx-cloud'
    q = f'SELECT * FROM email.{client_id}'

    df = pd.read_gbq(q,p)
    # print(df)
    df['Segment'] = df['Original_Segment']
    #str_cols = ['Campaign','Mailing','by_Variant','Variant','Subject_Line','Segment','Type_2','Test_Type','Offer','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    #int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue',]
    for i in str_cols:
        df[i] = df[i].replace(np.nan, '-', regex=True)
        df[i] = df[i].replace('', '-', regex=True)
    for i in int_cols:
        df[i] = df[i].replace(np.nan, 0, regex=True)
        df[i] = df[i].replace('', 0, regex=True)
    
    return df
def get_table_old(client_id):
    
    p = 'alx-cloud'
    q = f'SELECT * FROM reporter_etl.{client_id}'

    df = pd.read_gbq(q,p)
    #str_cols = ['Campaign','Mailing','by_Variant','Variant','Subject_Line','Segment','Type_2','Test_Type','Offer','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    #int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue',]

    return df

def make_backup(client_id):
    uri = 'gs://' + os.environ['BUCKET'] + '/' + 'client_backups' + '/'
    df = get_table_old(client_id)
    df.to_excel(uri + client_id + '.xlsx', sheet_name='Data',index=False)
    return

def delete_table(client_id):
    client = bigquery.Client()
    #client_id = 'vant_panels_copy'
    q = f'DELETE FROM `alx-cloud.email.{client_id}` WHERE TRUE'

    query_job = client.query(q)
    results = query_job.result()
    print(results)

def logs(dt_start,name,count_input,count_output,status,client_id,client_esp):
    dt_end = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    client = bigquery.Client()
    table_id = "alx-cloud.reporter_etl.logs"
    rows_to_insert = [
    {"datetime_start": dt_start, "datetime_end": dt_end, "name": name, "input": str(count_input), "output":str(count_output), "result": status, "brand_esp": client_id + ' - ' + client_esp}
    ]
    errors = client.insert_rows_json(
        table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    )  # Make an API request.
    message = ''
    if errors == []:
        message = "New Log has been Added"
    else:
        message = "Encountered errors while inserting rows: {}".format(errors)
    print(message)
    return(message)

def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))

def read_old_campaign(uri,client_id,rev_order_df,session_df):
    
    #rev_order_df = pd.read_excel(uri + client_id + '-Old-Campaign-rev-order.xlsx',sheet_name='Dataset2')
    #session_df = pd.read_excel(uri + client_id + '-Old-Campaign-session.xlsx', sheet_name='Dataset2')
    
    df = pd.merge(rev_order_df,session_df, on='Day Index', how='left')
    
    # Renaming Columns
    df.rename(
        columns={
            'Day Index': 'Date',
            'Revenue': 'GA_Revenue',
            'Sessions': 'GA_Sessions',
            'Transactions': 'GA_Orders'
        },
        inplace=True
    )
    
    metric_columns = [
        'GA_Orders',
        'GA_Sessions',
        'GA_Revenue',
    ]
    # Fills and replacement
    df[metric_columns].fillna('0', inplace=True)
    df[metric_columns] = df[metric_columns].replace('|'.join([',', '\$']), '', regex=True)
    # Set data types
    for col in metric_columns:
        df[col] = pd.to_numeric(df[col])

    df = df.groupby('Date', as_index=False, sort=False).sum()

    # Format Date
    df['Date'] = df['Date'].astype('datetime64')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    df = df[~df.Date.isnull()]
    
    return(df)


def process_old_campaign(uri,client_id,updated_masterlist,rev_order_df,session_df):
    # Reading and combining old campaign files 
    old_campaign_df = read_old_campaign(uri,client_id,rev_order_df,session_df)

    # Filtering Out Existing Old Campaign from Updated Masterlist
    updated_masterlist = updated_masterlist[updated_masterlist['Type_1'] != 'Old Campaign']
    # Merging with Old Campaign rows with the Updated
    temp_table = updated_masterlist[['Date', 'GA_Revenue', 'GA_Orders', 'GA_Sessions']].copy()
    

    updated_table = pd.merge(old_campaign_df,temp_table, on='Date', how='left')
    
    # Format Values
    updated_table[['GA_Revenue_y', 'GA_Orders_y', 'GA_Sessions_y']] = updated_table[['GA_Revenue_y', 'GA_Orders_y', 'GA_Sessions_y']].replace(['',np.nan], 0, regex=True)
    metric_columns = ['GA_Orders_x','GA_Sessions_x','GA_Revenue_x','GA_Revenue_y', 'GA_Orders_y', 'GA_Sessions_y']
    updated_table[metric_columns].fillna('0', inplace=True)
    updated_table[metric_columns] = updated_table[metric_columns].replace('|'.join([',', '\$']), '', regex=True)
    # Set data types
    for col in metric_columns:
        updated_table[col] = pd.to_numeric(updated_table[col])

    # Grouping Duplicate Values from the Old Campaign
    updated_table = updated_table.groupby(['Date', 'GA_Revenue_x', 'GA_Orders_x', 'GA_Sessions_x'], as_index=False, sort=False).sum()

    old_campaign = pd.DataFrame()
    # Setting Default Values for rows and Calculated Values (Subtracting GA metrics Updated Masterlist from Old Campaign Data)
    old_campaign['Date'] = updated_table['Date']
    old_campaign['Name'] = 'Old Campaign'
    old_campaign['Type_0'] = 'Campaign'
    old_campaign['Type_1'] = 'Old Campaign'
    old_campaign['GA_Revenue'] = updated_table['GA_Revenue_x'] - updated_table['GA_Revenue_y']
    old_campaign['GA_Orders'] = updated_table['GA_Orders_x'] - updated_table['GA_Orders_y']
    old_campaign['GA_Sessions'] = updated_table['GA_Sessions_x'] - updated_table['GA_Sessions_y']
    
    # Making Null rows for the Old Campaign Dataframe
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Type_3','Test_Type','Offer','campaign_id','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue',]
    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','campaign_id','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Sessions','GA_Orders','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    
    for i in str_cols:
        old_campaign[i] = '-'
    for i in int_cols:
        old_campaign[i] = 0

    
    old_campaign = old_campaign[all_cols].copy()

    return(old_campaign)



    
def get_mailing_types(client_id,client_name):
    uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/'
    # Mailing Types
    mailing_type_df = pd.DataFrame()
    try:
        mailing_type_df = pd.read_csv(uri + client_id +'-Campaign-Types.csv')
        mailing_type_df = mailing_type_df[mailing_type_df['Client'] == client_name]
        mailing_type_df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True, inplace=True)
        
        mailing_type_df.rename(columns={'Test Type':'Test_Type','Variant':'Variant','Type 2':'Type_2'},inplace=True)
        mailing_type_df = mailing_type_df[['Mailing','Test_Type', 'Variant', 'Type_2','Offer']].copy()
        
        mailing_type_df['Variant2'] = mailing_type_df['Variant']
        mailing_type_df['Variant2'] = (mailing_type_df['Variant'].str.split(':', expand=True)[int(0)])
        
        mailing_type_df['Variant2'] = mailing_type_df['Variant2'].replace('No Test','n/a', regex=True)
        
        mailing_type_df['MailingVariant'] = (mailing_type_df['Mailing'] + mailing_type_df['Variant2'])
        
        mailing_type_df = mailing_type_df[['MailingVariant','Test_Type', 'Variant', 'Type_2','Offer']].copy()
        
        #promo_pattern = '|'.join(['-Promo-Buyer', '-Promo-NonBuyer', '-Promo-Other','-NonPurch','-Purch'])
        #mailing_type_df['MailingVariant'] = mailing_type_df['MailingVariant'].str.replace(promo_pattern, '')
        mailing_type_df = mailing_type_df.drop_duplicates(subset=['MailingVariant'], keep='last')
        
       

    except Exception as e:
        print(e)
    
    return mailing_type_df


def list_blobs(client_id):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs('reporter-etl', prefix=client_id+'/', delimiter='/')
    #for blob in blobs:
        #print(blob.name)

    return blobs
def checkZipFile(client_id,file_name,archive_datetime):
    print("going for zip check")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('reporter-etl')
    print("working....")
    zipfilename_with_path = client_id+'/'+file_name
    destination_blob_pathname = zipfilename_with_path
    # blob = bucket.blob('mattress_warehouse/mattress_warehouse0430_0506.zip')
    zipbytes=""
    try:
        blob = bucket.blob(destination_blob_pathname)
        print("====>")
        zipbytes = io.BytesIO(blob.download_as_string())
        print("file downloaded")
    except:
        return "error in checkzipfile"
    if is_zipfile(zipbytes):
        print("zip file found")
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                # print("contentfile ",contentfile)
                blob = bucket.blob(client_id+ "/" + contentfilename)
                blob.upload_from_string(contentfile)
    # archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    copy_blob('reporter-etl',client_id + '/' + file_name,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +'/'+ file_name)
    delete_blob('reporter-etl',client_id + '/' + file_name)
    zipbytes=""
    return ""

def relogs(dt_start,dt_end,name,count_input,count_output,status,client_id,client_esp):
    dt_start= '11/14/2022 02:08:09'
    dt_end = '11/14/2022 02:08:09'
    name = 'Message-Activity.xlsx' # ConversationActivity-SCAProgram-Abandonment.xlsx #Message-Activity.xlsx
    count_input = '274 rows'
    count_output = '274 rows'
    status = 'success'
    client_id = 'mattress_warehouse'
    client_esp = 'Listrak'

    relogs(dt_start,dt_end,name,count_input,count_output,status,client_id,client_esp)

    return('success')
    
    client = bigquery.Client()
    table_id = "alx-cloud.reporter_etl.logs"
    rows_to_insert = [
    {"datetime_start": dt_start, "datetime_end": dt_end, "name": name, "input": str(count_input), "output":str(count_output), "result": status, "brand_esp": client_id + ' - ' + client_esp}
    ]
    errors = client.insert_rows_json(
        table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    )  # Make an API request.
    message = ''
    if errors == []:
        message = "New Log has been Added"
    else:
        message = "Encountered errors while inserting rows: {}".format(errors)
    print(message)
    return(message)

