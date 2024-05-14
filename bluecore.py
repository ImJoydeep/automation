import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime
import json
from google.cloud import storage
import datetime as dt
import requests


def update_masterlist(uri,client_id,client_esp):
    
    main_module = __import__('main')
    
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_type_df = pd.DataFrame()
    trigger_analytics_data_df = pd.DataFrame()
    
    # Reading Campaign Files
    try:
        campaign_data_df = pd.read_csv(uri + client_id +'-bluecore.py-Campaigns.csv')
    except:
        dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
        main_module.logs(dt_start, 'bluecore.py-Campaigns.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return

    try:
        campaign_analytics_data_df = pd.read_csv(uri + client_id +'-Campaign-Analytics.csv', skiprows=6)
    except:
        dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
        main_module.logs(dt_start,'Campaign-Analytics.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return

    # Reading Trigger Files
    try:
        trigger_data_df = pd.read_csv(uri + client_id +'-Klaviyo-Flows.csv',skiprows=5) 
        
    except:
        dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
        main_module.logs(dt_start,'bluecore.py-Trigger.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return

    try:
        trigger_analytics_data_df = pd.read_csv(uri + client_id +'-Trigger-Analytics.csv', skiprows=6) 
    except:
        dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
        main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return

    try:
        trigger_type_df = pd.read_csv(uri + client_id +'-Trigger-Types.csv')
    except:
        dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
        main_module.logs(dt_start,'Trigger-Types.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return

    
    # For Reconciliation
    initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum(), 2) 
    initial_revenue = round(campaign_data_df['Ordered Product Value'].sum() + trigger_data_df['Ordered Product Value'].sum(), 2) 

    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Campaign_ID','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Sessions','GA_Orders','GA_Revenue']
    
    # Campaign Transformation / Process
    main_campaign_df = campaign_transform(campaign_data_df,client_id,client_esp)
    
    main_campaign_analytics_df = ga_transform(campaign_analytics_data_df,'Campaign-Analytics.csv',client_id,client_esp)
    main_campaign_masterlist_df = campaign_ga_join(main_campaign_df,main_campaign_analytics_df)
    main_campaign_masterlist_df = main_campaign_masterlist_df[all_cols].copy()
    

    # Trigger Transformation / Joining Trigger Types
    main_trigger_analytics_df = ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
    
    main_trigger_df = Trigger_Transform(trigger_data_df,client_id,client_esp)
    main_trigger_df = pd.merge(main_trigger_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
    main_trigger_df = main_trigger_df.drop(columns=['Campaign Name'])
    main_trigger_masterlist_df = campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
    main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()

    main_masterlist_df = main_campaign_masterlist_df.append(main_trigger_masterlist_df)
    main_masterlist_df = main_masterlist_df[all_cols].copy()

    # For Reconciliation
    initial_rowcount = main_campaign_masterlist_df['Name'].count() + main_trigger_masterlist_df['Name'].count()
    final_rowcount = main_masterlist_df['Name'].count()
    final_delivered = round(main_masterlist_df['Delivered'].sum(), 2)
    final_revenue = round(main_masterlist_df['Revenue'].sum(), 2)
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    # Check Row Count
    if initial_rowcount != final_rowcount:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs BigQuery rows', str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows', 'not matched',client_id,client_esp)
        main_module.alert_email('Campaign + Trigger rows vs BigQuery rows did not matched',client_id)
        return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs BigQuery rows', str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows', 'matched',client_id,client_esp)
    # Check Delivered
    if initial_delivered != final_delivered:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'not matched',client_id,client_esp)
        main_module.alert_email('Campaign + Trigger rows vs Masterlist Delivered Counts did not matched',client_id)
        return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'matched',client_id,client_esp)
    # Check Revenue
    if initial_revenue != final_revenue:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'not matched',client_id,client_esp)
        main_module.alert_email('Campaign + Trigger rows vs Masterlist Revenue Counts did not matched',client_id)
        return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'matched',client_id,client_esp)

    # Backing up Dataframes
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_masterlist_df.to_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv', index=False)
    masterlist_bigquery = main_module.get_table(client_id)
    masterlist_bigquery.to_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv', index=False)

    # Updating Bigquery Dataframe
    updated_masterlist = pd.concat([masterlist_bigquery,main_masterlist_df])
    updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Variant','Subject_Line','Segment'], keep='last').sort_values('Date')
    updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)

    # Updating to BigQuery
    main_module.delete_table(client_id)
    bigquery_insert(client_id)

    # Archiving Source Files
    files_list = [client_id+'-bluecore.py-Campaigns.csv', client_id+'-bluecore.py-Trigger.csv', client_id+'-Campaign-Analytics.csv', client_id+'-Trigger-Analytics.csv', client_id+'-Trigger-Types.csv']
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    for file_name in files_list:
        main_module.copy_blob('reporter-etl',client_id + '/' + file_name,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +' '+ file_name)
    
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
    

def campaign_transform(campaign_data_df,client_id,client_esp,client_name):
    main_module = __import__('main')
    df = campaign_data_df            
    

    # Rename columns to Default Column Names
    cols_include = {
        'Send Time' : 'Date',
        'Campaign Name' : 'Name',
        'Campaign ID' : 'Campaign_ID',
        'Subject' : 'Subject_Line',
        'Variant' : 'Variant',
        'Variant Name': 'Variant',
        'Total Opens' : 'Total_Opens',
        'Total Clicks' : 'Total_Clicks',
        'List' : 'Segment',
        'Total Recipients' : 'Sent',
        'Successful Deliveries' : 'Delivered',
        'Unique Opens' : 'Opens',
        'Unique Clicks' : 'Clicks',
        'Unique purchaseComplete': 'Orders',
        'Unique Placed Order' : 'Orders',
        'Ordered Product': 'Orders',
        'Unique Ordered Product': 'Orders',
        'Subscription Started': 'Orders',
        'Purchase Event': 'Orders',
        'Unique Purchase Event': 'Orders',
        'Unique Subscription Started':'Orders',
        'Revenue' : 'Revenue',
        'Subscription Started Value':'Revenue',
        'purchaseComplete Value': 'Revenue',
        'Ordered Product Value': 'Revenue',
        'Purchase Event Value': 'Revenue',
        'Placed Order Value': 'Revenue',
        'Unsubscribes' : 'Unsub',
        'Spam Complaints': 'Complaints',
        'Bounces': 'Total_Bounces',
        'Send Weekday': 'Send_Weekday',
        'Winning Variant?': 'Winning_Variant_question_',

        }
    df.rename(
        columns=cols_include,
        inplace=True)
    
    # For Logging / Reconcilliation
    initial_rowcount = str(df['Name'].count())
    initial_delivered = df['Delivered'].sum()
    initial_revenue = df['Revenue'].sum()
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    metric_columns = [
        'Sent',
        'Delivered',
        'Opens',
        'Total_Opens',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue'
    ]
    

    # Fills and replacement
    df[metric_columns].fillna('0', inplace=True)
    df[metric_columns] = df[metric_columns].replace('|'.join([',', '\$']), '', regex=True)
    

    # Format metric columns to numeric
    for col in metric_columns:
        df[col] = pd.to_numeric(df[col])

    

    # Add "By Mailing" column
    df['Mailing'] = (
        df['Name']
        .str.split('*', expand=True)[int(0)]
        #.iloc[:, :1]
        #.transpose()
        #.apply(lambda x: x.str.cat(sep=esp_obj["delimiter"]))
    )

    promo_pattern = '|'.join(['-Promo-Buyer', '-Promo-NonBuyer', '-Promo-Other'])
    df['Mailing'] = df['Mailing'].str.replace(promo_pattern, '')

    #df['Campaign'] = df['Mailing']
    # Add "By Campaign" column
    df['Campaign'] = (
        df['Mailing']
        .str.split('-',n=1, expand=True)[int(1)]
    )
    # Removing Versions
    version_pattern = '|'.join(['-v1', '-v2', '-v3','-v4','-v5','-v6','-V1','-V2','-V3','-V4','-V5','-V6','-resend'])
    df['Mailing'] = df['Mailing'].str.replace(version_pattern, '')

    # Format Date
    df['Date'] = df['Date'].astype('datetime64')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    mailing_types = main_module.get_mailing_types(client_id,client_name)
    if mailing_types.empty:
        # Constant value
        df['ESP'] = 'Klaviyo'
        df['Type_0'] = 'Campaign'
        df['Type_1'] = 'Promo'
        df['Type_2'] = '-'
        df['Type_3'] = '-'
        df['Flow_Message_ID'] = '-'
        df['Flow_ID'] = '-'
        df['Flow_Message_Name'] = '-'
        df['Test_Type'] = '-'
        df['Offer'] = '-'
        print('no mailing types')
    else:
        df['ESP'] = 'Klaviyo'
        df['Type_0'] = 'Campaign'
        df['Type_1'] = 'Promo'
        df['Type_3'] = '-'
        df['Flow_Message_ID'] = '-'
        df['Flow_ID'] = '-'
        df['Flow_Message_Name'] = '-'

        df['Variant'] = df['Variant'].replace(np.nan, 'n/a', regex=True)
        df['Variant'] = df['Variant'].replace('', 'n/a', regex=True)
        df['MailingVariant'] = df['Mailing'] + df['Variant']
        first_col = df.pop('MailingVariant')
        df.insert(0, 'MailingVariant', first_col)
        df.rename(columns={'Variant':'Variant New'},inplace=True)
        uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/'
        
        df = pd.merge(df,mailing_types, on='MailingVariant', how='left')
        df.loc[df["Variant"].isnull(),'Variant'] = df["Variant New"]
        df['Variant'] = df['Variant'].str.replace('A: |B: |C: |D: |E: ', "", regex=True)
        #df['Variant'] = df['Variant'].str.replace('', "-", regex=True)
        
        #df = pd.merge(df,mailing_types, on='Mailing', how='left')
        
    # Removing Unwanted Substrings
    promo_pattern = '|'.join(['-NonPurch','-Purch'])
    df['Mailing'] = df['Mailing'].str.replace(promo_pattern, '')

    str_cols = ['Date','Name','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    df[str_cols] = df[str_cols].replace(np.nan, '-', regex=True)
    df[str_cols] = df[str_cols].replace('', '-', regex=True)
    
    


    # Selecting rows Needed
    df = df[[
        'Date',
        'Name',
        'Campaign',
        'Mailing',
        'Variant',
        'Subject_Line',
        'Segment',
        'Type_0',
        'Type_1',
        'Type_2',
        'Type_3',
        'Test_Type',
        'Offer',
        'Campaign_ID',
        'ESP',
        'Sent',
        'Delivered',
        'Opens',
        'Total_Opens',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue',
        'Send_Weekday',
        'Winning_Variant_question_',
        'Flow_Message_ID',
        'Flow_ID',
        'Flow_Message_Name']].copy()

    # For Logging / Reconciliation
    final_rowcount = str(df['Name'].count())
    final_delivered = df['Delivered'].sum()
    final_revenue = df['Revenue'].sum()

    if initial_delivered != final_delivered:
        print("Campaigns Delivered doesn't match")
        return
    
    if initial_revenue != final_revenue:
        print("the revenue are",initial_revenue,final_revenue)
        print("Campaign Revenue doesn't match")
        return

    logs(dt_start, 'Klaviyo-Campaign.csv',str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows','success',client_id,client_esp)
    
    return df

def campaign_transform_sms(campaign_data_df,client_id,client_esp,client_name):
    main_module = __import__('main')
    df = campaign_data_df            
    

    # Rename columns to Default Column Names
    cols_include = {
        'Send Time' : 'Date',
        'Campaign Name' : 'Name',
        'Campaign ID' : 'Campaign_ID',
        'Subject' : 'Subject_Line',
        'Variant' : 'Variant',
        'Total Opens' : 'Total_Opens',
        'Total Clicks' : 'Total_Clicks',
        'List' : 'Segment',
        'Total Recipients' : 'Sent',
        'Successful Deliveries' : 'Delivered',
        'Unique Opens' : 'Opens',
        'Unique Clicks' : 'Clicks',
        'Unique purchaseComplete': 'Orders',
        'Unique Placed Order' : 'Orders',
        'Ordered Product': 'Orders',
        'Unique Ordered Product': 'Orders',
        'Subscription Started': 'Orders',
        'Unique Purchase Event': 'Orders',
        'Purchase Event': 'Orders',
        'Unique Subscription Started':'Orders',
        'Revenue' : 'Revenue',
        'Subscription Started Value':'Revenue',
        'Purchase Event Value': 'Revenue',
        'purchaseComplete Value': 'Revenue',
        'Ordered Product Value': 'Revenue',
        'Placed Order Value': 'Revenue',
        'Unsubscribes' : 'Unsub',
        'Spam Complaints': 'Complaints',
        'Bounces': 'Total_Bounces',
        'Send Weekday': 'Send_Weekday',
        'Winning Variant?': 'Winning_Variant_question_',

        }
    df.rename(
        columns=cols_include,
        inplace=True)
    
    # For Logging / Reconcilliation
    initial_rowcount = str(df['Name'].count())
    initial_delivered = df['Delivered'].sum()
    initial_revenue = df['Revenue'].sum()
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    metric_columns = [
        'Sent',
        'Delivered',
        'Opens',
        'Total_Opens',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue'
    ]
    

    # Fills and replacement
    df[metric_columns].fillna('0', inplace=True)
    df[metric_columns] = df[metric_columns].replace('|'.join([',', '\$']), '', regex=True)
    

    # Format metric columns to numeric
    for col in metric_columns:
        df[col] = pd.to_numeric(df[col])

    

    # Add "By Mailing" column
    df['Mailing'] = (
        df['Name']
        .str.split('*', expand=True)[int(0)]
        #.iloc[:, :1]
        #.transpose()
        #.apply(lambda x: x.str.cat(sep=esp_obj["delimiter"]))
    )

    # Removing Unwanted Substrings
    promo_pattern = '|'.join(['-Promo-Buyer', '-Promo-NonBuyer', '-Promo-Other','-NonPurch','-Purch'])
    df['Mailing'] = df['Mailing'].str.replace(promo_pattern, '')

    #df['Campaign'] = df['Mailing']
    # Add "By Campaign" column
    df['Campaign'] = (
        df['Mailing']
        .str.split('-',n=1, expand=True)[int(1)]
    )
    # Removing Versions
    version_pattern = '|'.join(['-v1', '-v2', '-v3','-v4','-v5','-v6','-V1','-V2','-V3','-V4','-V5','-V6','-resend'])
    df['Mailing'] = df['Mailing'].str.replace(version_pattern, '')

    
    # Format Date
    df['Date'] = df['Date'].astype('datetime64')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    #mailing_types = main_module.get_mailing_types(client_id,client_name)
    #if mailing_types.empty:
        # Constant value
    df['ESP'] = 'Klaviyo'
    df['Type_0'] = 'Campaign'
    df['Type_1'] = 'Promo'
    df['Type_2'] = 'SMS'
    df['Type_3'] = '-'
    df['Flow_Message_ID'] = '-'
    df['Flow_ID'] = '-'
    df['Flow_Message_Name'] = '-'
    df['Test_Type'] = '-'
    df['Offer'] = '-'
    #print('no mailing types')
    #else:
        #df['ESP'] = 'Klaviyo'
        #df['Type_0'] = 'Campaign'
        #df['Type_1'] = 'Promo'
        #df['Type_3'] = '-'
        #df['Flow_Message_ID'] = '-'
        #df['Flow_ID'] = '-'
        #df['Flow_Message_Name'] = '-'
        #df = pd.merge(df,mailing_types, on='Mailing', how='left')
    
    str_cols = ['Date','Name','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    df[str_cols] = df[str_cols].replace(np.nan, '-', regex=True)
    

    # Selecting rows Needed
    df = df[[
        'Date',
        'Name',
        'Campaign',
        'Mailing',
        'Variant',
        'Subject_Line',
        'Segment',
        'Type_0',
        'Type_1',
        'Type_2',
        'Type_3',
        'Test_Type',
        'Offer',
        'Campaign_ID',
        'ESP',
        'Sent',
        'Delivered',
        'Opens',
        'Total_Opens',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue',
        'Send_Weekday',
        'Winning_Variant_question_',
        'Flow_Message_ID',
        'Flow_ID',
        'Flow_Message_Name']].copy()

    # For Logging / Reconciliation
    final_rowcount = str(df['Name'].count())
    final_delivered = df['Delivered'].sum()
    final_revenue = df['Revenue'].sum()

    if initial_delivered != final_delivered:
        print("Campaigns Delivered doesn't match")
        return
    
    if initial_revenue != final_revenue:
        print("the revenue are",initial_revenue,final_revenue)
        print("Campaign Revenue doesn't match")
        return

    logs(dt_start, 'Klaviyo-Campaign.csv',str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows','success',client_id,client_esp)
    
    return df

def ga_transform(analytics_data_df,file_name,client_id,client_esp):
    df = analytics_data_df
    
    # For Logging
    initial_rowcount = str(df['Campaign'].count())

    df = df[df['Revenue'].notna()]
    df = df[df['Campaign'].notna()]
    
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    # Renaming Columns
    df.rename(
        columns={
            'Campaign': 'Campaign_ID',
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
    
    initial_revenue = df['GA_Revenue'].sum()

    # Extract Campaign ID from Name
    df['Campaign Regex'] = df['Campaign_ID'].str.extract('(\(.*?\))')[0].str.strip('()')
    df['Campaign Regex'].fillna(df['Campaign_ID'], inplace=True)
    df['Campaign_ID'] = df['Campaign Regex']
    
    # Combine duplicate IDs
    df = df.groupby('Campaign_ID', as_index=False, sort=False).sum()

    df = df[[
        'Campaign_ID',
        'GA_Sessions',
        'GA_Orders',
        'GA_Revenue']].copy()

    final_rowcount = str(df['Campaign_ID'].count())
    final_revenue = df['GA_Revenue'].sum()

    #uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/'
    #df.to_csv(uri + 'test.csv', index=False)
    #print('stop--------------')
    #if initial_revenue != final_revenue:
        #print(initial_revenue,final_revenue)
        #print(file_name," Revenue doesn't match")
        #return
    
        #logs(dt_start, file_name,str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows','success',client_id,client_esp)
    return df

def campaign_ga_join(df_esp,df_ga):
    # Lowercase campaign id
    df_esp['Campaign_ID'] = df_esp['Campaign_ID'].str.lower()
    df_ga['Campaign_ID'] = df_ga['Campaign_ID'].str.lower()

    # Sorting Table
    df_esp.sort_values(['Name','Date','Campaign_ID','Variant'], inplace=True)
    
    # Establish a unique ID for each rowitem.
    df_ga['uuid'] = df_ga.apply(lambda _: uuid.uuid4(), axis=1)
    
    # merge_tables together
    df_merge = pd.merge(
        df_esp,
        df_ga,
        on='Campaign_ID',
        how='left',
    )
    
    # Include a cumulative count based onUUID across all merged rows.
    df_merge['dupe'] = df_merge.groupby('uuid').cumcount()
    
    # Find all duplicates (rows which were not the first of its uuid)
    # Replace the GA data with 0.
    
    df_merge.loc[df_merge['dupe'] > 0, df_ga.columns] = 0
    
    # Remove unnecessary rows.
    df_merge = df_merge[[c for c in df_merge.columns if c not in ['uuid', 'dupe',]]]
    df_merge[['GA_Orders', 'GA_Sessions','GA_Revenue']] = df_merge[['GA_Orders', 'GA_Sessions','GA_Revenue']].apply(pd.to_numeric) 
    df_merge = df_merge.replace(np.nan, '', regex=True)
    df_merge.sort_values(['Date'], inplace=True)
    
    return df_merge

def Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name):
    df = trigger_data_df
    
    # Filtering Rows with Delivered Value
    df = df[df['Delivered'] != 0]

    df['Send_Weekday'] = '-'
    df['Winning_Variant_question_'] = '-'
    df['Test_Type'] = '-'
    df['Offer'] = '-'
    
    rename_col ={
        'Day' : 'Date',
        trigger_name : 'Name',
        'Flow Message ID' : 'Flow_Message_ID',
        'Flow ID' : 'Flow_ID',
        trigger_flow_name : 'Flow_Message_Name',
        'Variant' : 'Variant',
        'Variant Name': 'Variant',
        'Delivered' : 'Delivered',
        'Unique Opens' : 'Opens',
        'Unique Clicks' : 'Clicks',
        'Revenue' : 'Revenue',
        'Placed Order' : 'Orders',
        'Ordered Product': 'Orders',
        'Purchase Event': 'Orders',
        'Unique Purchase Event': 'Orders',
        'purchaseComplete Value': 'Revenue',
        'Ordered Product Value': 'Revenue',
        'Purchase Event Value': 'Revenue',
        'Order Complete Value': 'Revenue',
        'Subscription Started Value': 'Revenue',
        'Unique Order Complete': 'Orders',
        'Subscription Started': 'Orders',
        'Unique Purchase Event': 'Orders',
        'purchaseComplete': 'Orders',
        'Unsub Rate' : 'Unsub',
        'Complaint Rate' : 'Complaints',
        'Bounce Rate' : 'Total_Bounces',
        }
    # Rename columns to Default Column Names
    df.rename(
        columns=rename_col,
        inplace=True)
    df['Sent'] = df['Delivered']

    # For Logging / Reconciliation
    initial_rowcount = str(df['Name'].count())
    initial_delivered = df['Delivered'].sum()
    initial_revenue = df['Revenue'].sum()
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    df['Campaign'] = df['Name']
    df['Mailing'] = df['Name']

    # Including Null Columns
    cols_exclude = {
        'Total_Opens': '0',
        'Total_Clicks': '0',
        'Segment': '-',
        'Subject_Line':'-',
        }
    for i in cols_exclude:
        df[i] = cols_exclude[i]

    metric_columns = [
    'Sent',
    'Delivered',
    'Opens',
    'Total_Opens',
    'Clicks',
    'Total_Clicks',
    'Unsub',
    'Complaints',
    'Total_Bounces',
    'Orders',
    'Revenue'
    ]
    
    # Fills and replacement 
    
    df[metric_columns].fillna('0', inplace=True)
    df[metric_columns] = df[metric_columns].replace('|'.join([',', '\$']), 0, regex=True)

    # Formats Metrics columns to numeric
    for col in metric_columns:
        df[col] = pd.to_numeric(df[col])

    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    
    

    # Computed Fields
    df['Complaints'] = df['Complaints'] * df['Delivered']
    df['Total_Bounces'] = df['Total_Bounces'] * df['Delivered']
    df['Unsub'] = df['Unsub'] * df['Delivered']
    df['Campaign_ID'] = df['Flow_Message_ID']
    
    str_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Test_Type','Offer','Campaign_ID','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
    df[str_cols] = df[str_cols].replace(np.nan, '-', regex=True)
    
    # Selecting Columns to return
    df = df[[
        'Date',
        'Name',
        'Campaign',
        'Mailing',
        'Variant',
        'Subject_Line',
        'Segment',
        'Test_Type',
        'Offer',
        'Campaign_ID',
        'Sent',
        'Delivered',
        'Opens',
        'Total_Opens',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue',
        'Send_Weekday',
        'Winning_Variant_question_',
        'Flow_Message_ID',
        'Flow_ID',
        'Flow_Message_Name']].copy()
    

    # For Loggin / Reconciliation
    final_rowcount = str(df['Name'].count())
    final_delivered = df['Delivered'].sum()
    final_revenue = df['Revenue'].sum()

    
    if initial_delivered != final_delivered:
        print("Campaigns Delivered doesn't match")
        return
    
    if round(initial_revenue, 2) != round(final_revenue, 2):
        print(initial_revenue,final_revenue)
        print("Trigger Revenue doesn't match")
        return

    logs(dt_start,'Klaviyo-Flows.csv',str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows','success',client_id,client_esp)
    
    return df


def bigquery_insert(client_id):
    # Bigquery Schema
    client = bigquery.Client()
    dataset_id = 'email'
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField('Date', 'DATE'),
        bigquery.SchemaField('Name', 'STRING'),
        bigquery.SchemaField('Campaign', 'STRING'),
        bigquery.SchemaField('Mailing', 'STRING'),
        bigquery.SchemaField('Variant', 'STRING'),
        bigquery.SchemaField('Subject_Line', 'STRING'),
        bigquery.SchemaField('Segment', 'STRING'),
        bigquery.SchemaField('Type_0', 'STRING'),
        bigquery.SchemaField('Type_1', 'STRING'),
        bigquery.SchemaField('Type_2', 'STRING'),
        bigquery.SchemaField('Offer', 'STRING'),
        bigquery.SchemaField('Segment_Engagement', 'STRING'),
        bigquery.SchemaField('Segment_BNB', 'STRING'),
        bigquery.SchemaField('Segment_Freq', 'STRING'),
        bigquery.SchemaField('Test_Type', 'STRING'),
        bigquery.SchemaField('Sent', 'FLOAT'),
        bigquery.SchemaField('Delivered', 'FLOAT'),
        bigquery.SchemaField('Opens', 'FLOAT'),
        bigquery.SchemaField('Clicks', 'FLOAT'),
        bigquery.SchemaField('Revenue', 'FLOAT'),
        bigquery.SchemaField('Orders', 'FLOAT'),
        bigquery.SchemaField('GA_Revenue', 'FLOAT'),
        bigquery.SchemaField('GA_Orders', 'FLOAT'),
        bigquery.SchemaField('GA_Sessions', 'FLOAT'),
        bigquery.SchemaField('AA_Visits', 'FLOAT'),
        bigquery.SchemaField('AA_Revenue', 'FLOAT'),
        bigquery.SchemaField('AA_SFCC_Demand', 'FLOAT'),
        bigquery.SchemaField('AA_Orders', 'FLOAT'),
        bigquery.SchemaField('AA_Mobile_Visits', 'FLOAT'),
        bigquery.SchemaField('Units', 'FLOAT'),
        bigquery.SchemaField('Product_Margin', 'FLOAT'),
        bigquery.SchemaField('Total_Margin', 'FLOAT'),
        bigquery.SchemaField('Unsub', 'FLOAT'),
        bigquery.SchemaField('Complaints', 'FLOAT'),
        bigquery.SchemaField('Hard_Bounces', 'FLOAT'),
        bigquery.SchemaField('Soft_Bounces', 'FLOAT'),
        bigquery.SchemaField('Total_Bounces', 'FLOAT'),
        bigquery.SchemaField('by_Variant', 'STRING'),
        bigquery.SchemaField('ESP', 'STRING'),
        bigquery.SchemaField('Send_Weekday', 'STRING'),
        bigquery.SchemaField('Total_Opens', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Campaign_ID', 'STRING'),
        bigquery.SchemaField('Winning_Variant_question_', 'STRING'),
        bigquery.SchemaField('Flow_Message_ID', 'STRING'),
        bigquery.SchemaField('Flow_ID', 'STRING'),
        bigquery.SchemaField('Flow_Message_Name', 'STRING'),
        bigquery.SchemaField('Error', 'STRING'),
        bigquery.SchemaField('Original_Segment', 'STRING'),
        bigquery.SchemaField('FISCAL_YEAR', 'INTEGER'),
        bigquery.SchemaField('FISCAL_YEAR_START', 'DATE'),
        bigquery.SchemaField('FISCAL_WEEK', 'INTEGER'),
        bigquery.SchemaField('FISCAL_MONTH', 'INTEGER'),
        bigquery.SchemaField('FISCAL_QUARTER', 'INTEGER'),

            ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    # get the URI for uploaded CSV in GCS from 'data'
    uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/Masterlist.csv'

    # lets do this
    load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(client_id),
            job_config=job_config)

    # Logs
    print('Starting job {}'.format(load_job.job_id))
    print('Function=csv_loader, Version=' + os.environ['VERSION'])
    #print('File: {}'.format(data['name']))

    load_job.result()  # wait for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(dataset_ref.table(client_id))
    print('Loaded {} rows.'.format(destination_table.num_rows))

def bigquery_reinsert(client_id):
    # Bigquery Schema
    client = bigquery.Client()
    dataset_id = 'email'
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
            bigquery.SchemaField('Date', 'DATE'),
            bigquery.SchemaField('Name', 'STRING'),
            bigquery.SchemaField('Campaign', 'STRING'),
            bigquery.SchemaField('Mailing', 'STRING'),
            bigquery.SchemaField('Variant', 'STRING'),
            bigquery.SchemaField('Subject_Line', 'STRING'),
            bigquery.SchemaField('Segment', 'STRING'),
            bigquery.SchemaField('Type_0', 'STRING'),
            bigquery.SchemaField('Type_1', 'STRING'),
            bigquery.SchemaField('Type_2', 'STRING'),
            bigquery.SchemaField('Offer', 'STRING'),
            bigquery.SchemaField('Segment_Engagement', 'STRING'),
            bigquery.SchemaField('Segment_BNB', 'STRING'),
            bigquery.SchemaField('Segment_Freq', 'STRING'),
            bigquery.SchemaField('Test_Type', 'STRING'),
            bigquery.SchemaField('Sent', 'FLOAT'),
            bigquery.SchemaField('Delivered', 'FLOAT'),
            bigquery.SchemaField('Opens', 'FLOAT'),
            bigquery.SchemaField('Clicks', 'FLOAT'),
            bigquery.SchemaField('Revenue', 'FLOAT'),
            bigquery.SchemaField('Orders', 'FLOAT'),
            bigquery.SchemaField('GA_Revenue', 'FLOAT'),
            bigquery.SchemaField('GA_Orders', 'FLOAT'),
            bigquery.SchemaField('GA_Sessions', 'FLOAT'),
            bigquery.SchemaField('AA_Visits', 'FLOAT'),
            bigquery.SchemaField('AA_Revenue', 'FLOAT'),
            bigquery.SchemaField('AA_SFCC_Demand', 'FLOAT'),
            bigquery.SchemaField('AA_Orders', 'FLOAT'),
            bigquery.SchemaField('AA_Mobile_Visits', 'FLOAT'),
            bigquery.SchemaField('Units', 'FLOAT'),
            bigquery.SchemaField('Product_Margin', 'FLOAT'),
            bigquery.SchemaField('Total_Margin', 'FLOAT'),
            bigquery.SchemaField('Unsub', 'FLOAT'),
            bigquery.SchemaField('Complaints', 'FLOAT'),
            bigquery.SchemaField('Hard_Bounces', 'FLOAT'),
            bigquery.SchemaField('Soft_Bounces', 'FLOAT'),
            bigquery.SchemaField('Total_Bounces', 'FLOAT'),
            bigquery.SchemaField('by_Variant', 'STRING'),
            bigquery.SchemaField('ESP', 'STRING'),
            bigquery.SchemaField('Send_Weekday', 'STRING'),
            bigquery.SchemaField('Total_Opens', 'STRING'),
            bigquery.SchemaField('Total_Clicks', 'STRING'),
            bigquery.SchemaField('Campaign_ID', 'STRING'),
            bigquery.SchemaField('Winning_Variant_question_', 'STRING'),
            bigquery.SchemaField('Flow_Message_ID', 'STRING'),
            bigquery.SchemaField('Flow_ID', 'STRING'),
            bigquery.SchemaField('Flow_Message_Name', 'STRING'),
            bigquery.SchemaField('Error', 'STRING'),
            bigquery.SchemaField('Original_Segment', 'STRING'),
            bigquery.SchemaField('FISCAL_YEAR', 'INTEGER'),
            bigquery.SchemaField('FISCAL_YEAR_START', 'DATE'),
            bigquery.SchemaField('FISCAL_WEEK', 'INTEGER'),
            bigquery.SchemaField('FISCAL_MONTH', 'INTEGER'),
            bigquery.SchemaField('FISCAL_QUARTER', 'INTEGER'),
                      
            ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    # get the URI for uploaded CSV in GCS from 'data'
    uri = 'gs://' + os.environ['BUCKET'] + '/' + client_id + '/new_master.csv'

    # lets do this
    load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(client_id),
            job_config=job_config)

    # Logs
    print('Starting job {}'.format(load_job.job_id))
    print('Function=csv_loader, Version=' + os.environ['VERSION'])
    #print('File: {}'.format(data['name']))

    load_job.result()  # wait for table load to complete.
    print('Job finished.')

    destination_table = client.get_table(dataset_ref.table(client_id))
    print('Loaded {} rows.'.format(destination_table.num_rows))

def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"
    
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

def alert_email(alert_message,client_id):
    alert = requests.get('https://subjectlinepro.com/sendmail?to=rroman@alchemyworx.com&subject='+ client_id + ' alert'+ '&key=c9ee2dfb-d1e6-4762-9984-de659a29f8d8&message='+ alert_message)
    print('email alert status: ',alert.status_code)