from base64 import encode
from doctest import master
from tarfile import ENCODING
from click import argument
import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
import datetime as dt
import requests


def update_masterlist(uri,client_id,client_esp):
    
    error_result = ''
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_type_df = pd.DataFrame()
    trigger_analytics_data_df = pd.DataFrame()
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    # Reading Campaign and Trigger Files
    list_file = main_module.list_blobs(client_id)
    for file in list_file:
        file_name = str(file.name).replace(client_id+'/','')
        if 'MessageActivity' in file_name:
            try:
                df = pd.read_csv(uri + file_name)
                campaign_data_df = campaign_data_df.append(df)
               
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed',client_id,client_esp)
                print(file_name,'failed to read excel')
                return
            
        elif 'ConversationActivity' in file_name:
            trigger_date = get_trigger_dates(file_name)
            try:
                df = pd.read_csv(uri + file_name)
                df['Publish Date (UTC-04)'] = trigger_date
                trigger_data_df = trigger_data_df.append(df)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed',client_id,client_esp)
                print(file_name,'failed to read excel')
                return

    # Reading Google Analytics
    try:
        campaign_analytics_data_df = pd.read_csv(uri + client_id +'-Campaign-Analytics.csv', skiprows=6)
    except:
        main_module.logs(dt_start,'Campaign-Analytics.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return
    try:
        trigger_analytics_data_df = pd.read_csv(uri + client_id +'-Trigger-Analytics.csv', skiprows=6) 
    except:
        main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return

    # Trigger Types
    try:
        trigger_type_df = pd.read_csv(uri + client_id +'-Trigger-Types.csv')
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Trigger-Types.csv File Not Found'
        main_module.logs(dt_start,'Trigger-Types.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)

    # Reading Old Campaign Files
    try:
        rev_order_df = pd.read_excel(uri + client_id + '-Old-Campaign-rev-order.xlsx',sheet_name='Dataset2')
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Old-Campaign-rev-order.xlsx File Not Found'
        main_module.logs(dt_start,'Old-Campaign-rev-order.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)
    try:
        session_df = pd.read_excel(uri + client_id + '-Old-Campaign-session.xlsx', sheet_name='Dataset2')
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Old-Campaign-session.xlsx File Not Found'
        main_module.logs(dt_start,'Old-Campaign-session.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)

    # Removing 0 Send in Trigger
    #trigger_data_df = trigger_data_df[trigger_data_df['Sent'] != 0]
    trigger_data_df['Sent'] = trigger_data_df['Sent'].astype(str)

    # For Reconciliation
    campaign_data_df['Delivered'] = campaign_data_df['Delivered'].str.replace(',','').astype(int) 
    trigger_data_df['Sent'] = trigger_data_df['Sent'].str.replace(',','').astype(int)
    campaign_data_df['Revenue'] = campaign_data_df['Revenue'].str.replace('$','').str.replace(',','').astype(float)
    trigger_data_df['Revenue'] = trigger_data_df['Revenue'].str.replace('$','').str.replace(',','').astype(float)
    initial_delivered = round(campaign_data_df['Delivered'].sum() + trigger_data_df['Sent'].sum(), 2) 
    initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum(), 2)
    
    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Test_Type','Offer','Campaign_ID','ESP','Sent','Delivered','Opens','Reads','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Sessions','GA_Orders','GA_Revenue','Visits','Pass_Along',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name"]
    
    campaign_data_df.to_csv(uri + 'campaign_data.csv', index=False)
    trigger_data_df.to_csv(uri + 'trigger_data.csv', index=False)
    campaign_data_df = pd.read_csv(uri + 'campaign_data.csv')

    # Campaign Transformation / Process
    main_campaign_df = campaign_transform(campaign_data_df,client_id,client_esp,client_name)
    main_campaign_df.to_csv(uri + 'test-new-masterlist.csv', index=False)
    # Combining Campaign with Google Analytics
    main_campaign_analytics_df = ga_transform(campaign_analytics_data_df,'Campaign-Analytics.csv',client_id,client_esp)
    main_campaign_masterlist_df = campaign_ga_join(main_campaign_df,main_campaign_analytics_df)
    main_campaign_masterlist_df = main_campaign_masterlist_df[all_cols].copy()
    
    main_campaign_masterlist_df.to_csv(uri + 'main_campaign_masterlist_df.csv', index=False)
   
    # Trigger Transformation / Joining Trigger Types
    
    main_trigger_df = Trigger_Transform(trigger_data_df,client_id,client_esp)
    main_trigger_analytics_df = ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
    main_trigger_df['temporary_name'] = main_trigger_df['Name']
    main_trigger_df['Name'] = main_trigger_df['Listrak_Conversion_Analytics_Module_Name']
    main_trigger_df = pd.merge(main_trigger_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
    main_trigger_df = main_trigger_df.drop(columns=['Campaign Name'])
    main_trigger_df['Name'] = main_trigger_df['temporary_name']
    main_trigger_df = main_trigger_df.drop_duplicates()
    main_trigger_masterlist_df = campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
    main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()
    main_trigger_masterlist_df.to_csv(uri + 'main_trigger_masterlist_df.csv', index=False)

    # Combining Campaign Data with Trigger Data
    main_masterlist_df = main_campaign_masterlist_df.append(main_trigger_masterlist_df)
    main_masterlist_df = main_masterlist_df[all_cols].copy()
    
    # For Reconciliation
    initial_rowcount = main_campaign_masterlist_df['Name'].count() + main_trigger_masterlist_df['Name'].count()
    final_rowcount = round(main_masterlist_df['Name'].count(), 2)
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
        print('Delivered Does not matched',initial_delivered,final_delivered)
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'not matched',client_id,client_esp)
        main_module.alert_email('Campaign + Trigger rows vs Masterlist Delivered Counts did not matched',client_id)
        return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'matched',client_id,client_esp)
    
    # Check Revenue
    if initial_revenue != final_revenue:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'not matched',client_id,client_esp)
        main_module.alert_email('Campaign + Trigger rows vs Masterlist Revenue Counts did not matched',client_id)
        #return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'matched',client_id,client_esp)

    # Backing up Dataframes
    str_cols = ['Name','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Test_Type','Offer','ESP','Original_Segment',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name",]
    int_cols = ['Sent','Delivered','Opens','Reads','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue',]
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_masterlist_df.to_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv', index=False)
    main_masterlist_df = pd.read_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv')
    masterlist_bigquery = main_module.get_table(client_id,str_cols,int_cols)
    masterlist_bigquery.to_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv', index=False)
    masterlist_bigquery = pd.read_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv')
    main_masterlist_df['Variant'] = main_masterlist_df['Variant'].replace(['',np.nan], "-", regex=True)
    
    # Updating Bigquery Dataframe
    main_masterlist_df['Date'] = pd.to_datetime(main_masterlist_df['Date']).dt.strftime('%Y-%m-%d')

    updated_masterlist = pd.concat([masterlist_bigquery,main_masterlist_df])
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name'], keep='last').sort_values('Date')
    dedup = updated_masterlist.loc[(updated_masterlist['Date'] >= '2022-09-01') & (updated_masterlist['ESP'] != 'Bouncex') ].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Google_Analytics_Campaign_Name'], keep='last').sort_values('Date')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < '2022-09-01') | (updated_masterlist['ESP'] == 'Bouncex')].append(dedup)
    
    # Adding Old Campaign

    old_campaign_data = process_old_campaign(uri,client_id,updated_masterlist,rev_order_df,session_df)
    updated_masterlist = pd.concat([updated_masterlist,old_campaign_data])

    # Updating Old record by removing Duplicates
    deduped = updated_masterlist.loc[(updated_masterlist['Date'] >= '2022-09-01') & (updated_masterlist['ESP'] != 'Bouncex') ].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Google_Analytics_Campaign_Name'], keep='last').sort_values('Date')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < '2022-09-01') | (updated_masterlist['ESP'] == 'Bouncex')].append(deduped)
    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','Name_Bkp',]
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    for i in missing_str_cols:
        updated_masterlist[i] = '-'
    for i in missing_int_cols:
        updated_masterlist[i] = 0
    updated_masterlist['Original_Segment'] = updated_masterlist['Segment']
    cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","Name_Bkp","ESP","Reads","Total_Clicks","Visits","Pass_Along","Google_Analytics_Campaign_Name","Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name","Error","Original_Segment","FISCAL_YEAR"]
    updated_masterlist = updated_masterlist[cols_all].copy()

    # Adding Fiscals

    updated_masterlist['FISCAL_YEAR'] = pd.to_datetime(updated_masterlist['Date']).dt.year
    #d = datetime(updated_masterlist['FISCAL_YEAR'], int(1), 7)
    #updated_masterlist['FISCAL_YEAR_START'] = d + timedelta(-d.isoweekday())
    #for l, i in enumerate(updated_masterlist['FISCAL_YEAR']):
        #d = datetime(i, int(1), 7)
        #k=d + timedelta(-d.isoweekday())
        #updated_masterlist.loc[l, 'FISCAL_YEAR_START']= k
    #new_master['FISCAL_YEAR_START'] = new_master['Date']
    updated_masterlist['FISCAL_YEAR_START'] = updated_masterlist['Date']
    updated_masterlist['FISCAL_WEEK'] = pd.to_datetime(updated_masterlist['Date']).dt.isocalendar().week
    updated_masterlist['FISCAL_MONTH'] = pd.to_datetime(updated_masterlist['Date']).dt.month
    updated_masterlist['FISCAL_QUARTER'] = pd.to_datetime(updated_masterlist['Date']).dt.quarter

    updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)

    dbx_rename = {"Date":"Send Date (UTC-04)","Name":"Campaign","Campaign":"By Campaign","Mailing":"By Mailing","Variant":"By Variant","Subject_Line":"Subject","ESP":"ESP","Type_0":"Type 0","Type_1":"Type","Type_2":"Type 2","Sent":"Sent","Delivered":"Delivered","Total_Bounces":"Total Bounces","Unsub":"Unsubs.","Opens":"Opens","Reads":"Reads","Total_Clicks":"Clicks","Clicks":"Unique Clickers","Revenue":"Revenue","Visits":"Visits","Orders":"Conversions","Pass_Along":"Pass Along","Google_Analytics_Campaign_Name":"Google Analytics Campaign Name","Google_Analytics_Campaign_Content":"Google Analytics Campaign Content","Listrak_Conversion_Analytics_Campaign_Name":"Listrak Conversion Analytics Campaign Name","Listrak_Conversion_Analytics_Version":"Listrak Conversion Analytics Version","Listrak_Conversion_Analytics_Module_Name":"Listrak Conversion Analytics Module Name","GA_Revenue":"GA Revenue","GA_Orders":"GA Order","GA_Sessions":"GA Session"}
    dbx_schema = ["Send Date (UTC-04)","Campaign","Name","By Campaign","By Mailing","By Variant","Subject","ESP","Type 0","Type","Type 2","Sent","Delivered","Total Bounces","Unsubs.","Opens","Reads","Clicks","Unique Clickers","Revenue","Visits","Conversions","Pass Along","Google Analytics Campaign Name","Google Analytics Campaign Content","Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version","Listrak Conversion Analytics Module Name","GA Revenue","GA Order","GA Session"]
    dbx_masterlist = updated_masterlist
    dbx_masterlist.rename(columns=dbx_rename, inplace=True)
    #dbx_masterlist['Variant'] = dbx_masterlist['Variant'].replace('-','n/a')
    dbx_masterlist['Name'] = dbx_masterlist['By Campaign']
    dbx_masterlist = dbx_masterlist[dbx_schema].copy()
    if os.environ['USEDROPBOX'] == 'yes':
        main_module.to_dropbox(dbx_masterlist,dbx_path,client_name)

    # Updating to BigQuery
    main_module.delete_table(client_id)
    bigquery_insert(client_id)

    # Archiving Source Files
    files_list = [client_id+'-Klaviyo-Campaigns.csv', client_id+'-Klaviyo-Flows.csv', client_id+'-Campaign-Analytics.csv', client_id+'-Trigger-Analytics.csv', client_id+'-Trigger-Types.csv']
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    
    #files_list = main_module.list_blobs(client_id)
    #for file in files_list:
        #file_name_only = str(file.name).replace(client_id+'/','')
        #main_module.copy_blob(os.environ['BUCKET'],file.name,os.environ['BUCKET'],client_id + '/' +'archive/' + archive_datetime +' '+ file_name_only)

    return(error_result)
    
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
    
    # Constant value
    
    df['Total_Opens'] = df['Opens']
    df['Complaints'] = ''

    df['Segment'] = df['Listrak Conversion Analytics Version']
    df['Variant'] = df['Listrak Conversion Analytics Campaign Name']
    df['Campaign_ID'] = df['Google Analytics Campaign Name']

    # Rename columns to Default Column Names
    cols_include = {
        "Campaign" : "Name",
        "Subject" : "Subject_Line",
        "Send Date (UTC-04)" : "Date",
        "Sent" : "Sent",
        "Delivered" : "Delivered",
        "Bounces" : "Total_Bounces",
        "Unsubs." : "Unsub",
        "Opens" : "Opens",
        "Reads" : "Reads",
        "Clicks" : "Total_Clicks",
        "Unique Clickers" : "Clicks",
        "Revenue" : "Revenue",
        "Conversions" : "Orders",
        "Visits" : "Visits",
        "Pass Along" : "Pass_Along",
        "Google Analytics Campaign Name" : "Google_Analytics_Campaign_Name",
        "Google Analytics Campaign Content" : "Google_Analytics_Campaign_Content",
        "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
        "Listrak Conversion Analytics Version" : "Listrak_Conversion_Analytics_Version",
        "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
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
        'Reads',
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
    df[metric_columns] = df[metric_columns].replace('|'.join([',', '\$']), '',regex=True)
    for col in metric_columns:
        #df[col] = df[col].str.replace(',', '').str.replace('$', '')
        df[col] = pd.to_numeric(df[col])
    
    # Add "By Mailing" column
    df['Mailing'] = df['Name'].str.split("(", n = 1, expand = True)[0].str.rstrip()
        
    df['Campaign'] = df['Mailing'].str.split("-", n = 1, expand = True)[1].str.rstrip()
    # Add "By Campaign" column
    #df['Campaign'] = df['Campaign'].str.split('*')[0]
    
    # Format 
    date_split = df['Date'].str.split(" ", n = 1, expand = True)
    df['Date'] = date_split[0].astype('datetime64')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%m/%d/%Y')
    
    mailing_types = main_module.get_mailing_types(client_id,client_name)
    if mailing_types.empty:
        # Constant value
        df['ESP'] = 'Listrak'
        df['Type_0'] = 'Campaign'
        df['Type_1'] = 'Promo'
        df['Type_2'] = '-'
        df['Type_3'] = '-'
        df['Test_Type'] = '-'
        df['Offer'] = '-'
        print('no mailing types')
    else:
        df['ESP'] = 'Listrak'
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

    str_cols = ['Date','Name','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','Campaign_ID','ESP',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name",]
    df[str_cols] = df[str_cols].replace(np.nan, '-', regex=True)
    df[str_cols] = df[str_cols].replace('', '-', regex=True)

    df.loc[df["Listrak_Conversion_Analytics_Version"].str.contains('-B|-b'),'Type_2'] = 'Buyer'
    df.loc[df["Listrak_Conversion_Analytics_Version"].str.contains('-NB|-nb'),'Type_2'] = 'Non Buyer'
    df.loc[df["Name"].str.contains('Flash|flash'),'Type_1'] = 'Flash'
    #df.loc[df["Name"].str.contains('In-Store|-in-store'),'Type_2'] = 'Other'
    
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
        'Reads',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue',
        'Visits',
        'Pass_Along',
        'Google_Analytics_Campaign_Name', 
        'Google_Analytics_Campaign_Content', 
        'Listrak_Conversion_Analytics_Campaign_Name', 
        'Listrak_Conversion_Analytics_Version',
        'Listrak_Conversion_Analytics_Module_Name',
        ]].copy()

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
    #df['Campaign Regex'] = df['Campaign_ID'].str.extract('(\(.*?\))')[0].str.strip('()')
    #df['Campaign Regex'].fillna(df['Campaign_ID'], inplace=True)
    #df['Campaign_ID'] = df['Campaign Regex']
    
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

def Trigger_Transform(trigger_data_df,client_id,client_esp):
    df = trigger_data_df
    #df = df[df['Sent'] != 0]
    df['Test_Type'] = '-'
    df['Offer'] = '-'
    
    rename_col ={
        "Publish Date (UTC-04)": "Date",
        "Thread / Step" : "Name",
        "Subject" : "Subject_Line",
        "Sent" : "Sent",
        "Bounces" : "Total_Bounces",
        "Unsubs." : "Unsub",
        "Opens" : "Opens",
        "Reads" : "Reads",
        "Clicks" : "Total_Clicks",
        "Clickers" : "Clicks",
        "Revenue" : "Revenue",
        "Conversions" : "Orders",
        "Visits" : "Visits",
        "Pass Along" : "Pass_Along",
        "Google Analytics Campaign Name" : "Google_Analytics_Campaign_Name",
        "Google Analytics Campaign Content" : "Google_Analytics_Campaign_Content",
        "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
        "Listrak Conversion Analytics Version" : "Listrak_Conversion_Analytics_Version",
        "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",

        }
    # Rename columns to Default Column Names
    df.rename(
        columns=rename_col,
        inplace=True)
    df['Delivered'] = df['Sent']

    # For Logging / Reconciliation
    initial_rowcount = str(df['Name'].count())
    initial_delivered = df['Delivered'].sum()
    initial_revenue = df['Revenue'].sum()
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")

    # Including Null Columns
    cols_exclude = {
        'Campaign': '-',
        'Mailing': '-',
        'Variant': '-',
        'Total_Opens': '0',
        'Segment': '-',
        'Complaints': '0',
        }
    for i in cols_exclude:
        df[i] = cols_exclude[i]

    metric_columns = [
    'Sent',
    'Delivered',
    'Opens',
    'Reads',
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

    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%m/%d/%Y')
    
    # Filtering Rows with Delivered Value
    

    df['Campaign_ID'] = df['Google_Analytics_Campaign_Name']
    
    str_cols = ['Date','Name','Variant','Subject_Line','Segment','Test_Type','Offer','Campaign_ID','Visits','Pass_Along',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name",]
    df[str_cols] = df[str_cols].replace(np.nan, '-', regex=True)
    df[str_cols] = df[str_cols].replace('', '-', regex=True)
    
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
        'Reads',
        'Clicks',
        'Total_Clicks',
        'Unsub',
        'Complaints',
        'Total_Bounces',
        'Orders',
        'Revenue',
        'Visits',
        'Pass_Along',
        'Google_Analytics_Campaign_Name', 
        'Google_Analytics_Campaign_Content', 
        'Listrak_Conversion_Analytics_Campaign_Name', 
        'Listrak_Conversion_Analytics_Version',
        'Listrak_Conversion_Analytics_Module_Name',
        ]].copy()

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

def process_old_campaign(uri,client_id,updated_masterlist,rev_order_df,session_df):
    # Reading and combining old campaign files 
    old_campaign_df = read_old_campaign(uri,client_id,rev_order_df,session_df)

    # Filtering Out Existing Old Campaign from Updated Masterlist
    updated_masterlist = updated_masterlist[updated_masterlist['Type_1'] != 'Old Campaign']
    updated_masterlist = updated_masterlist[updated_masterlist['ESP'] != 'Bouncex']
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
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Test_Type','Offer','ESP',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name"]
    int_cols = ['Sent','Delivered','Opens','Reads','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','Visits','Pass_Along']
    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Test_Type','Offer','ESP','Sent','Delivered','Opens','Reads','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Sessions','GA_Orders','GA_Revenue','Visits','Pass_Along',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name"]
    
    for i in str_cols:
        old_campaign[i] = '-'
    for i in int_cols:
        old_campaign[i] = 0

    old_campaign = old_campaign[all_cols].copy()

    return(old_campaign)

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

    # Format Date
    df['Date'] = df['Date'].astype('datetime64')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    df = df[~df.Date.isnull()]
    
    return(df)

def alert_email(alert_message,client_id):
    alert = requests.get('https://subjectlinepro.com/sendmail?to=rroman@alchemyworx.com&subject='+ client_id + ' alert'+ '&key=c9ee2dfb-d1e6-4762-9984-de659a29f8d8&message='+ alert_message)
    print('email alert status: ',alert.status_code)

def get_trigger_dates(file_name):
    temp_file_name = file_name.split('_')
    temp_first_date = temp_file_name[-1]
    first_date = temp_first_date[4:6] + '/' + temp_first_date[6:8] + '/' + temp_first_date[0:4]
    return(first_date)


def update_bigquery(uri,client_id,client_esp):
    main_module = __import__('main')
    esp_module = __import__(client_esp)
    error_result = ''

    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M:%S")

    try:
        new_master = pd.read_excel(uri + client_id +'-reporter-masterlist.xlsx',sheet_name='Data')
        
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + str(e)
        main_module.logs(dt_start,'reporter-masterlist.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)

    rename_cols = {
        "Send Date (UTC-04)":"Date",
        "Campaign":"Name",
        "By Campaign":"Campaign",
        "By Mailing":"Mailing",
        "By Variant":"Variant",
        "Subject":"Subject_Line",
        "ESP":"ESP",
        "Type 0":"Type_0",
        "Type":"Type_1",
        "Type 2":"Type_2",
        "Sent":"Sent",
        "Delivered":"Delivered",
        "Total Bounces":"Total_Bounces",
        "Unsubs.":"Unsub",
        "Opens":"Opens",
        "Reads":"Reads",
        "Clicks":"Total_Clicks",
        "Unique Clickers":"Clicks",
        "Revenue":"Revenue",
        "Visits":"Visits",
        "Conversions":"Orders",
        "Pass Along":"Pass_Along",
        "Google Analytics Campaign Name":"Google_Analytics_Campaign_Name",
        "Google Analytics Campaign Content":"Google_Analytics_Campaign_Content",
        "Listrak Conversion Analytics Campaign Name":"Listrak_Conversion_Analytics_Campaign_Name",
        "Listrak Conversion Analytics Version":"Listrak_Conversion_Analytics_Version",
        "Listrak Conversion Analytics Module Name":"Listrak_Conversion_Analytics_Module_Name",
        "GA Revenue":"GA_Revenue",
        "GA Order":"GA_Orders",
        "GA Session":"GA_Sessions",

        }
    new_master.rename(
        columns=rename_cols,
        inplace=True)
    
    new_master['Date'] = pd.to_datetime(new_master['Date']).dt.strftime('%Y-%m-%d')
    new_master['Segment'] = new_master['Listrak_Conversion_Analytics_Version']
    new_master['Original_Segment'] = new_master['Segment']
    
    str_cols = ['Name','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','ESP','Original_Segment',"Google_Analytics_Campaign_Name", "Google_Analytics_Campaign_Content", "Listrak_Conversion_Analytics_Campaign_Name", "Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name",]
    int_cols = ['Sent','Delivered','Opens','Reads','Clicks','Total_Clicks','Unsub','Total_Bounces','Orders','Revenue',]
    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','Name_Bkp','Test_Type','Offer']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','Complaints','FISCAL_YEAR']
    #all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Offer','Segment_Engagement','Segment_BNB','Segment_Freq','Test_Type','Sent','Delivered','Opens','Clicks','Revenue','Orders','GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Unsub','Complaints','Hard_Bounces','Soft_Bounces','Total_Bounces','ESP','Type_3','Send_Weekday','Total_Opens','Total_Clicks','Campaign_ID','Winning_Variant_question_','Flow_Message_ID','Flow_ID','Flow_Message_Name','Error','Original_Segment','FISCAL_YEAR']
    all_cols = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","Name_Bkp","ESP","Reads","Total_Clicks","Visits","Pass_Along","Google_Analytics_Campaign_Name","Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name","Error","Original_Segment","FISCAL_YEAR"]
    new_master[int_cols] = new_master[int_cols].replace('|'.join([',', '\$']), 0, regex=True)
    for i in missing_str_cols:
        new_master[i] = '-'
    for i in missing_int_cols:
        new_master[i] = 0
    for i in str_cols:
        new_master[i] = new_master[i].replace(np.nan, '-', regex=True)
        new_master[i] = new_master[i].replace('', '-', regex=True)
    for i in int_cols:
        new_master[i] = new_master[i].replace(np.nan, 0, regex=True)
        new_master[i] = new_master[i].replace('', 0, regex=True)
    print('test 1')
    #df_trigger = new_master[new_master['Type_0'] != 'Campaign']
    #df_campaign = new_master[new_master['Type_0'] == 'Campaign']
    #df_trigger['Campaign'] = df_trigger['Name']
    #df_trigger['Mailing'] = df_trigger['Name']
    #new_master = df_campaign.append(df_trigger)

    new_master = new_master[all_cols].copy()
    print('test 2')
    new_master['FISCAL_YEAR'] = pd.to_datetime(new_master['Date']).dt.year
    #d = datetime(updated_masterlist['FISCAL_YEAR'], int(1), 7)
    #updated_masterlist['FISCAL_YEAR_START'] = d + timedelta(-d.isoweekday())
    #for l, i in enumerate(updated_masterlist['FISCAL_YEAR']):
        #d = datetime(i, int(1), 7)
        #k=d + timedelta(-d.isoweekday())
        #updated_masterlist.loc[l, 'FISCAL_YEAR_START']= k
    #new_master['FISCAL_YEAR_START'] = new_master['Date']
    new_master['FISCAL_YEAR_START'] = new_master['Date']
    new_master['FISCAL_WEEK'] = pd.to_datetime(new_master['Date']).dt.isocalendar().week
    new_master['FISCAL_MONTH'] = pd.to_datetime(new_master['Date']).dt.month
    new_master['FISCAL_QUARTER'] = pd.to_datetime(new_master['Date']).dt.quarter
    
    new_master.to_csv(uri + 'new_master.csv',index=False)
    
    dbx_rename = {"Date":"Send Date (UTC-04)","Name":"Campaign","Campaign":"By Campaign","Mailing":"By Mailing","Variant":"By Variant","Subject_Line":"Subject","ESP":"ESP","Type_0":"Type 0","Type_1":"Type","Type_2":"Type 2","Sent":"Sent","Delivered":"Delivered","Total_Bounces":"Total Bounces","Unsub":"Unsubs.","Opens":"Opens","Reads":"Reads","Total_Clicks":"Clicks","Clicks":"Unique Clickers","Revenue":"Revenue","Visits":"Visits","Orders":"Conversions","Pass_Along":"Pass Along","Google_Analytics_Campaign_Name":"Google Analytics Campaign Name","Google_Analytics_Campaign_Content":"Google Analytics Campaign Content","Listrak_Conversion_Analytics_Campaign_Name":"Listrak Conversion Analytics Campaign Name","Listrak_Conversion_Analytics_Version":"Listrak Conversion Analytics Version","Listrak_Conversion_Analytics_Module_Name":"Listrak Conversion Analytics Module Name","GA_Revenue":"GA Revenue","GA_Orders":"GA Order","GA_Sessions":"GA Session"}
    dbx_schema = ["Send Date (UTC-04)","Campaign","By Campaign","By Mailing","By Variant","Subject","ESP","Type 0","Type","Type 2","Sent","Delivered","Total Bounces","Unsubs.","Opens","Reads","Clicks","Unique Clickers","Revenue","Visits","Conversions","Pass Along","Google Analytics Campaign Name","Google Analytics Campaign Content","Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version","Listrak Conversion Analytics Module Name","GA Revenue","GA Order","GA Session"]
    dbx_masterlist = new_master
    dbx_masterlist.rename(columns=dbx_rename, inplace=True)
    #dbx_masterlist['Variant'] = dbx_masterlist['Variant'].replace('-','n/a')
    dbx_masterlist['Name'] = dbx_masterlist['By Campaign']
    dbx_masterlist = dbx_masterlist[dbx_schema].copy()
    if os.environ['USEDROPBOX'] == 'yes':
        main_module.to_dropbox(dbx_masterlist,dbx_path,client_name)
    
    # Backing BigQuery Up
    masterlist_bigquery = main_module.get_table(client_id,str_cols,int_cols)
    masterlist_bigquery.to_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv', index=False)
    
    # Updating BigQuery
    main_module.delete_table(client_id)
    bigquery_reinsert(client_id)

    return(error_result)


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
        bigquery.SchemaField('Name_Bkp', 'STRING'),
        bigquery.SchemaField('ESP', 'STRING'),
        bigquery.SchemaField('Reads', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Visits', 'STRING'),
        bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Module_Name', 'STRING'),
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
        bigquery.SchemaField('Name_Bkp', 'STRING'),
        bigquery.SchemaField('ESP', 'STRING'),
        bigquery.SchemaField('Reads', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Visits', 'STRING'),
        bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Module_Name', 'STRING'),
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

    try:
        load_job.result()  # wait for table load to complete.
        print('Job finished.')
    except Exception as e:
        print(e)
    destination_table = client.get_table(dataset_ref.table(client_id))
    print('Loaded {} rows.'.format(destination_table.num_rows))
