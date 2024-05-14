from base64 import encode
from doctest import master
from tarfile import ENCODING
import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
import datetime as dt


client_name = 'Felina'
dbx_path = '/Client/Felina/Internal Files/Reporting/'

def update_masterlist(uri,client_id,client_esp):
    
    main_module = __import__('main')
    esp_module = __import__(client_esp)
    error_result = ''
    

    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_type_df = pd.DataFrame()
    trigger_analytics_data_df = pd.DataFrame()
    trigger_data_df_2 = pd.DataFrame()
    trigger_analytics_data_df_2 = pd.DataFrame()
    rev_order_df = pd.DataFrame()
    session_df = pd.DataFrame()
    
    # Reading Campaign Files
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    try:
        campaign_data_df = pd.read_csv(uri + client_id +'-Klaviyo-Campaigns.csv')
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Klaviyo-Campaigns.csv File Not Found'
        main_module.logs(dt_start, 'Klaviyo-Campaigns.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return(error_result)

    try:
        campaign_analytics_data_df = pd.read_csv(uri + client_id +'-Campaign-Analytics.csv', skiprows=6)
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Campaign-Analytics.csv Not Found'
        main_module.logs(dt_start,'Campaign-Analytics.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return(error_result)


    # Reading Trigger Files
    try:
        trigger_data_df = pd.read_csv(uri + client_id +'-Klaviyo-Flows.csv',skiprows=5)
        trigger_data_df = trigger_data_df[~trigger_data_df['Flow Name'].str.contains("wknd-")]
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Klaviyo-Flows.csv File Not Found'
        main_module.logs(dt_start,'Klaviyo-Flows.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)


    try:
        trigger_analytics_data_df = pd.read_csv(uri + client_id +'-Trigger-Analytics.csv', skiprows=6) 
        trigger_analytics_data_df = trigger_analytics_data_df[['Campaign','Users','New Users','Sessions','Bounce Rate','Pages / Session','Avg. Session Duration','Ecommerce Conversion Rate','Transactions','Revenue']].copy()
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Trigger-Analytics.csv File Not Found'
        main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)

    # Trigger Half
    try:
        trigger_data_df_2 = pd.read_csv(uri + client_id +'-Klaviyo-Flows_2.csv',skiprows=5) 
        
    except Exception as e:
        print(e)


    try:
        trigger_analytics_data_df_2 = pd.read_csv(uri + client_id +'-Trigger-Analytics_2.csv', skiprows=6) 
        trigger_analytics_data_df_2 = trigger_analytics_data_df_2[['Campaign','Users','New Users','Sessions','Bounce Rate','Pages / Session','Avg. Session Duration','Ecommerce Conversion Rate','Transactions','Revenue']].copy()
    except Exception as e:
        print(e)

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

    # For Reconciliation
    if not trigger_data_df_2.empty:
        initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum() + trigger_data_df_2['Delivered'].sum(), 2) 
        initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum() + trigger_data_df_2['Revenue'].sum(), 2)    
    else:
        initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum(), 2) 
        initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum(), 2) 

    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','Campaign_ID','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Sessions','GA_Orders','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question_']
     
    # Campaign Transformation / Process
    
    main_campaign_df = esp_module.campaign_transform(campaign_data_df,client_id,client_esp,client_name)
    
    main_campaign_analytics_df = esp_module.ga_transform(campaign_analytics_data_df,'Campaign-Analytics.csv',client_id,client_esp)
    main_campaign_masterlist_df = esp_module.campaign_ga_join(main_campaign_df,main_campaign_analytics_df)
    main_campaign_masterlist_df = main_campaign_masterlist_df[all_cols].copy()
    
    # Trigger Transformation / Joining Trigger Types
    trigger_name = 'Flow Name'
    trigger_flow_name =  'Flow Message Name'

    if trigger_data_df_2.empty:
        main_trigger_df = esp_module.Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name)
        main_trigger_analytics_df = esp_module.ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
        main_trigger_df = pd.merge(main_trigger_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_df = main_trigger_df.drop(columns=['Campaign Name'])
        main_trigger_masterlist_df = esp_module.campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
        main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()
        print(main_trigger_masterlist_df)
    else:
        main_trigger_df = esp_module.Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name)
        main_trigger_analytics_df = esp_module.ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
        main_trigger_df = pd.merge(main_trigger_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_df = main_trigger_df.drop(columns=['Campaign Name'])
        main_trigger_masterlist_df = esp_module.campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
        main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()

        main_trigger_df_2 = esp_module.Trigger_Transform(trigger_data_df_2,client_id,client_esp,trigger_name,trigger_flow_name)
        main_trigger_analytics_df_2 = esp_module.ga_transform(trigger_analytics_data_df_2,'Trigger-Analytics_2.csv',client_id,client_esp)
        main_trigger_df_2 = pd.merge(main_trigger_df_2,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_df_2 = main_trigger_df_2.drop(columns=['Campaign Name'])
        main_trigger_masterlist_df_2 = esp_module.campaign_ga_join(main_trigger_df_2,main_trigger_analytics_df_2)
        main_trigger_masterlist_df_2 = main_trigger_masterlist_df_2[all_cols].copy()
    
    if trigger_data_df_2.empty:
        main_masterlist_df = main_campaign_masterlist_df.append(main_trigger_masterlist_df)

    else:
        main_masterlist_df = main_campaign_masterlist_df.append(main_trigger_masterlist_df)
        main_masterlist_df = main_masterlist_df.append(main_trigger_masterlist_df_2)
        
    main_masterlist_df = main_masterlist_df[all_cols].copy()

    # For Reconciliation

    if trigger_data_df_2.empty:
        initial_rowcount = main_campaign_masterlist_df['Name'].count() + main_trigger_masterlist_df['Name'].count()
    else:
        initial_rowcount = main_campaign_masterlist_df['Name'].count() + main_trigger_masterlist_df['Name'].count() + main_trigger_masterlist_df_2['Name'].count()
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
        print('test -------',initial_delivered,final_delivered)
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
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday',]
    int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Revenue','GA_Orders','GA_Sessions']
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_masterlist_df.to_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv', index=False)
    main_masterlist_df = pd.read_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv')
    masterlist_bigquery = main_module.get_table(client_id,str_cols,int_cols)
    masterlist_bigquery.to_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv', index=False)
    masterlist_bigquery = pd.read_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv')
    main_masterlist_df['Variant'] = main_masterlist_df['Variant'].replace('', "-", regex=True)
    main_masterlist_df['Variant'] = main_masterlist_df['Variant'].replace(np.nan, "-", regex=True)

    # Updating Bigquery Dataframe
    updated_masterlist = pd.concat([masterlist_bigquery,main_masterlist_df])
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name'], keep='last').sort_values('Date')
    dedup = updated_masterlist.loc[(updated_masterlist['Date'] >= '2022-01-01')].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name','Flow_Message_ID'], keep='last').sort_values('Date')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < '2022-01-01')].append(dedup)
    
    # Adding Old Campaign
    
    old_campaign_data = main_module.process_old_campaign(uri,client_id,updated_masterlist,rev_order_df,session_df)
    updated_masterlist = pd.concat([updated_masterlist,old_campaign_data])
    
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name'], keep='last').sort_values('Date')
    deduped = updated_masterlist.loc[(updated_masterlist['Date'] >= '2022-01-01')].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name','Flow_Message_ID'], keep='last').sort_values('Date')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < '2022-01-01')].append(deduped)

    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','by_Variant']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    for i in missing_str_cols:
        updated_masterlist[i] = '-'
    for i in missing_int_cols:
        updated_masterlist[i] = 0
    updated_masterlist['Original_Segment'] = updated_masterlist['Segment']

    cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","Send_Weekday","ESP","Total_Opens","Total_Clicks","Campaign_ID","Flow_Message_Name","Flow_Message_ID","Flow_ID","Error","Original_Segment","FISCAL_YEAR"]
    
    updated_masterlist = updated_masterlist[cols_all]
    
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

    dbx_rename = {"Name":"Name","Campaign":"Campaign","Mailing":"Mailing","Variant":"Variant","Offer":"Offer","Test_Type":"Test Type","Subject_Line":"Subject Line","Segment":"Segment","Date":"Date","Send_Weekday":"Send Weekday","ESP":"ESP","Type_0":"Type 0","Type_1":"Type 1","Type_2":"Type 2","Sent":"Sent","Orders":"Orders","Revenue":"Revenue","Opens":"Opens","Total_Opens":"Total Opens","Clicks":"Clicks","Total_Clicks":"Total Clicks","Unsub":"Unsub","Complaints":"Complaints","Delivered":"Delivered","Total_Bounces":"Total Bounces","Campaign_ID":"Campaign ID","Flow_Message_Name":"Flow Message Name","Flow_Message_ID":"Flow Message ID","Flow_ID":"Flow ID","GA_Revenue":"GA Revenue","GA_Orders":"GA Orders","GA_Sessions":"GA Sessions"}
    dbx_schema = ["Name","Campaign","Mailing","Variant","Test Type","Offer","Subject Line","Segment","Date","Send Weekday","ESP","Type 0","Type 1","Type 2","Sent","Orders","Revenue","Opens","Total Opens","Clicks","Total Clicks","Unsub","Complaints","Delivered","Total Bounces","Campaign ID","Flow Message Name","Flow Message ID","Flow ID","GA Revenue","GA Orders","GA Sessions"]
    dbx_masterlist = updated_masterlist
    dbx_masterlist.rename(columns=dbx_rename, inplace=True)
    dbx_masterlist = dbx_masterlist[dbx_schema].copy()
    
    if os.environ['USEDROPBOX'] == 'yes':
        main_module.to_dropbox(dbx_masterlist,dbx_path,client_name)
    
    updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)

    # Updating to BigQuery
    main_module.delete_table(client_id)
    #esp_module.bigquery_insert(client_id)
    bigquery_insert(client_id)

    # Archiving Source Files
    files_list = [client_id+'-Klaviyo-Campaigns.csv', client_id+'-Klaviyo-Flows.csv', client_id+'-Campaign-Analytics.csv', client_id+'-Trigger-Analytics.csv', client_id+'-Trigger-Types.csv']
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    for file_name in files_list:
        main_module.copy_blob('reporter-etl',client_id + '/' + file_name,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +' '+ file_name)
    
    return(error_result)


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
        "Name":"Name",
        "Campaign":"Campaign",
        "Mailing":"Mailing",
        "Variant":"Variant",
        "Test Type": "Test_Type",
        "Offer": "Offer",
        "Subject Line":"Subject_Line",
        "Segment":"Segment",
        "Date":"Date",
        "Send Weekday":"Send_Weekday",
        "ESP":"ESP",
        "Type 0":"Type_0",
        "Type 1":"Type_1",
        "Type 2":"Type_2",
        "Sent":"Sent",
        "Orders":"Orders",
        "Revenue":"Revenue",
        "Opens":"Opens",
        "Total Opens":"Total_Opens",
        "Clicks":"Clicks",
        "Total Clicks":"Total_Clicks",
        "Unsub":"Unsub",
        "Complaints":"Complaints",
        "Delivered":"Delivered",
        "Total Bounces":"Total_Bounces",
        "Campaign ID":"Campaign_ID",
        "Flow Message Name":"Flow_Message_Name",
        "Flow Message ID":"Flow_Message_ID",
        "Flow ID":"Flow_ID",
        "GA Revenue":"GA_Revenue",
        "GA Orders":"GA_Orders",
        "GA Sessions":"GA_Sessions",
        }
    new_master.rename(
        columns=rename_cols,
        inplace=True)
    
    new_master['Date'] = pd.to_datetime(new_master['Date']).dt.strftime('%Y-%m-%d')
    new_master['Campaign_ID'] = new_master['Campaign_ID'].str.lower()
    new_master['Type_3'] = '-'
    new_master['Original_Segment'] = new_master['Segment']
    
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_1','Type_2','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Test_Type','Offer']
    int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Revenue','GA_Orders','GA_Sessions']
    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','Winning_Variant_question_']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    #all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','campaign_id','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Bounces','Order','Revenue','GA_Sessions','GA_Order','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question']
    all_cols = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","Send_Weekday","ESP","Total_Opens","Total_Clicks","Campaign_ID","Flow_Message_Name","Flow_Message_ID","Flow_ID","Error","Original_Segment","FISCAL_YEAR"]
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
    
    df_trigger = new_master[new_master['Type_0'] != 'Campaign']
    df_campaign = new_master[new_master['Type_0'] == 'Campaign']
    df_trigger['Campaign'] = df_trigger['Name']
    df_trigger['Mailing'] = df_trigger['Name']
    new_master = df_campaign.append(df_trigger)

    new_master = new_master[all_cols].copy()
    new_master['FISCAL_YEAR'] = pd.to_datetime(new_master['Date']).dt.year
    for l, i in enumerate(new_master['FISCAL_YEAR']):
        d = datetime(i, int(1), 7)
        k=d + timedelta(-d.isoweekday())
        new_master.loc[l, 'FISCAL_YEAR_START']= k
    #new_master['FISCAL_YEAR_START'] = new_master['Date']
    new_master['FISCAL_WEEK'] = pd.to_datetime(new_master['Date']).dt.isocalendar().week
    new_master['FISCAL_MONTH'] = pd.to_datetime(new_master['Date']).dt.month
    new_master['FISCAL_QUARTER'] = pd.to_datetime(new_master['Date']).dt.quarter
    
    new_master.to_csv(uri + 'new_master.csv',index=False)
    
    dbx_rename = {"Name":"Name","Campaign":"Campaign","Mailing":"Mailing","Variant":"Variant","Offer":"Offer","Test_Type":"Test Type","Subject_Line":"Subject Line","Segment":"Segment","Date":"Date","Send_Weekday":"Send Weekday","ESP":"ESP","Type_0":"Type 0","Type_1":"Type 1","Type_2":"Type 2","Sent":"Sent","Orders":"Orders","Revenue":"Revenue","Opens":"Opens","Total_Opens":"Total Opens","Clicks":"Clicks","Total_Clicks":"Total Clicks","Unsub":"Unsub","Complaints":"Complaints","Delivered":"Delivered","Total_Bounces":"Total Bounces","Campaign_ID":"Campaign ID","Flow_Message_Name":"Flow Message Name","Flow_Message_ID":"Flow Message ID","Flow_ID":"Flow ID","GA_Revenue":"GA Revenue","GA_Orders":"GA Orders","GA_Sessions":"GA Sessions"}
    dbx_schema = ["Name","Campaign","Mailing","Variant","Test Type","Offer","Subject Line","Segment","Date","Send Weekday","ESP","Type 0","Type 1","Type 2","Sent","Orders","Revenue","Opens","Total Opens","Clicks","Total Clicks","Unsub","Complaints","Delivered","Total Bounces","Campaign ID","Flow Message Name","Flow Message ID","Flow ID","GA Revenue","GA Orders","GA Sessions"]
    dbx_masterlist = new_master
    dbx_masterlist.rename(columns=dbx_rename, inplace=True)
    #dbx_masterlist['Variant'] = dbx_masterlist['Variant'].replace('-','n/a')
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
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Name", "STRING"),
        bigquery.SchemaField("Campaign", "STRING"),
        bigquery.SchemaField("Mailing", "STRING"),
        bigquery.SchemaField("Variant", "STRING"),
        bigquery.SchemaField("Subject_Line", "STRING"),
        bigquery.SchemaField("Segment", "STRING"),
        bigquery.SchemaField("Type_0", "STRING"),
        bigquery.SchemaField("Type_1", "STRING"),
        bigquery.SchemaField("Type_2", "STRING"),
        bigquery.SchemaField("Offer", "STRING"),
        bigquery.SchemaField("Segment_Engagement", "STRING"),
        bigquery.SchemaField("Segment_BNB", "STRING"),
        bigquery.SchemaField("Segment_Freq", "STRING"),
        bigquery.SchemaField("Test_Type", "STRING"),
        bigquery.SchemaField("Sent", "FLOAT"),
        bigquery.SchemaField("Delivered", "FLOAT"),
        bigquery.SchemaField("Opens", "FLOAT"),
        bigquery.SchemaField("Clicks", "FLOAT"),
        bigquery.SchemaField("Revenue", "FLOAT"),
        bigquery.SchemaField("Orders", "FLOAT"),
        bigquery.SchemaField("GA_Revenue", "FLOAT"),
        bigquery.SchemaField("GA_Orders", "FLOAT"),
        bigquery.SchemaField("GA_Sessions", "FLOAT"),
        bigquery.SchemaField("AA_Visits", "FLOAT"),
        bigquery.SchemaField("AA_Revenue", "FLOAT"),
        bigquery.SchemaField("AA_SFCC_Demand", "FLOAT"),
        bigquery.SchemaField("AA_Orders", "FLOAT"),
        bigquery.SchemaField("AA_Mobile_Visits", "FLOAT"),
        bigquery.SchemaField("Units", "FLOAT"),
        bigquery.SchemaField("Product_Margin", "FLOAT"),
        bigquery.SchemaField("Total_Margin", "FLOAT"),
        bigquery.SchemaField("Unsub", "FLOAT"),
        bigquery.SchemaField("Complaints", "FLOAT"),
        bigquery.SchemaField("Hard_Bounces", "FLOAT"),
        bigquery.SchemaField("Soft_Bounces", "FLOAT"),
        bigquery.SchemaField("Total_Bounces", "FLOAT"),
        bigquery.SchemaField("By_Variant", "STRING"),
        bigquery.SchemaField("ESP", "STRING"),
        bigquery.SchemaField("Type_3", "STRING"),
        bigquery.SchemaField("Send_Weekday", "STRING"),
        bigquery.SchemaField("Total_Recipients", "STRING"),
        bigquery.SchemaField("Total_Opens", "STRING"),
        bigquery.SchemaField("Total_Clicks", "STRING"),
        bigquery.SchemaField("Campaign_ID", "STRING"),
        bigquery.SchemaField("Winning_Variant_question_", "STRING"),
        bigquery.SchemaField("Flow_Message_ID", "STRING"),
        bigquery.SchemaField("Flow_ID", "STRING"),
        bigquery.SchemaField("Flow_Message_Name", "STRING"),
        bigquery.SchemaField("Error", "STRING"),
        bigquery.SchemaField("Original_Segment", "STRING"),
        bigquery.SchemaField("FISCAL_YEAR", "INTEGER"),
        bigquery.SchemaField("FISCAL_YEAR_START", "DATE"),
        bigquery.SchemaField("FISCAL_WEEK", "INTEGER"),
        bigquery.SchemaField("FISCAL_MONTH", "INTEGER"),
        bigquery.SchemaField("FISCAL_QUARTER", "INTEGER"),

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
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Campaign", "STRING"),
            bigquery.SchemaField("Mailing", "STRING"),
            bigquery.SchemaField("Variant", "STRING"),
            bigquery.SchemaField("Subject_Line", "STRING"),
            bigquery.SchemaField("Segment", "STRING"),
            bigquery.SchemaField("Type_0", "STRING"),
            bigquery.SchemaField("Type_1", "STRING"),
            bigquery.SchemaField("Type_2", "STRING"),
            bigquery.SchemaField("Offer", "STRING"),
            bigquery.SchemaField("Segment_Engagement", "STRING"),
            bigquery.SchemaField("Segment_BNB", "STRING"),
            bigquery.SchemaField("Segment_Freq", "STRING"),
            bigquery.SchemaField("Test_Type", "STRING"),
            bigquery.SchemaField("Sent", "FLOAT"),
            bigquery.SchemaField("Delivered", "FLOAT"),
            bigquery.SchemaField("Opens", "FLOAT"),
            bigquery.SchemaField("Clicks", "FLOAT"),
            bigquery.SchemaField("Revenue", "FLOAT"),
            bigquery.SchemaField("Orders", "FLOAT"),
            bigquery.SchemaField("GA_Revenue", "FLOAT"),
            bigquery.SchemaField("GA_Orders", "FLOAT"),
            bigquery.SchemaField("GA_Sessions", "FLOAT"),
            bigquery.SchemaField("AA_Visits", "FLOAT"),
            bigquery.SchemaField("AA_Revenue", "FLOAT"),
            bigquery.SchemaField("AA_SFCC_Demand", "FLOAT"),
            bigquery.SchemaField("AA_Orders", "FLOAT"),
            bigquery.SchemaField("AA_Mobile_Visits", "FLOAT"),
            bigquery.SchemaField("Units", "FLOAT"),
            bigquery.SchemaField("Product_Margin", "FLOAT"),
            bigquery.SchemaField("Total_Margin", "FLOAT"),
            bigquery.SchemaField("Unsub", "FLOAT"),
            bigquery.SchemaField("Complaints", "FLOAT"),
            bigquery.SchemaField("Hard_Bounces", "FLOAT"),
            bigquery.SchemaField("Soft_Bounces", "FLOAT"),
            bigquery.SchemaField("Total_Bounces", "FLOAT"),
            bigquery.SchemaField("By_Variant", "STRING"),
            bigquery.SchemaField("ESP", "STRING"),
            bigquery.SchemaField("Type_3", "STRING"),
            bigquery.SchemaField("Send_Weekday", "STRING"),
            bigquery.SchemaField("Total_Recipients", "STRING"),
            bigquery.SchemaField("Total_Opens", "STRING"),
            bigquery.SchemaField("Total_Clicks", "STRING"),
            bigquery.SchemaField("Campaign_ID", "STRING"),
            bigquery.SchemaField("Winning_Variant_question_", "STRING"),
            bigquery.SchemaField("Flow_Message_ID", "STRING"),
            bigquery.SchemaField("Flow_ID", "STRING"),
            bigquery.SchemaField("Flow_Message_Name", "STRING"),
            bigquery.SchemaField("Error", "STRING"),
            bigquery.SchemaField("Original_Segment", "STRING"),
            bigquery.SchemaField("FISCAL_YEAR", "INTEGER"),
            bigquery.SchemaField("FISCAL_YEAR_START", "DATE"),
            bigquery.SchemaField("FISCAL_WEEK", "INTEGER"),
            bigquery.SchemaField("FISCAL_MONTH", "INTEGER"),
            bigquery.SchemaField("FISCAL_QUARTER", "INTEGER"),  
                      
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