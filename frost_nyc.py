import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime
from google.cloud import storage
import datetime as dt


def update_masterlist(uri,client_id,client_esp):
    
    main_module = __import__('main')
    esp_module = __import__(client_esp)
    
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
    except:
        main_module.logs(dt_start, 'Klaviyo-Campaigns.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return

    try:
        campaign_analytics_data_df = pd.read_csv(uri + client_id +'-Campaign-Analytics.csv', skiprows=6)
    except:
        main_module.logs(dt_start,'Campaign-Analytics.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return


    # Reading Trigger Files
    try:
        trigger_data_df = pd.read_csv(uri + client_id +'-Klaviyo-Flows.csv',skiprows=5)
    except:
        main_module.logs(dt_start,'Klaviyo-Flows.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return


    try:
        trigger_analytics_data_df = pd.read_csv(uri + client_id +'-Trigger-Analytics.csv', skiprows=6) 
    except:
        main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return

    # Trigger Half
    try:
        trigger_data_df_2 = pd.read_csv(uri + client_id +'-Klaviyo-Flows_2.csv',skiprows=5) 
        
    except Exception as e:
        print(e)


    try:
        trigger_analytics_data_df_2 = pd.read_csv(uri + client_id +'-Trigger-Analytics_2.csv', skiprows=6) 
    except Exception as e:
        print(e)

    # Trigger Types
    try:
        trigger_type_df = pd.read_csv(uri + client_id +'-Trigger-Types.csv')
    except:
        main_module.logs(dt_start,'Trigger-Types.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return


    # Reading Old Campaign Files
    try:
        rev_order_df = pd.read_excel(uri + client_id + '-Old-Campaign-rev-order.xlsx',sheet_name='Dataset2')
    except:
        main_module.logs(dt_start,'Old-Campaign-rev-order.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return
    try:
        session_df = pd.read_excel(uri + client_id + '-Old-Campaign-session.xlsx', sheet_name='Dataset2')
    except:
        main_module.logs(dt_start,'Old-Campaign-session.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return

    # For Reconciliation
    if not trigger_data_df_2.empty:
        initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum() + trigger_data_df_2['Delivered'].sum(), 2) 
        initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum() + trigger_data_df_2['Revenue'].sum(), 2)    
    else:
        initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum(), 2) 
        initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum(), 2) 

    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','campaign_id','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Spam_Complaints','Bounces','Order','Revenue','GA_Sessions','GA_Order','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question']
    
    # Campaign Transformation / Process
    main_campaign_df = esp_module.campaign_transform(campaign_data_df,client_id,client_esp)
    
    main_campaign_analytics_df = esp_module.ga_transform(campaign_analytics_data_df,'Campaign-Analytics.csv',client_id,client_esp)
    main_campaign_masterlist_df = esp_module.campaign_ga_join(main_campaign_df,main_campaign_analytics_df)
    main_campaign_masterlist_df = main_campaign_masterlist_df[all_cols].copy()
    

    # Trigger Transformation / Joining Trigger Types
    trigger_name = 'Flow Message Name'
    trigger_flow_name = 'Flow Name'

    if trigger_data_df_2.empty:
        main_trigger_df = esp_module.Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name)
        main_trigger_analytics_df = esp_module.ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
        main_trigger_df = pd.merge(main_trigger_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_df = main_trigger_df.drop(columns=['Campaign Name'])
        main_trigger_masterlist_df = esp_module.campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
        main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()
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
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_masterlist_df.to_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv', index=False)
    masterlist_bigquery = main_module.get_table(client_id)
    masterlist_bigquery.to_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv', index=False)

    # Updating Bigquery Dataframe
    updated_masterlist = pd.concat([masterlist_bigquery,main_masterlist_df])
    updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant'], keep='last').sort_values('Date')
    #updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)
    
    # Adding Old Campaign
    old_campaign_data = main_module.process_old_campaign(uri,client_id,updated_masterlist,rev_order_df,session_df)
    updated_masterlist = pd.concat([updated_masterlist,old_campaign_data])
    updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant'], keep='last').sort_values('Date')
    updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)

    # Updating to BigQuery
    main_module.delete_table(client_id)
    esp_module.bigquery_insert(client_id)

    # Archiving Source Files
    files_list = [client_id+'-Klaviyo-Campaigns.csv', client_id+'-Klaviyo-Flows.csv', client_id+'-Campaign-Analytics.csv', client_id+'-Trigger-Analytics.csv', client_id+'-Trigger-Types.csv']
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    for file_name in files_list:
        main_module.copy_blob('reporter-etl',client_id + '/' + file_name,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +' '+ file_name)
    