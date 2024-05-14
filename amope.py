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


client_name = 'Amope'
dbx_path = '/Client/Amope/Internal FIles/Reporting/'
main_module = __import__('main')
def update_masterlist(uri,client_id,client_esp):
    
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

    #try:
        #campaign_analytics_data_df = pd.read_csv(uri + client_id +'-Campaign-Analytics.csv', skiprows=6)
    #except Exception as e:
        #print(e)
        #error_result = error_result + ' ' + 'Campaign-Analytics.csv Not Found'
        #main_module.logs(dt_start,'Campaign-Analytics.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        #return(error_result)


    # Reading Trigger Files
    try:
        trigger_data_df = pd.read_csv(uri + client_id +'-Klaviyo-Flows.csv',skiprows=5)
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Klaviyo-Flows.csv File Not Found'
        main_module.logs(dt_start,'Klaviyo-Flows.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)

    #try:
        #trigger_analytics_data_df = pd.read_csv(uri + client_id +'-Trigger-Analytics.csv', skiprows=6) 
    #except Exception as e:
        #print(e)
        #error_result = error_result + ' ' + 'Trigger-Analytics.csv File Not Found'
        #main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        #return(error_result)

    # Trigger Half
    try:
        trigger_data_df_2 = pd.read_csv(uri + client_id +'-Klaviyo-Flows_2.csv',skiprows=5) 
        
    except Exception as e:
        print(e)


    #try:
        #trigger_analytics_data_df_2 = pd.read_csv(uri + client_id +'-Trigger-Analytics_2.csv', skiprows=6) 
    #except Exception as e:
        #print(e)

    # Trigger Types
    try:
        trigger_type_df = pd.read_csv(uri + client_id +'-Trigger-Types.csv')
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Trigger-Types.csv File Not Found'
        main_module.logs(dt_start,'Trigger-Types.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)
    

    # Reading Old Campaign Files
    #try:
        #rev_order_df = pd.read_excel(uri + client_id + '-Old-Campaign-rev-order.xlsx',sheet_name='Dataset2')
    #except Exception as e:
        #print(e)
        #error_result = error_result + ' ' + 'Old-Campaign-rev-order.xlsx File Not Found'
        #main_module.logs(dt_start,'Old-Campaign-rev-order.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        #return(error_result)
    #try:
        #session_df = pd.read_excel(uri + client_id + '-Old-Campaign-session.xlsx', sheet_name='Dataset2')
    #except Exception as e:
        #print(e)
        #error_result = error_result + ' ' + 'Old-Campaign-session.xlsx File Not Found'
        #main_module.logs(dt_start,'Old-Campaign-session.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        #return(error_result)

    # For Reconciliation
    if not trigger_data_df_2.empty:
        initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum() + trigger_data_df_2['Delivered'].sum(), 2) 
        initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum() + trigger_data_df_2['Revenue'].sum(), 2)    
    else:
        initial_delivered = round(campaign_data_df['Successful Deliveries'].sum() + trigger_data_df['Delivered'].sum(), 2) 
        initial_revenue = round(campaign_data_df['Revenue'].sum() + trigger_data_df['Revenue'].sum(), 2) 

    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','Campaign_ID','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Sessions','GA_Orders','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Name','Send_Weekday','Winning_Variant_question_']

    # Campaign Transformation / Process
    
    main_campaign_masterlist_df = campaign_transform(campaign_data_df,client_id,client_esp,client_name)
    main_campaign_masterlist_df['GA_Revenue'] = 0
    main_campaign_masterlist_df['GA_Orders'] = 0
    main_campaign_masterlist_df['GA_Sessions'] = 0
    #main_campaign_analytics_df = esp_module.ga_transform(campaign_analytics_data_df,'Campaign-Analytics.csv',client_id,client_esp)
    #main_campaign_masterlist_df = esp_module.campaign_ga_join(main_campaign_df,main_campaign_analytics_df)
    main_campaign_masterlist_df = main_campaign_masterlist_df[all_cols].copy()
    
    # Trigger Transformation / Joining Trigger Types
    trigger_name = 'Flow Message Name'
    trigger_flow_name = 'Flow Name'

    if trigger_data_df_2.empty:
        main_trigger_masterlist_df = Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name)
        main_trigger_masterlist_df['GA_Revenue'] = 0
        main_trigger_masterlist_df['GA_Orders'] = 0
        main_trigger_masterlist_df['GA_Sessions'] = 0
        #main_trigger_analytics_df = esp_module.ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
        main_trigger_masterlist_df = pd.merge(main_trigger_masterlist_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_masterlist_df = main_trigger_masterlist_df.drop(columns=['Campaign Name'])
        #main_trigger_masterlist_df = esp_module.campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
        main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()
        
    else:
        main_trigger_masterlist_df = Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name)
        #main_trigger_analytics_df = esp_module.ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
        main_trigger_masterlist_df['GA_Revenue'] = 0
        main_trigger_masterlist_df['GA_Orders'] = 0
        main_trigger_masterlist_df['GA_Sessions'] = 0
        main_trigger_masterlist_df = pd.merge(main_trigger_masterlist_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_masterlist_df = main_trigger_masterlist_df.drop(columns=['Campaign Name'])
        #main_trigger_masterlist_df = esp_module.campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
        main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()

        main_trigger_masterlist_df_2 = Trigger_Transform(trigger_data_df_2,client_id,client_esp,trigger_name,trigger_flow_name)
        main_trigger_masterlist_df_2['GA_Revenue'] = 0
        main_trigger_masterlist_df_2['GA_Orders'] = 0
        main_trigger_masterlist_df_2['GA_Sessions'] = 0
        #main_trigger_analytics_df_2 = esp_module.ga_transform(trigger_analytics_data_df_2,'Trigger-Analytics_2.csv',client_id,client_esp)
        main_trigger_masterlist_df_2 = pd.merge(main_trigger_masterlist_df_2,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
        main_trigger_masterlist_df_2 = main_trigger_masterlist_df_2.drop(columns=['Campaign Name'])
        #main_trigger_masterlist_df_2 = esp_module.campaign_ga_join(main_trigger_df_2,main_trigger_analytics_df_2)
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
        #main_module.alert_email('Campaign + Trigger rows vs BigQuery rows did not matched',client_id)
        #return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs BigQuery rows', str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows', 'matched',client_id,client_esp)
    # Check Delivered
    if initial_delivered != final_delivered:
        print('test -------',initial_delivered,final_delivered)
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'not matched',client_id,client_esp)
        #main_module.alert_email('Campaign + Trigger rows vs Masterlist Delivered Counts did not matched',client_id)
        #return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'matched',client_id,client_esp)
    # Check Revenue
    if initial_revenue != final_revenue:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'not matched',client_id,client_esp)
        #main_module.alert_email('Campaign + Trigger rows vs Masterlist Revenue Counts did not matched',client_id)
        #return
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'matched',client_id,client_esp)

    # Backing up Dataframes
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Name','Send_Weekday',]
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
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Name'], keep='last').sort_values('Date')
    dedup = updated_masterlist.loc[(updated_masterlist['Date'] >= '2023-01-01')].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Name'], keep='last').sort_values('Date')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < '2023-01-01')].append(dedup)
    
    # Adding Old Campaign
    #old_campaign_data = main_module.process_old_campaign(uri,client_id,updated_masterlist,rev_order_df,session_df)
    #updated_masterlist = pd.concat([updated_masterlist,old_campaign_data])
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Name'], keep='last').sort_values('Date')
    #deduped = updated_masterlist.loc[(updated_masterlist['Date'] >= '2023-01-01')].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Name'], keep='last').sort_values('Date')
    #updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < '2023-01-01')].append(deduped)

    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    for i in missing_str_cols:
        updated_masterlist[i] = '-'
    for i in missing_int_cols:
        updated_masterlist[i] = 0
    updated_masterlist['Original_Segment'] = updated_masterlist['Segment']
    #cols_all = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Offer','Segment_Engagement','Segment_BNB','Segment_Freq','Test_Type','Sent','Delivered','Opens','Clicks','Revenue','Orders','GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Unsub','Complaints','Hard_Bounces','Soft_Bounces','Total_Bounces','by_Variant','ESP','Send_Weekday','Total_Opens','Total_Clicks','Campaign_ID','Winning_Variant_question_','Flow_Message_ID','Flow_ID','Flow_Name','Error','Original_Segment','FISCAL_YEAR']
    cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","ESP","Send_Weekday","Total_Recipients","Total_Opens","Total_Clicks","Campaign_ID","Winning_Variant","Flow_Message_ID","Flow_Name","Flow_ID","Error","Original_Segment","FISCAL_YEAR"]
    
    updated_masterlist = updated_masterlist[cols_all]
    print('did it made here?')
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

    dbx_rename = {"Date": "Date","Name": "Name","Campaign": "Campaign","Mailing": "Mailing","Variant": "Variant","Test_Type": "Test Type","Offer": "Offer","Subject_Line": "Subject Line","Segment": "Segment","ESP": "ESP","Type_0": "Type 0","Type_1": "Type 1","Type_2": "Type 2","Sent": "Sent","Delivered": "Delivered","Opens": "Opens","Clicks": "Clicks","Revenue": "Revenue","Orders": "Orders","Unsub": "Unsub","Complaints": "Complaints","Hard_Bounces": "Hard Bounces","Soft_Bounces": "Soft Bounces","Total_Bounces": "Total Bounces","Send_Weekday": "Send Weekday","Total_Recipients": "Total Recipients","Total_Opens": "Total Opens","Total_Clicks": "Total Clicks","Campaign_ID": "Campaign ID","Winning_Variant": "Winning Variant","Flow_Message_ID": "Flow Message ID","Flow_Name": "Flow Name","Flow_ID": "Flow ID","GA_Revenue": "GA Revenue","GA_Orders": "GA Orders","GA_Sessions": "GA Sessions"}
    dbx_schema = ["Date","Name","Campaign","Mailing","Variant","Test Type","Offer","Subject Line","Segment","ESP","Type 0","Type 1","Type 2","Sent","Delivered","Opens","Clicks","Revenue","Orders","Unsub","Complaints","Hard Bounces","Soft Bounces","Total Bounces","Send Weekday","Total Recipients","Total Opens","Total Clicks","Campaign ID","Winning Variant","Flow Message ID","Flow Name","Flow ID","GA Revenue","GA Orders","GA Sessions"]
    dbx_masterlist = updated_masterlist
    dbx_masterlist.rename(columns=dbx_rename, inplace=True)
    dbx_masterlist = dbx_masterlist[dbx_schema].copy()
    
    if os.environ['USEDROPBOX'] == 'yes':
        main_module.to_dropbox(dbx_masterlist,dbx_path,client_name)
    
    updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)

    # Updating to BigQuery
    #main_module.delete_table(client_id)
    #esp_module.bigquery_insert(client_id)
    #bigquery_insert(client_id)

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
        'Name':'Name',
        'Campaign':'Campaign',
        'Mailing':'Mailing',
        'Variant':'Variant',
        'Subject Line':'Subject_Line',
        'Segment':'Segment',
        'Date':'Date',
        'Send Weekday':'Send_Weekday',
        'ESP':'ESP',
        'Type 0':'Type_0',
        'Type 1':'Type_1',
        'Type 2':'Type_2',
        'Sent':'Sent',
        'Orders':'Orders',
        'Revenue':'Revenue',
        'Opens':'Opens',
        'Total Opens':'Total_Opens',
        'Clicks':'Clicks',
        'Total Clicks':'Total_Clicks',
        'Unsub':'Unsub',
        'Complaints':'Complaints',
        'Delivered':'Delivered',
        'Total Bounce':'Total_Bounces',
        'Campaign ID':'Campaign_ID',
        'Flow Message Name':'Flow_Name',
        'Flow Message ID':'Flow_Message_ID',
        'Flow ID':'Flow_ID',
        'GA Revenue':'GA_Revenue',
        'GA Orders':'GA_Orders',
        'GA Sessions':'GA_Sessions',
        }
    new_master.rename(
        columns=rename_cols,
        inplace=True)
    
    new_master['Date'] = pd.to_datetime(new_master['Date']).dt.strftime('%Y-%m-%d')
    new_master['Campaign_ID'] = new_master['Campaign_ID'].str.lower()
    #new_master['Type_3'] = '-'
    new_master['Original_Segment'] = new_master['Segment']
    
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Name','Send_Weekday',]
    int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Revenue','GA_Orders','GA_Sessions']
    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','Test_Type','Offer','Winning_Variant_question_']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    #all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','campaign_id','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Bounces','Order','Revenue','GA_Sessions','GA_Order','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Name','Send_Weekday','Winning_Variant_question']
    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Offer','Segment_Engagement','Segment_BNB','Segment_Freq','Test_Type','Sent','Delivered','Opens','Clicks','Revenue','Orders','GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Unsub','Complaints','Hard_Bounces','Soft_Bounces','Total_Bounces','ESP','Type_3','Send_Weekday','Total_Opens','Total_Clicks','Campaign_ID','Winning_Variant_question_','Flow_Message_ID','Flow_ID','Flow_Name','Error','Original_Segment','FISCAL_YEAR']
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
    
    dbx_rename = {"Date":"Date","Name":"Name","Campaign":"Campaign","Mailing":"Mailing","Variant":"Variant","Subject_Line":"Subject Line","Segment":"Segment","Type_0":"Type 0","Type_1":"Type 1","Type_2":"Type 2","Sent":"Sent","Delivered":"Delivered","Opens":"Opens","Clicks":"Clicks","Revenue":"Revenue","Orders":"Orders","GA_Revenue":"GA Revenue","GA_Orders":"GA Orders","GA_Sessions":"GA Sessions","Unsub":"Unsub","Complaints":"Complaints","Total_Bounces":"Total Bounce","ESP":"ESP","Type_3":"Type 3","Send_Weekday":"Send Weekday","Total_Opens":"Total Opens","Total_Clicks":"Total Clicks","Campaign_ID":"Campaign ID","Winning_Variant_question_":"Winning Variant?","Flow_Message_ID":"Flow Message ID","Flow_ID":"Flow ID","Flow_Name":"Flow Message Name"}
    dbx_schema = ["Name","Campaign","Mailing","Variant","Subject Line","Segment","Date","ESP","Type 0","Type 1","Type 2","Type 3","Send Weekday","Sent","Orders","Revenue","Opens","Total Opens","Clicks","Total Clicks","Unsub","Complaints","Delivered","Total Bounce","Campaign ID","Winning Variant?","Flow Message ID","Flow ID","Flow Message Name","GA Revenue","GA Orders","GA Sessions"]
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
        bigquery.SchemaField('ESP', 'STRING'),
        bigquery.SchemaField('Type_3', 'STRING'),
        bigquery.SchemaField('Send_Weekday', 'STRING'),
        bigquery.SchemaField('Total_Opens', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Campaign_ID', 'STRING'),
        bigquery.SchemaField('Winning_Variant_question_', 'STRING'),
        bigquery.SchemaField('Flow_Message_ID', 'STRING'),
        bigquery.SchemaField('Flow_ID', 'STRING'),
        bigquery.SchemaField('Flow_Name', 'STRING'),
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
            bigquery.SchemaField('ESP', 'STRING'),
            bigquery.SchemaField('Type_3', 'STRING'),
            bigquery.SchemaField('Send_Weekday', 'STRING'),
            bigquery.SchemaField('Total_Opens', 'STRING'),
            bigquery.SchemaField('Total_Clicks', 'STRING'),
            bigquery.SchemaField('Campaign_ID', 'STRING'),
            bigquery.SchemaField('Winning_Variant_question_', 'STRING'),
            bigquery.SchemaField('Flow_Message_ID', 'STRING'),
            bigquery.SchemaField('Flow_ID', 'STRING'),
            bigquery.SchemaField('Flow_Name', 'STRING'),
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
        trigger_flow_name : 'Flow_Name',
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
    
    str_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Test_Type','Offer','Campaign_ID','Flow_Message_ID','Flow_ID','Flow_Name','Send_Weekday','Winning_Variant_question_']
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
        'Flow_Name']].copy()
    

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

    main_module.logs(dt_start,'Klaviyo-Flows.csv',str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows','success',client_id,client_esp)
    
    return df

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
        df['Flow_Name'] = '-'
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
        df['Flow_Name'] = '-'

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
    #promo_pattern = '|'.join(['-NonPurch','-Purch'])
    #df['Mailing'] = df['Mailing'].str.replace(promo_pattern, '')

    str_cols = ['Date','Name','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Name','Send_Weekday','Winning_Variant_question_']
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
        'Flow_Name']].copy()

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

    main_module.logs(dt_start, 'Klaviyo-Campaign.csv',str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows','success',client_id,client_esp)
    
    return df