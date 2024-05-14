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


client_name = 'CBD.co - Retail'
dbx_path = '/Client/CBD.co/CBD.Co Retail/Internal Files/Reporting/'

def update_masterlist(uri,client_id,client_esp):
    
    main_module = __import__('main')
    esp_module = __import__(client_esp)
    error_result = ''
    
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/cbd_co_retail/2023_0625-0701/' #local
    # print(uri)
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_type_df = pd.DataFrame()
    trigger_analytics_data_rev_df = pd.DataFrame()
    trigger_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df_2 = pd.DataFrame()
    trigger_analytics_data_rev_df_2 = pd.DataFrame()
    trigger_analytics_data_ses_df_2 = pd.DataFrame()
    rev_order_df = pd.DataFrame()
    session_df = pd.DataFrame()
    oldCampDate_df = pd.DataFrame()
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
        campaign_analytics_data_rev_df = pd.read_csv(uri + client_id +'-Campaign-Analytics-rev.csv', skiprows=6,usecols=["Campaign","Total revenue","Conversions"])
        campaign_analytics_data_rev_df = campaign_analytics_data_rev_df.dropna()
        campaign_analytics_data_rev_df.rename(
            columns={
                "Campaign":"Session campaign"
            },
            inplace=True
        )
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Campaign-Analytics-rev.csv Not Found'
        main_module.logs(dt_start,'Campaign-Analytics-rev.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return(error_result)
    
    try:
        campaign_analytics_data_ses_df = pd.read_csv(uri + client_id +'-Campaign-Analytics-ses.csv', skiprows=6,usecols=["Session campaign","Sessions"])
        campaign_analytics_data_ses_df = campaign_analytics_data_ses_df.dropna()
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Campaign-Analytics-ses.csv Not Found'
        main_module.logs(dt_start,'Campaign-Analytics-ses.csv', '0 rows', '0 rows','failed',client_id,client_esp)
        return(error_result)


    # Reading Trigger Files
    try:
        trigger_data_df = pd.read_csv(uri + client_id +'-Klaviyo-Flows.csv',skiprows=5)
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Klaviyo-Flows.csv File Not Found'
        main_module.logs(dt_start,'Klaviyo-Flows.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)


    try:
        trigger_analytics_data_rev_df = pd.read_csv(uri + client_id +'-Trigger-Analytics-rev.csv', skiprows=6, usecols=["Campaign","Total revenue","Conversions"]) 
        trigger_analytics_data_rev_df = trigger_analytics_data_rev_df.dropna()
        trigger_analytics_data_rev_df.rename(
            columns={
               "Campaign":"Session campaign"
            },
            inplace=True
        )
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Trigger-Analytics-rev.csv File Not Found'
        main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)
    try:
        trigger_analytics_data_ses_df = pd.read_csv(uri + client_id +'-Trigger-Analytics-ses.csv', skiprows=6, usecols=["Session campaign","Sessions"])
        trigger_analytics_data_ses_df = trigger_analytics_data_ses_df.dropna()
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + 'Trigger-Analytics-ses.csv File Not Found'
        main_module.logs(dt_start,'Trigger-Analytics.csv', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)

    # Trigger Half
    try:
        trigger_data_df_2 = pd.read_csv(uri + client_id +'-Klaviyo-Flows_2.csv',skiprows=5) 
        
    except Exception as e:
        print(e)
    try:
        trigger_analytics_data_rev_df_2 = pd.read_csv(uri + client_id +'-Trigger-Analytics-rev_2.csv', skiprows=6, usecols=["Campaign","Total revenue","Conversions"]) 
        trigger_analytics_data_rev_df_2 = trigger_analytics_data_rev_df_2.dropna()
        trigger_analytics_data_rev_df_2.rename(
            columns={
               "Campaign":"Session campaign"
            },
            inplace=True
        )
    except Exception as e:
        print(e)
    
    try:
        trigger_analytics_data_ses_df_2 = pd.read_csv(uri + client_id +'-Trigger-Analytics-ses_2.csv', skiprows=6, usecols=["Session campaign","Sessions"])
        trigger_analytics_data_ses_df_2 = trigger_analytics_data_ses_df_2.dropna()
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
    
    try:
        rev_order_date_df = pd.read_csv(uri + client_id + '-Old-Campaign-rev.csv',header=None,nrows=5)
        oldcamdate = rev_order_date_df.iloc[3][0]
        oldcamdate = oldcamdate.split("#")[1].strip()
        oldcamdatelst = oldcamdate.split("-")
        oldCampDate_time_df = pd.date_range(start = oldcamdatelst[0], end = oldcamdatelst[1])
        oldCampDate_time_df = oldCampDate_time_df.tolist()
        oldCampDate_time_df = [int(x.strftime('%Y%m%d')) for x in oldCampDate_time_df]
        oldCampDate_df = pd.DataFrame({
            "Date":oldCampDate_time_df
        })
        rev_order_rev_df = pd.read_csv(uri + client_id + '-Old-Campaign-rev.csv',skiprows=6, usecols=["Date","Total revenue","Conversions"])
        rev_order_rev_df = rev_order_rev_df.dropna()
        rev_order_rev_df = pd.merge(oldCampDate_df,rev_order_rev_df, on="Date",how="left")
        rev_order_rev_df = rev_order_rev_df.fillna(0)
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + '-Old-Campaign-rev.csv File Not Found'
        main_module.logs(dt_start,'-Old-Campaign.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
        return(error_result)
    try:
        rev_order_ses_df = pd.read_csv(uri + client_id + '-Old-Campaign-ses.csv',skiprows=6, usecols=["Date","Sessions"])
        rev_order_ses_df = rev_order_ses_df.dropna()
        rev_order_ses_df = pd.merge(oldCampDate_df,rev_order_ses_df, on="Date",how="left")
        rev_order_ses_df = rev_order_ses_df.fillna(0)
    except Exception as e:
        print(e)
        error_result = error_result + ' ' + '-Old-Campaign-ses.csv File Not Found'
        main_module.logs(dt_start,'-Old-Campaign.xlsx', '0 rows', '0 rows', 'failed',client_id,client_esp)
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
    version_pattern = '|'.join(['-v1', '-v2', '-v3','-v4','-v5','-v6','-V1','-V2','-V3','-V4','-V5','-V6','-resend'])
    main_campaign_df['Campaign'] = main_campaign_df['Campaign'].str.replace(version_pattern, '')
    ## amit
    campaign_analytics_data_df = pd.merge(campaign_analytics_data_rev_df,campaign_analytics_data_ses_df, on="Session campaign",how="outer")
    campaign_analytics_data_df = campaign_analytics_data_df.fillna(0)
    ##
    main_campaign_analytics_df = ga_transform(campaign_analytics_data_df,'Campaign-Analytics.csv',client_id,client_esp)
    main_campaign_masterlist_df = esp_module.campaign_ga_join(main_campaign_df,main_campaign_analytics_df)
    main_campaign_masterlist_df = main_campaign_masterlist_df[all_cols].copy()
    # Trigger Transformation / Joining Trigger Types
    trigger_name = 'Flow Message Name'
    trigger_flow_name = 'Flow Name' 

    main_trigger_df = esp_module.Trigger_Transform(trigger_data_df,client_id,client_esp,trigger_name,trigger_flow_name)
    trigger_analytics_data_df = pd.merge(trigger_analytics_data_rev_df,trigger_analytics_data_ses_df, on="Session campaign",how="outer")
    trigger_analytics_data_df = trigger_analytics_data_df.fillna(0)
    main_trigger_analytics_df = ga_transform(trigger_analytics_data_df,'Trigger-Analytics.csv',client_id,client_esp)
    main_trigger_df = pd.merge(main_trigger_df,trigger_type_df, left_on='Name', right_on='Campaign Name', how='left')
    main_trigger_df = main_trigger_df.drop(columns=['Campaign Name'])
    main_trigger_masterlist_df = esp_module.campaign_ga_join(main_trigger_df,main_trigger_analytics_df)
    main_trigger_masterlist_df = main_trigger_masterlist_df[all_cols].copy()
    if not trigger_data_df_2.empty:
        main_trigger_df_2 = esp_module.Trigger_Transform(trigger_data_df_2,client_id,client_esp,trigger_name,trigger_flow_name)
        trigger_analytics_data_df_2 = pd.merge(trigger_analytics_data_rev_df_2,trigger_analytics_data_ses_df_2, on="Session campaign",how="outer")
        trigger_analytics_data_df_2 = trigger_analytics_data_df_2.fillna(0)
        main_trigger_analytics_df_2 = ga_transform(trigger_analytics_data_df_2,'Trigger-Analytics_2.csv',client_id,client_esp)
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
    # main_masterlist_df.to_csv(uri + 'newGATest.csv', index=False)
    
    
    # For Reconciliation
    
    if trigger_data_df_2.empty:
        initial_rowcount = main_campaign_masterlist_df['Name'].count() + main_trigger_masterlist_df['Name'].count()
    else:
        initial_rowcount = main_campaign_masterlist_df['Name'].count() + main_trigger_masterlist_df['Name'].count() + main_trigger_masterlist_df_2['Name'].count()
    final_rowcount = main_masterlist_df['Name'].count()
    final_delivered = round(main_masterlist_df['Delivered'].sum(), 2)
    final_revenue = round(main_masterlist_df['Revenue'].sum(), 2)
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    """
    # Check Row Count
    if initial_rowcount != final_rowcount:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs BigQuery rows', str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows', 'not matched',client_id,client_esp)
        #main_module.alert_email('Campaign + Trigger rows vs BigQuery rows did not matched',client_id)
        
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs BigQuery rows', str(initial_rowcount) + ' rows',str(final_rowcount) + ' rows', 'matched',client_id,client_esp)
    # Check Delivered
    if initial_delivered != final_delivered:
        print('test -------',initial_delivered,final_delivered)
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'not matched',client_id,client_esp)
        #main_module.alert_email('Campaign + Trigger rows vs Masterlist Delivered Counts did not matched',client_id)
        
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Delivered Counts', str(initial_delivered), str(final_delivered), 'matched',client_id,client_esp)
    # Check Revenue
    if initial_revenue != final_revenue:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'not matched',client_id,client_esp)
        #main_module.alert_email('Campaign + Trigger rows vs Masterlist Revenue Counts did not matched',client_id)
        
    else:
        main_module.logs(dt_start, 'Campaign + Trigger rows vs Masterlist Revenue Counts', str(initial_revenue), str(final_revenue), 'matched',client_id,client_esp)
    """
    # Backing up Dataframes
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Type_3','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday',]
    int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Revenue','GA_Orders','GA_Sessions']
    timestamp = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    #
    main_masterlist_df['Date'] = main_masterlist_df['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
    main_masterlist_df.to_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv', index=False)
    # main_masterlist_df = pd.read_csv(uri + 'archive/' + str(timestamp) + ' New Data Masterlist.csv')
    masterlist_bigquery = main_module.get_table(client_id,str_cols,int_cols)
    
    masterlist_bigquery.to_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv', index=False)
    # masterlist_bigquery = pd.read_csv(uri + 'archive/' + str(timestamp) + ' Bigquery Masterlist.csv')
    
    main_masterlist_df['Variant'] = main_masterlist_df['Variant'].replace('', "-", regex=True)
    main_masterlist_df['Variant'] = main_masterlist_df['Variant'].replace(np.nan, "-", regex=True)
    # Updating Bigquery Dataframe
    updated_masterlist = pd.concat([masterlist_bigquery,main_masterlist_df])
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name'], keep='last').sort_values('Date')
    filter_date = dt.datetime.strptime('2022-01-01', "%Y-%m-%d")
    filter_date = filter_date.date()
    dedup = updated_masterlist.loc[(updated_masterlist['Date'] >= filter_date)].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name',"Flow_Message_ID"], keep='last').sort_values('Date')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < filter_date)].append(dedup)
    # Adding Old Campaign
    rev_order_df = pd.merge(rev_order_rev_df,rev_order_ses_df,on="Date",how="outer")
    old_campaign_data = process_old_campaign(uri,client_id,updated_masterlist,rev_order_df)
    updated_masterlist = pd.concat([updated_masterlist,old_campaign_data])
    #updated_masterlist = updated_masterlist.drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name'], keep='last').sort_values('Date')
    # deduped = updated_masterlist.loc[(updated_masterlist['Date'] >= filter_date)].drop_duplicates(subset=['Date','Name','Subject_Line','Segment','Variant','Flow_Message_Name',"Flow_Message_ID","Campaign_ID","Winning_Variant_question_"], keep='last').sort_values('Date')
    # updated_masterlist = updated_masterlist.loc[(updated_masterlist['Date'] < filter_date)].append(deduped)
    # 
    updated_masterlist_oldcamp = updated_masterlist.loc[(updated_masterlist['Name'] == "Old Campaign")].drop_duplicates(subset=['Date'], keep='last')
    updated_masterlist = updated_masterlist.loc[(updated_masterlist['Name'] != "Old Campaign")].append(updated_masterlist_oldcamp)
    updated_masterlist.reset_index(inplace=True)
    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','by_Variant']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    for i in missing_str_cols:
        updated_masterlist[i] = '-'
    for i in missing_int_cols:
        updated_masterlist[i] = 0
    updated_masterlist['Original_Segment'] = updated_masterlist['Segment']
    #cols_all = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Offer','Segment_Engagement','Segment_BNB','Segment_Freq','Test_Type','Sent','Delivered','Opens','Clicks','Revenue','Orders','GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Unsub','Complaints','Hard_Bounces','Soft_Bounces','Total_Bounces','by_Variant','ESP','Send_Weekday','Total_Opens','Total_Clicks','Campaign_ID','Winning_Variant_question_','Flow_Message_ID','Flow_ID','Flow_Message_Name','Error','Original_Segment','FISCAL_YEAR']
    cols_all = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Offer','Segment_Engagement','Segment_BNB','Segment_Freq','Test_Type','Sent','Delivered','Opens','Clicks','Revenue','Orders','GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Unsub','Complaints','Hard_Bounces','Soft_Bounces','Total_Bounces','ESP','Type_3','Send_Weekday','Total_Opens','Total_Clicks','Campaign_ID','Winning_Variant_question_','Flow_Message_ID','Flow_ID','Flow_Message_Name','Error','Original_Segment','FISCAL_YEAR']
    
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

    dbx_rename = {"Date":"Date","Name":"Name","Campaign":"Campaign","Mailing":"Mailing","Variant":"Variant","Subject_Line":"Subject Line","Original_Segment":"Segment","Type_0":"Type 0","Type_1":"Type 1","Type_2":"Type 2","Test_Type":"Test Type","Offer":"Offer","Sent":"Sent","Delivered":"Delivered","Opens":"Opens","Clicks":"Clicks","Revenue":"Revenue","Orders":"Orders","GA_Revenue":"GA Revenue","GA_Orders":"GA Orders","GA_Sessions":"GA Sessions","Unsub":"Unsub","Complaints":"Complaints","Total_Bounces":"Total Bounce","ESP":"ESP","Type_3":"Type 3","Send_Weekday":"Send Weekday","Total_Opens":"Total Opens","Total_Clicks":"Total Clicks","Campaign_ID":"Campaign ID","Winning_Variant_question_":"Winning Variant?","Flow_Message_ID":"Flow Message ID","Flow_ID":"Flow ID","Flow_Message_Name":"Flow Message Name"}
    dbx_schema = ["Name","Campaign","Mailing","Variant","Subject Line","Segment","Date","ESP","Type 0","Type 1","Type 2","Type 3","Test Type","Offer","Send Weekday","Sent","Orders","Revenue","Opens","Total Opens","Clicks","Total Clicks","Unsub","Complaints","Delivered","Total Bounce","Campaign ID","Winning Variant?","Flow Message ID","Flow ID","Flow Message Name","GA Revenue","GA Orders","GA Sessions"]
    dbx_masterlist = updated_masterlist
    dbx_masterlist.rename(columns=dbx_rename, inplace=True)
    #dbx_masterlist['Variant'] = dbx_masterlist['Variant'].replace('-','n/a')
    dbx_masterlist = dbx_masterlist[dbx_schema].copy()
    if os.environ['USEDROPBOX'] == 'yes':
        main_module.to_dropbox(dbx_masterlist,dbx_path,client_name)
    updated_masterlist.to_csv(uri + 'Masterlist.csv', index=False)
    # return('test')
    # Updating to BigQuery
    main_module.delete_table(client_id)
    #esp_module.bigquery_insert(client_id)
    bigquery_insert(client_id)

    # Archiving Source Files
    files_list = [client_id+'-Klaviyo-Campaigns.csv', client_id+'-Klaviyo-Flows.csv', client_id+'-Campaign-Analytics-rev.csv', client_id+'-Campaign-Analytics-ses.csv', 
                client_id+'-Trigger-Analytics-rev.csv',client_id+'-Trigger-Analytics-ses.csv', client_id+'-Trigger-Types.csv',client_id+'-Old-Campaign-rev.csv',client_id+'-Old-Campaign-ses.csv']
    if not trigger_data_df_2.empty:
        files_list.append(client_id +'-Klaviyo-Flows_2.csv')
        files_list.append(client_id +'-Trigger-Analytics-rev_2.csv')
        files_list.append(client_id +'-Trigger-Analytics-ses_2.csv')
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    for file_name in files_list:
        main_module.copy_blob('reporter-etl',client_id + '/' + file_name,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +' '+ file_name)
        main_module.delete_blob('reporter-etl',client_id + '/' + file_name)
    
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
        'Flow Message Name':'Flow_Message_Name',
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
    new_master['Type_3'] = '-'
    new_master['Original_Segment'] = new_master['Segment']
    
    str_cols = ['Campaign','Mailing','Variant','Subject_Line','Segment','Type_2','Type_3','Campaign_ID','ESP','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday',]
    int_cols = ['Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Total_Bounces','Orders','Revenue','GA_Revenue','GA_Orders','GA_Sessions']
    missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','Test_Type','Offer','Winning_Variant_question_']
    missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','FISCAL_YEAR']
    #all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Type_3','Test_Type','Offer','campaign_id','ESP','Sent','Delivered','Opens','Total_Opens','Clicks','Total_Clicks','Unsub','Complaints','Bounces','Order','Revenue','GA_Sessions','GA_Order','GA_Revenue','Flow_Message_ID','Flow_ID','Flow_Message_Name','Send_Weekday','Winning_Variant_question']
    all_cols = ['Date','Name','Campaign','Mailing','Variant','Subject_Line','Segment','Type_0','Type_1','Type_2','Offer','Segment_Engagement','Segment_BNB','Segment_Freq','Test_Type','Sent','Delivered','Opens','Clicks','Revenue','Orders','GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Unsub','Complaints','Hard_Bounces','Soft_Bounces','Total_Bounces','ESP','Type_3','Send_Weekday','Total_Opens','Total_Clicks','Campaign_ID','Winning_Variant_question_','Flow_Message_ID','Flow_ID','Flow_Message_Name','Error','Original_Segment','FISCAL_YEAR']
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
    
    dbx_rename = {"Date":"Date","Name":"Name","Campaign":"Campaign","Mailing":"Mailing","Variant":"Variant","Subject_Line":"Subject Line","Segment":"Segment","Type_0":"Type 0","Type_1":"Type 1","Type_2":"Type 2","Sent":"Sent","Delivered":"Delivered","Opens":"Opens","Clicks":"Clicks","Revenue":"Revenue","Orders":"Orders","GA_Revenue":"GA Revenue","GA_Orders":"GA Orders","GA_Sessions":"GA Sessions","Unsub":"Unsub","Complaints":"Complaints","Total_Bounces":"Total Bounce","ESP":"ESP","Type_3":"Type 3","Send_Weekday":"Send Weekday","Total_Opens":"Total Opens","Total_Clicks":"Total Clicks","Campaign_ID":"Campaign ID","Winning_Variant_question_":"Winning Variant?","Flow_Message_ID":"Flow Message ID","Flow_ID":"Flow ID","Flow_Message_Name":"Flow Message Name"}
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
    ## archive the files
    files_list = [client_id+'-Klaviyo-Campaigns.csv', client_id+'-Klaviyo-Flows.csv', client_id+'-Campaign-Analytics-rev.csv',client_id+'-Campaign-Analytics-ses.csv', 
                client_id+'-Trigger-Analytics-rev.csv',client_id+'-Trigger-Analytics-ses.csv', client_id+'-Trigger-Types.csv']
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    for file_name in files_list:
        main_module.copy_blob('reporter-etl',client_id + '/' + file_name,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +' '+ file_name)

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
    job_config.quote_character = '"'
    job_config.allow_quoted_newlines =True

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

    try:
        load_job.result()  # wait for table load to complete.
        print('Job finished.')
    except Exception as e:
        print(e)
    destination_table = client.get_table(dataset_ref.table(client_id))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    
def ga_transform(analytics_data_df,file_name,client_id,client_esp):
    df = analytics_data_df
    
    # For Logging
    #initial_rowcount = str(df['Campaign'].count())

    df = df[df['Total revenue'].notna()]
    df = df[df['Session campaign'].notna()]
    
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    # Renaming Columns
    df.rename(
        columns={
            'Session campaign': 'Campaign_ID',
            'Total revenue': 'GA_Revenue',
            'Sessions': 'GA_Sessions',
            'Conversions': 'GA_Orders'
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
    # df['Campaign Regex'] = df['Campaign_ID'].str.extract('(\(.*?\))')[0].str.strip('()')
    # df['Campaign Regex'].fillna(df['Campaign_ID'], inplace=True)
    df['Campaign_ID'] = df['Campaign_ID'].str.replace("(","|")
    df['Campaign_ID'] = df['Campaign_ID'].str.replace(")","")
    df['Campaign_ID'] = "|"+df['Campaign_ID']
    df['Campaign_ID'] = df['Campaign_ID'].str.rsplit("|", n = 1, expand = True)[1].str.strip()
    # df['Campaign_ID'] = df['Campaign Regex']
    
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

def read_old_campaign(uri,client_id,rev_order_df):
    
    #rev_order_df = pd.read_excel(uri + client_id + '-Old-Campaign-rev-order.xlsx',sheet_name='Dataset2')
    #session_df = pd.read_excel(uri + client_id + '-Old-Campaign-session.xlsx', sheet_name='Dataset2')
    
    #df = pd.merge(rev_order_df,session_df, on='Date', how='left')
    df = rev_order_df
    # Renaming Columns
    df.rename(
        columns={
            'Date': 'Date',
            'Total revenue': 'GA_Revenue',
            'Sessions': 'GA_Sessions',
            'Conversions': 'GA_Orders'
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
    #df['Date'] = df['Date'].astype('datetime64')
    #df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    
    df = df[~df.Date.isnull()]
    df = df[[
        'Date',
        'GA_Sessions',
        'GA_Orders',
        'GA_Revenue']].copy()
    
    return(df)

def process_old_campaign(uri,client_id,updated_masterlist,rev_order_df):
    # Reading and combining old campaign files 
    old_campaign_df = read_old_campaign(uri,client_id,rev_order_df)

    # Filtering Out Existing Old Campaign from Updated Masterlist
    updated_masterlist = updated_masterlist[updated_masterlist['Type_1'] != 'Old Campaign']
    # Merging with Old Campaign rows with the Updated
    temp_table = updated_masterlist[['Date', 'GA_Revenue', 'GA_Orders', 'GA_Sessions']].copy()
    #temp_table['Date'] = pd.to_datetime(temp_table['Date'], format='%Y%m%d')

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