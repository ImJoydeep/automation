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
# import datetime as dt
import requests

main_module = __import__('main')
client_name = 'Dooney and Bourke'
dbx_path = '/Client/DooneyAndBourke/Internal Files/Reporting/'

def update_masterlist(uri,client_id,client_esp):
    esp_module = __import__(client_esp)
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    merged_workflow_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    master_df = pd.DataFrame()
    master_gacalcu_df = pd.DataFrame()
    master_gacalcu_original_df = pd.DataFrame()
    session_df = pd.DataFrame()
    revenue_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    trigger_ga_rev_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    trigger_ga_ses_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    master_original_df = pd.DataFrame()
    keyword_dashboard_df = pd.DataFrame()
    broadcast_dashboard_df = pd.DataFrame()
    list_dashboard = pd.DataFrame()
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    # Read All files
    # Reading Campaign and Trigger Files
    ## check zip file
    # print("start zip check---------------")
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_module.checkZipFile(client_id,'tire_buyer-zipfile.zip',archive_datetime)
    print("start reading files----------------")
    list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/dooneyandbourke/2023_0702-0708/' #local
    # list_file = os.listdir(uri)
    sourcelistfiles = []
    for file in list_file:
        file_name = str(file.name).replace(client_id+'/','') # REMOTE DEPLOY
        # file_name =  file # local
        if 'MessageActivity' in file_name:
            try:
                # df = pd.read_csv(uri + file_name) #remote
                df = pd.read_csv(uri + file_name) # local
                campaign_data_df = campaign_data_df.append(df)
                masterfile_filter_start_date = esp_module.get_trigger_dates(file_name,-2,"date")
                masterfile_filter_end_date = esp_module.get_trigger_dates(file_name,-1,"date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
                sourcelistfiles.append(file_name) 
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'rev-Campaign Analytics' in file_name:
            try:
                # campaign_analytics_data_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                campaign_analytics_data_rev_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Total revenue","Conversions"]) # local
                campaign_analytics_data_rev_df = campaign_analytics_data_rev_df.dropna()
                campaign_analytics_data_rev_df.rename(
                    columns={
                        "Campaign":"Session campaign"
                    },
                    inplace=True
                )
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'ses-Campaign Analytics' in file_name:
            try:
                # campaign_analytics_data_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                campaign_analytics_data_ses_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Session campaign","Sessions"]) # local
                campaign_analytics_data_ses_df = campaign_analytics_data_ses_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'SplitTestActivity' in file_name:
            try:
                abTesting_df = pd.read_csv(uri + file_name)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name) # local
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "TriggerGArev" in file_name:
            try:
                correct_filename = file_name
                correct_filename = correct_filename.replace(" ","")
                correct_filename = correct_filename.replace("-","_")
                correct_filename = correct_filename.replace("_","-",1)
                each_trigger_ga_df = pd.DataFrame()
                trigger_date = esp_module.get_trigger_dates(correct_filename,-2,"date")
                #get the trigger start date and end date    
                s1 = esp_module.get_trigger_dates(correct_filename,-2,"date")
                s2 = esp_module.get_trigger_dates(correct_filename,-1,"date")
                if trigger_start_date == "":
                    trigger_start_date = s1
                elif trigger_start_date > s1:
                    trigger_start_date = s1
                ## end date for multiple trigger file
                if trigger_end_date == "":
                    trigger_end_date = s2
                elif trigger_end_date < s2:
                    trigger_end_date = s2
                
                each_trigger_ga_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Total revenue","Conversions"])
                each_trigger_ga_df = each_trigger_ga_df.dropna()
                each_trigger_ga_df.rename(
                    columns={
                        "Conversions": "GA Orders",
                        "Total revenue" : "GA Revenue"
                    },
                    inplace=True
                )
                each_trigger_ga_df["triggerDate"] = trigger_date
                # each_trigger_ga_df.drop(each_trigger_ga_df[each_trigger_ga_df['Campaign'].isnull().values.any()].index, inplace = True)
                trigger_ga_rev_df = trigger_ga_rev_df.append(each_trigger_ga_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("trigger ga Error",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
            
        elif "TriggerGAses" in file_name:
            try:
                correct_filename = file_name
                correct_filename = correct_filename.replace(" ","")
                correct_filename = correct_filename.replace("-","_")
                correct_filename = correct_filename.replace("_","-",1)
                each_trigger_ga_df = pd.DataFrame()
                trigger_date = esp_module.get_trigger_dates(correct_filename,-2,"date")
                each_trigger_ga_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Session campaign","Sessions"])
                each_trigger_ga_df = each_trigger_ga_df.dropna()
                each_trigger_ga_df.rename(
                    columns={
                        "Session campaign" : "Campaign",
                        "Sessions" :"GA Sessions"
                    },
                    inplace=True
                )
                each_trigger_ga_df["triggerDateses"] = trigger_date
                # each_trigger_ga_df.drop(each_trigger_ga_df[each_trigger_ga_df['Campaign'].isnull().values.any()].index, inplace = True)
                trigger_ga_ses_df = trigger_ga_ses_df.append(each_trigger_ga_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("trigger ga Error",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'ConversationActivity' in file_name:
            trigger_date = esp_module.get_trigger_dates(file_name,-2,"date")
            try:
                # df = pd.read_csv(uri + file_name) #remote
                tdf = pd.read_csv(uri + file_name)
                tdf['Date'] = trigger_date
                trigger_data_df = trigger_data_df.append(tdf)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'KeywordDashboard' in file_name:
            try:
                kd_df = pd.read_csv(uri + file_name)
                keyword_dashboard_df = keyword_dashboard_df.append(kd_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
            
        elif 'BroadcastDashboard' in file_name:
            try:
                bd_df = pd.read_csv(uri + file_name)
                broadcast_dashboard_df = broadcast_dashboard_df.append(bd_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'ListSubscriptionCampaignDashboard' in file_name:
            try:
                ld_df = pd.read_csv(uri + file_name)
                list_dashboard = list_dashboard.append(ld_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "Dooney And Bourke Masterfile" in file_name:
            try:
                ## get the file name:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = esp_module.convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        master_df = pd.read_excel(xls, "Data")
                        master_gacalcu_df = pd.read_excel(xls, "Old Campaign",header=[0, 1])
                    sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr ====>",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
        elif "ses.csv" in file_name:
            try:
                session_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Date","Sessions"],dtype={"Date": str})
                session_df.rename(
                    columns = {
                        "Date":"Day Index"
                    },inplace=True
                )
                session_df = session_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "rev.csv" in file_name:
            try:
                revenue_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Date","Total revenue","Conversions"],dtype={"Date": str})
                revenue_df.rename(
                    columns = {
                        "Date":"Day Index",
                        "Total revenue":"Revenue",
                        "Conversions" :"Transactions"
                    },inplace=True
                )
                revenue_df = revenue_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "ses_sms.csv" in file_name:
            try:
                attentive_ses_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Date","Sessions"],dtype={"Date": str})
                attentive_ses_df.rename(
                    columns = {
                        "Date":"Day Index"
                    },inplace=True
                )
                attentive_ses_df = attentive_ses_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "rev_sms.csv" in file_name:
            try:
                attentive_rev_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Date","Total revenue","Conversions"],dtype={"Date": str})
                attentive_rev_df.rename(
                    columns = {
                        "Date":"Day Index",
                        "Total revenue":"Revenue",
                        "Conversions" :"Transactions"
                    },inplace=True
                )
                attentive_rev_df = attentive_rev_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"    
    #Operation on files
    matric_column = ["Sent","Delivered","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders","GA Revenue","GA Orders","GA Sessions","Data Acquisitions","Mobile Originated Messages"]
    ## merge the trigger google analytics data
    print("pass 1")
    trigger_ga_df = pd.merge(trigger_ga_rev_df,trigger_ga_ses_df, on="Campaign",how="outer")
    trigger_ga_df.loc[trigger_ga_df["triggerDate"].isnull(),'triggerDate'] = trigger_ga_df["triggerDateses"]
    trigger_ga_dup1_df = trigger_ga_df.loc[trigger_ga_df.duplicated(['Campaign','GA Revenue','GA Orders','triggerDate'], keep="first"),:]
    trigger_ga_df.drop_duplicates(['Campaign','GA Revenue','GA Orders','triggerDate'],inplace=True,keep="first")
    trigger_ga_dup2_df = trigger_ga_df.loc[trigger_ga_df.duplicated(['Campaign','GA Sessions','triggerDateses'], keep="first"),:]
    trigger_ga_df.drop_duplicates(['Campaign','GA Sessions','triggerDateses'],inplace=True,keep="first")
    trigger_ga_dup1_df.loc[:,["GA Revenue","GA Orders"]] = [0,0]
    trigger_ga_dup2_df.loc[:,["GA Sessions"]] = [0]
    trigger_ga_df = pd.concat([trigger_ga_df,trigger_ga_dup1_df])
    trigger_ga_df = pd.concat([trigger_ga_df,trigger_ga_dup2_df])
    trigger_ga_df.loc[trigger_ga_df["triggerDate"] != trigger_ga_df["triggerDateses"] ,'triggerDate'] = trigger_ga_df["triggerDateses"]
    trigger_ga_df = trigger_ga_df.drop(columns=["triggerDateses"])
    trigger_ga_df = trigger_ga_df.fillna(0)
    ## attentive data
    
    ##########
    print("EMAIL")
    # find the name of date column It will handle the two different column name like send date utc 04 and send date utc 05
    cam_col_list1 = campaign_data_df.columns
    cam_col_list = []
    for cam_col in cam_col_list1:
        if "Send Date" in cam_col:
            cam_col_list.append("Date")
        else:
            cam_col_list.append(cam_col)
            
    # print(cam_col_list)
    campaign_data_df = campaign_data_df.set_axis(cam_col_list,axis="columns")
    # remove time
    # campaign_data_df['Date'] = campaign_data_df['Date'].apply(lambda x: x.strip().split(" ")[0])
    campaign_data_df['Date'] = campaign_data_df['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
    # remove extra column (%)
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV','Pass Along','Clicks','Visits','Reads'])
    #print(campaign_data_df.columns)
    #rename the column
    rename_col ={
        "Campaign":"Name",
        "Subject":"Subject Line",
        "Delivered":"Delivered",
        "Bounces" :"Total Bounce",
        "Unsubs.":"Unsub",
        "Opens":"Opens",
        "Unique Clickers":"Clicks",
        "Revenue":"Revenue",
        "Conversions":"Orders",
        "Google Analytics Campaign Name":"Google Analytics Campaign Name",
        "Google Analytics Campaign Content":"Google Analytics Campaign Content",
        "Listrak Conversion Analytics Campaign Name":"Listrak Conversion Analytics Campaign Name",
        "Listrak Conversion Analytics Version":"Segment",
        "Listrak Conversion Analytics Module Name":"Listrak Conversion Analytics Module Name"
    }
    campaign_data_df.rename(
        columns=rename_col,
        inplace=True)
    # add header column
    campaign_data_df["Variant"]=""
    #### pull GA Revenue, GA Order, and GA Session data from GA TAB campaign_analytics_data_df Campaign Analytics.csv file
    campaign_analytics_data_df = pd.merge(campaign_analytics_data_rev_df,campaign_analytics_data_ses_df, on="Session campaign",how="outer")
    campaign_analytics_data_df = campaign_analytics_data_df.fillna(0)
    ga_rename_col ={
        "Session campaign" :"ga_campaign",
        "Total revenue":"GA Revenue",
        "Conversions": "GA Orders",
        "Sessions" : "GA Sessions"
    }
    campaign_analytics_data_df.rename(
        columns=ga_rename_col,
        inplace=True)
    campaign_data_df = pd.merge(campaign_data_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    # add mailing column
    campaign_data_df["Mailing"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]
    print(campaign_data_df.shape)
    # prepare AB Testing df
    if not abTesting_df.empty:
        # find the name of date column
        ab_col_list1 = abTesting_df.columns
        ab_col_list = []
        for cam_col in ab_col_list1:
            if "Send Date" in cam_col:
               ab_col_list.append("Date")
            else:
                ab_col_list.append(cam_col)
        abTesting_df = abTesting_df.set_axis(ab_col_list,axis="columns")
        # remove time
        # abTesting_df['Date'] = abTesting_df['Date'].apply(lambda x: x.strip().split(" ")[0])
        abTesting_df['Date'] = abTesting_df['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
        # remove extra column (%)
        abTesting_df = abTesting_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','Visits','AOV','Pass Along','Reads'])
        #rename column of AB Testing
        ab_rename_col = rename_col.copy()
        ab_rename_col["Split Test"] = "Name"
        # ab_rename_col["Clicks"] = "Clicks"
        ab_rename_col["Group Name"] = "Variant"

        abTesting_df.rename(
            columns=ab_rename_col,
            inplace=True
        )
        campaign_data_df = campaign_data_df.drop(columns=["Variant"])
        left_on = ["Name","Subject Line","Sent","Delivered","Listrak Conversion Analytics Campaign Name","Segment","Date","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders","Google Analytics Campaign Name","Google Analytics Campaign Content","Listrak Conversion Analytics Module Name"]
        campaign_data_df = pd.merge(campaign_data_df,abTesting_df,left_on=left_on,right_on=left_on,how="left")
        # campaign_data_df = pd.concat([campaign_data_df,abTesting_df],ignore_index=True)
    campaign_data_df.reset_index(inplace=True, drop=True)
    campaign_data_df["Campaign"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"].str.split("-", n = 1, expand = True)[1].str.rstrip()
    campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df["Type 3"] = ""
    campaign_data_df["Type 2"] = "Email"
    # pull email link creatives data
    ## remove extra data VA and VB
   
    campaign_data_df["Mailing"] = campaign_data_df["Mailing"].apply(lambda x :esp_module.remove_extra_data(x))
    campaign_data_df["Campaign"] = campaign_data_df["Campaign"].apply(lambda x :esp_module.remove_extra_data(x))
    ## change variant, type 0 and type 1 based on Seed-list Segment value
    campaign_data_df.loc[campaign_data_df["Segment"] == 'Seed-list',["Variant","Type 1"]] = ["Seed Test","Test"]
    campaign_data_df["Data Acquisitions"]="0"
    campaign_data_df["Mobile Originated Messages"]="0"
    campaign_data_df = esp_module.convert_str_to_num(campaign_data_df,matric_column)
    #trigger tab data
    trig_col_list1 = trigger_data_df.columns
    trig_col_list = []
    for trig_col in trig_col_list1:
        if "Publish Date" in trig_col:
            trig_col_list.append("Publish Date")
        else:
            trig_col_list.append(trig_col)
    
    trigger_data_df = trigger_data_df.set_axis(trig_col_list,axis="columns")
    trigger_data_df = trigger_data_df.drop(columns = ['%','%.1','%.2','%.3','%.4','Reads','Clicks','Visits','Conversion Rate','AOV','Pass Along','Publish Date'])
    trigger_rename_col={
        "Thread / Step": "Name",
        "Subject": "Subject Line",
        "Bounces":"Total Bounce",
        "Unsubs." : "Unsub",
        "Opens": "Opens",
        "Clickers" : "Clicks",
        "Conversions": "Orders",
        "Listrak Conversion Analytics Version" : "Segment"
    }
    trigger_data_df.rename(
        columns=trigger_rename_col,
        inplace= True
    )
    ## remove extra row having sent 0
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,["Sent"])
    trigger_data_df = trigger_data_df[trigger_data_df["Sent"] > 0]
    
    trigger_data_df["Delivered"] = trigger_data_df["Sent"]
    #pull the value of GA order, GA Session, GA Revenue
    print("trigger operation===========>")
    trigger_data_df_original = trigger_data_df.copy()
    trigger_data_df = pd.merge(trigger_data_df,trigger_ga_df,left_on="Google Analytics Campaign Name",right_on="Campaign", how="left")
    #remove duplicate row
    # trigger_data_df.drop(trigger_data_df[(trigger_data_df['Date'] != trigger_data_df['triggerDate']) & (not trigger_data_df['triggerDate'].isnull().values.any())].index, inplace = True)

    # print(trigger_data_df.loc[:,["Name","Date","triggerDate"]])
    trigger_data_df_isnull = trigger_data_df[trigger_data_df[("triggerDate")].isnull()]
    trigger_data_df  = trigger_data_df[trigger_data_df[("triggerDate")].notnull()]
    trigger_data_df.drop(trigger_data_df[trigger_data_df['Date'] != trigger_data_df['triggerDate']].index, inplace = True)
    if not trigger_data_df_isnull.empty:
        trigger_data_df =  pd.concat([trigger_data_df,trigger_data_df_isnull])
        # trigger_data_df.reset_index(inplace=True, drop=True)
        trigger_data_df = trigger_data_df.sort_index(ascending=True)
    ## add missing row from left table
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_df_original])
    trigger_data_df.drop_duplicates(['Name','Subject Line','Google Analytics Campaign Name','Date','Sent','Revenue','Orders'],inplace=True,keep="first")
    del trigger_data_df_original
    #reset index
    trigger_data_df.reset_index(inplace=True, drop=True)
    trigger_data_df[["GA Revenue","GA Orders","GA Sessions"]] = trigger_data_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
    trigger_data_dup_df = trigger_data_df.loc[trigger_data_df.duplicated(['Google Analytics Campaign Name','Date'], keep="first"),:]
    
    trigger_data_df.drop_duplicates(['Google Analytics Campaign Name','Date'],inplace=True,keep="first")
    # FIll the value 0 of GA session, GA, order, GA revenue for duplicate row
    trigger_data_dup_df.loc[:,["GA Revenue","GA Orders","GA Sessions"]] = [0,0,0]
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_dup_df])
    trigger_data_df = trigger_data_df.sort_index(ascending=True)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    trigger_book_df.drop_duplicates(["Name"],inplace=True,keep="last")
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on="Name", how="left") ## made changes
    trigger_data_df["Campaign"]=""
    trigger_data_df["Mailing"]=""
    # trigger_data_df["Test Type"]=""
    trigger_data_df["Variant"]=""
    # trigger_data_df["Offer"]=""
    trigger_data_df["ESP"]="Listrak"
    # trigger_data_df["Test Type"] =""
    # trigger_data_df["Variant"] =""
    # trigger_data_df["Offer"] =""
    trigger_data_df["Segment"] =""
    trigger_data_df["Listrak Conversion Analytics Module Name"] =""
    trigger_data_df["Type 3"] = ""
    trigger_data_df["Type 2"] = "Email"
    trigger_data_df["Data Acquisitions"]="0"
    trigger_data_df["Mobile Originated Messages"]="0"
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,matric_column)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    print("going to process sms data")
    ### broadcast_dashboard_df sms campaign
    if not broadcast_dashboard_df.empty:
        bd_col_list1 = broadcast_dashboard_df.columns
        bd_col_list = []
        for bd_col in bd_col_list1:
            if "Send Date" in bd_col:
                bd_col_list.append("Date")
            else:
                bd_col_list.append(bd_col)
        
        broadcast_dashboard_df = broadcast_dashboard_df.set_axis(bd_col_list,axis="columns")
        broadcast_dashboard_df['Date'] = broadcast_dashboard_df['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
        keydash_rename_col={
            "Broadcast Name": "Name",
            "List Name" : "Segment",
            "Sent Messages" : "Delivered",
            "Total Clicks" : "Clicks",
            "Unique Clicks" : "Unique Clickers",
            "Conversions":"Orders"
        }
        broadcast_dashboard_df.rename(
            columns=keydash_rename_col,
            inplace= True
        )
        # broadcast_dashboard_df["aa_data"]="sms | listrak | "+broadcast_dashboard_df["Name"]
        broadcast_dashboard_df =  pd.merge(broadcast_dashboard_df,campaign_analytics_data_df, left_on="Name", right_on="ga_campaign", how="left")
        broadcast_dashboard_df = broadcast_dashboard_df.drop(columns=["ga_campaign","Capped","Send Status"])
        broadcast_dashboard_df["ESP"]="Listrak SMS"
        broadcast_dashboard_df["Type 0"]="Campaign"
        broadcast_dashboard_df["Type 1"]="Promo"
        broadcast_dashboard_df["Type 3"]=""
        broadcast_dashboard_df["Type 2"]="SMS"
        broadcast_dashboard_df["Segment"]=broadcast_dashboard_df["Name"].str.rsplit("*", n = 1, expand = True)[1].str.strip()
        broadcast_dashboard_df["Mailing"]=broadcast_dashboard_df["Name"].str.rsplit("*", n = 1, expand = True)[0].str.strip()
        broadcast_dashboard_df["Campaign"] = broadcast_dashboard_df["Mailing"].str.split("-", n = 1, expand = True)[1].str.strip()
        broadcast_dashboard_df["Variant"]=""
        # broadcast_dashboard_df["Test Type"]=""
        # broadcast_dashboard_df["Offer"]=""
        broadcast_dashboard_df["Subject Line"]=""
        broadcast_dashboard_df["Sent"]= broadcast_dashboard_df["Delivered"]
        broadcast_dashboard_df["Total Bounce"]="0"
        broadcast_dashboard_df["Opens"]="0"
        broadcast_dashboard_df["Unsub"]="0"
        broadcast_dashboard_df["Google Analytics Campaign Name"]=""
        broadcast_dashboard_df["Google Analytics Campaign Content"]=""
        broadcast_dashboard_df["Listrak Conversion Analytics Campaign Name"]=""
        broadcast_dashboard_df["Listrak Conversion Analytics Version"]=""
        broadcast_dashboard_df["Listrak Conversion Analytics Module Name"]=""
        broadcast_dashboard_df["Workflow"]=""
        broadcast_dashboard_df["Entry Type"]=""
        broadcast_dashboard_df["Mobile Originated Messages"]=""
        broadcast_dashboard_df["Data Acquisitions"]=""
        broadcast_dashboard_df = esp_module.convert_str_to_num(broadcast_dashboard_df,matric_column)
    ####
    ## keywordDashboard sms trigger
    if not keyword_dashboard_df.empty:
        kd_col_list1 = keyword_dashboard_df.columns
        kd_col_list = []
        for kd_col in kd_col_list1:
            if "Day" in kd_col:
                kd_col_list.append("Date")
            else:
                kd_col_list.append(kd_col)
        
        keyword_dashboard_df = keyword_dashboard_df.set_axis(kd_col_list,axis="columns")
        keydash_rename_col={
            "Campaign Name":"Name",
            "Opt-Ins": "Unsub",
            "Mobile Terminated Messages" : "Delivered",
            "Conversions":"Orders",
            "Total Messages" : "Sent"
        }
        keyword_dashboard_df.rename(
            columns=keydash_rename_col,
            inplace= True
        )
        keyword_dashboard_df = esp_module.convert_str_to_num(keyword_dashboard_df,["Sent"])
        keyword_dashboard_df = keyword_dashboard_df[keyword_dashboard_df["Sent"] > 0]
    ####
    #### list subscription Dashboard
    if not list_dashboard.empty:
        print("list_dashboard ",list_dashboard.columns)
        ld_col_list1 = list_dashboard.columns
        ld_col_list = []
        for ld_col in ld_col_list1:
            if "Day" in ld_col:
                ld_col_list.append("Date")
            else:
                ld_col_list.append(ld_col)
        
        list_dashboard = list_dashboard.set_axis(ld_col_list,axis="columns")
        list_dashboard['Date'] = list_dashboard['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
        listdash_rename_col={
            "Campaign Name":"Name",
            "Opt-Ins": "Unsub",
            "Mobile Terminated Messages" : "Delivered",
            "Conversions":"Orders",
            "Total Messages":"Sent"
        }

        list_dashboard.rename(
            columns=listdash_rename_col,
            inplace= True
        )
        list_dashboard = esp_module.convert_str_to_num(list_dashboard,["Sent"])
        list_dashboard = list_dashboard[list_dashboard["Sent"] > 0]
        ## Concat keyword dashboard data and list dashboard data. These are trigger data
        list_dashboard = pd.concat([keyword_dashboard_df,list_dashboard])
        ## add GA Trigger
        list_dashboard["Google Analytics Campaign Name"]=list_dashboard["Name"]
        list_dashboard = pd.merge(list_dashboard,trigger_ga_df,left_on="Google Analytics Campaign Name",right_on="Campaign", how="left")
        list_dashboard_dup_df = list_dashboard.loc[list_dashboard.duplicated(['Google Analytics Campaign Name'], keep="first"),:]
    
        list_dashboard.drop_duplicates(['Google Analytics Campaign Name'],inplace=True,keep="first")
        # FIll the value 0 of GA session, GA, order, GA revenue for duplicate row
        trigger_data_dup_df.loc[:,["GA Revenue","GA Orders","GA Sessions"]] = [0,0,0]
        list_dashboard = pd.concat([list_dashboard,list_dashboard_dup_df])
        list_dashboard = list_dashboard.sort_index(ascending=True)
        list_dashboard =  pd.merge(list_dashboard,trigger_book_df, on="Name", how="left")
        # list_dashboard["ESP"]=""
        # list_dashboard["Type 0"]=""
        # list_dashboard["Type 1"]=""
        list_dashboard["Type 2"]="SMS"
        list_dashboard["Campaign"]=""
        list_dashboard["Mailing"]=""
        list_dashboard["Variant"]=""
        list_dashboard["Subject Line"]=""
        list_dashboard["Segment"]=""
        list_dashboard["Total Bounce"]="0"
        list_dashboard["Opens"]="0"
        list_dashboard["Clicks"]="0"
        list_dashboard["Unique Clickers"]="0"
        # list_dashboard["GA Revenue"]="0"
        # list_dashboard["GA Orders"]="0"
        # list_dashboard["GA Sessions"]="0"
        list_dashboard["Google Analytics Campaign Content"]=""
        list_dashboard["Listrak Conversion Analytics Campaign Name"]=""
        list_dashboard["Listrak Conversion Analytics Version"]=""
        list_dashboard["Listrak Conversion Analytics Module Name"]=""
        list_dashboard["Workflow"]=""
        list_dashboard["Entry Type"]=""
        list_dashboard["Type 3"] =""
        list_dashboard["ESP"]="Listrak SMS"
        list_dashboard = esp_module.convert_str_to_num(list_dashboard,matric_column)
        print("list_dashboard ",list_dashboard.columns)
    ####
    excelcol = ["Name","Campaign","Mailing","Variant","Type 2","Type 3","Subject Line","Date","ESP",	
                "Type 0","Type 1","Sent","Delivered","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders",
                "Google Analytics Campaign Name","Google Analytics Campaign Content",
                "Listrak Conversion Analytics Campaign Name","Segment","Listrak Conversion Analytics Module Name",
                "GA Revenue","GA Orders","GA Sessions","Data Acquisitions","Mobile Originated Messages"]
    #re-arrange the columns
    print("pass 3")
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    broadcast_dashboard_df = broadcast_dashboard_df[excelcol]
    list_dashboard = list_dashboard[excelcol]
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'Dooney_and_Bourke_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer1 = pd.ExcelWriter(uri+newreportfilename, engine="xlsxwriter")
        campaign_data_df.to_excel(writer1, sheet_name="Promo",index=False)
        broadcast_dashboard_df.to_excel(writer1, sheet_name="SMS_Campaign",index=False)
        trigger_data_df.to_excel(writer1, sheet_name="Trigger",index=False)
        list_dashboard.to_excel(writer1, sheet_name="SMS_Trigger",index=False)

        workbook1 = writer1.book
        worksheet_promo = writer1.sheets["Promo"]
        worksheet_trigger = writer1.sheets["Trigger"]
        # Add some cell formats.
        format1 = workbook1.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_promo.set_column(18, 18, None, format1)
        worksheet_promo.set_column(25, 25, None, format1)
        worksheet_trigger.set_column(18, 18, None, format1)
        worksheet_trigger.set_column(25, 25, None, format1)
        writer1.save()
        writer1.close()
    ###### master file ######
    if not master_df.empty:
        print(master_df.columns)
        master_df["Date"] = master_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x))
        master_original_df = master_df.copy()
        master_df = master_df[(master_df["Date"] >= masterfile_filter_start_date) & (master_df["Date"] <= masterfile_filter_end_date)]
        master_original_df.drop(master_original_df[(master_original_df['Date'] >= masterfile_filter_start_date) & (master_original_df['Date'] <= masterfile_filter_end_date)].index, inplace = True)
        ## calcu tab
        master_gacalcu_df[("Final","Date")] = master_gacalcu_df[("Final","Date")].apply(lambda x: esp_module.convert_str_to_date(x))
        master_gacalcu_df[("SMS","Date")] = master_gacalcu_df[("SMS","Date")].apply(lambda x: esp_module.convert_str_to_date(x))
        master_gacalcu_original_df = master_gacalcu_df.copy()
        print("master_gacalcu_original_df shape ",master_gacalcu_original_df.shape)
        master_gacalcu_df = master_gacalcu_df[(master_gacalcu_df["Final"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_df["Final"]["Date"] <= masterfile_filter_end_date)]
        master_gacalcu_original_df.drop(master_gacalcu_original_df[(master_gacalcu_original_df["Final"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_original_df["Final"]["Date"] <= masterfile_filter_end_date)].index, inplace = True)
        print("master_gacalcu_original_df shape1 ",master_gacalcu_original_df.shape)
        ## filter with type 0 = Campaign and Type 1 = Promo
    
        master_df.drop(master_df[(master_df["Type 0"] == "Campaign" ) & (master_df["Type 1"] == "Promo")].index, inplace = True)
        master_df.drop(master_df[(master_df["Type 1"] == "Old Campaign") | (master_df["Type 1"] == "Old Campaign") ].index, inplace = True)
        # paste the data from promo tab, workflow tab, and trigger tab to master file
        master_df = pd.concat([master_df,campaign_data_df])
        master_df = pd.concat([master_df,trigger_data_df])
        ## for sms
        master_df = pd.concat([master_df,broadcast_dashboard_df])
        master_df = pd.concat([master_df,list_dashboard])
        # master_df = master_df.drop(columns="triggerDate")
    # add seven row in ga calcu sheet
    #typecast the state date and end date to string
    old_campaign_data = []
    old_campaign_data_sms = []
    old_campaign_final_data_df = []
    old_campaign_final_data_sms_df = []
    if not master_gacalcu_df.empty:
        mydata = []
        delta = masterfile_filter_end_date - masterfile_filter_start_date
        noofdays = delta.days
        print("no of days",noofdays)
        for x in range(noofdays+1):
            # newsdate =  trigger_start_date + timedelta(days=x)
            newsdate =  masterfile_filter_start_date + timedelta(days=x)
            # each_date = newsdate.strftime("%m/%d/%Y")
            #sum if date wise and name not equal to old campaign
            revenue_data = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign") & (master_df['ESP'] != "Listrak SMS"), "GA Revenue"].sum()
            order_data = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak SMS"), "GA Orders"].sum()
            session_data = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak SMS"), "GA Sessions"].sum()
            ## sms
            revenue_data_sms = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak"), "GA Revenue"].sum()
            order_data_sms = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak"), "GA Orders"].sum()
            session_data_sms = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak"), "GA Sessions"].sum()
            ###
            mydata.append([np.nan,revenue_data,order_data,session_data,0,0,0,newsdate,0,0,0,revenue_data_sms,order_data_sms,session_data_sms,0,0,0,newsdate,0,0,0])
            old_campaign_data.append(["Old Campaign",np.nan,np.nan,np.nan,np.nan,newsdate,np.nan,'Listrak','Campaign','Old Campaign','Email',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan,np.nan])
            old_campaign_data_sms.append(["Old Campaign",np.nan,np.nan,np.nan,np.nan,newsdate,np.nan,'Listrak SMS','Campaign','Old Campaign SMS','SMS',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan,np.nan])
        master_gacalcu_df = pd.DataFrame(mydata, columns=master_gacalcu_df.columns)
        # master_gacalcu_df = pd.concat([master_gacalcu_df,new_gacalcu_df])
        master_gacalcu_df.reset_index(inplace=True, drop=True)
        ### read session and revenue file session_df , revenue_df
        print("=====> pass2")
        revenue_df = revenue_df.dropna()
        session_df = session_df.dropna()
        revenue_df["Day Index"] = revenue_df["Day Index"].apply(lambda x: esp_module.convert_str_to_date(str(x),'%Y%m%d'))
        session_df["Day Index"] = session_df["Day Index"].apply(lambda x: esp_module.convert_str_to_date(str(x),'%Y%m%d'))
        # combine the session order and revenue data by date
        revenue_df = pd.merge(revenue_df,session_df,on="Day Index",how="inner")
        # revenue_df["Day Index"] = revenue_df["Day Index"].apply(lambda x: datetime.strptime(x,'%m/%d/%Y').date())
        revenue_df = esp_module.convert_str_to_num(revenue_df,["Transactions","Revenue","Sessions"])
        revenue_df.rename(
            columns={
                "Transactions":"GA|Order",
                "Day Index":"Final|Date",
                "Revenue" : "GA|Revenue",
                "Sessions":"GA|Sessions"
            },
            inplace=True
        )
        print("======> pass 2 SMS")
        attentive_rev_df = attentive_rev_df.dropna()
        attentive_ses_df = attentive_ses_df.dropna()
        attentive_rev_df["Day Index"] = attentive_rev_df["Day Index"].apply(lambda x: esp_module.convert_str_to_date(x,'%Y%m%d'))
        attentive_ses_df["Day Index"] = attentive_ses_df["Day Index"].apply(lambda x: esp_module.convert_str_to_date(x,'%Y%m%d'))
        # combine the session order and revenue data by date
        attentive_rev_df = pd.merge(attentive_rev_df,attentive_ses_df,on="Day Index",how="inner")
        # attentive_rev_df["Day Index"] = attentive_rev_df["Day Index"].apply(lambda x: datetime.strptime(x,'%m/%d/%Y').date())
        attentive_rev_df = esp_module.convert_str_to_num(attentive_rev_df,["Transactions","Revenue","Sessions"])
        attentive_rev_df.rename(
            columns={
                "Transactions":"GASMS|Order",
                "Day Index":"Final|Date",
                "Revenue" : "GASMS|Revenue",
                "Sessions":"GASMS|Sessions"
            },
            inplace=True
        )
        
        master_gacalcu_df.drop(columns=["GA","GASMS"],inplace=True)
        master_gacalcu_df.columns = master_gacalcu_df.columns.map('|'.join).str.strip('|')
        master_gacalcu_df = pd.merge(master_gacalcu_df,revenue_df,on="Final|Date",how="left")
        master_gacalcu_df = pd.merge(master_gacalcu_df,attentive_rev_df,on="Final|Date",how="left")
        print("pass 4")
        #convert str to number
        master_gacalcu_df = esp_module.convert_str_to_num(master_gacalcu_df,["GA|Revenue","GA|Order","GA|Sessions","ESP|Revenue","ESP|Order","ESP|Sessions","GASMS|Revenue","GASMS|Order","GASMS|Sessions","ESPSMS|Revenue","ESPSMS|Order","ESPSMS|Sessions"])
        master_gacalcu_df["Final|Revenue"] = master_gacalcu_df["GA|Revenue"] - master_gacalcu_df["ESP|Revenue"]
        master_gacalcu_df["Final|Order"] = master_gacalcu_df["GA|Order"] - master_gacalcu_df["ESP|Order"]
        master_gacalcu_df["Final|Sessions"] = master_gacalcu_df["GA|Sessions"] - master_gacalcu_df["ESP|Sessions"]
        ## sms
        master_gacalcu_df["SMS|Revenue"] = master_gacalcu_df["GASMS|Revenue"] - master_gacalcu_df["ESPSMS|Revenue"]
        master_gacalcu_df["SMS|Order"] = master_gacalcu_df["GASMS|Order"] - master_gacalcu_df["ESPSMS|Order"]
        master_gacalcu_df["SMS|Sessions"] = master_gacalcu_df["GASMS|Sessions"] - master_gacalcu_df["ESPSMS|Sessions"]
        ###
        old_campaign_final_data_df = master_gacalcu_df[["Final|Revenue","Final|Order","Final|Sessions","Final|Date"]].copy()
        old_campaign_final_data_sms_df = master_gacalcu_df[["SMS|Revenue","SMS|Order","SMS|Sessions","SMS|Date"]].copy()
        mycol = master_gacalcu_df.columns
        tuple_col =[]
        for c in mycol:
            newcol = c.split("|")
            newcol_tup = tuple(newcol)
            tuple_col.append(newcol_tup)
        # create multi index dataframe
        mycol1 = pd.MultiIndex.from_tuples(tuple_col)
        print("pass 5")
        master_gacalcu_df = master_gacalcu_df.set_axis(mycol1,axis='columns')
        # reording columns
        master_gacalcu_df = master_gacalcu_df[["ESP","GA","Final","ESPSMS","GASMS","SMS"]]
        master_gacalcu_original_df = master_gacalcu_original_df[["ESP","GA","Final","ESPSMS","GASMS","SMS"]]
        # print(master_gacalcu_df)
        print("pass 6")
        master_gacalcu_original_df = pd.concat([master_gacalcu_original_df, master_gacalcu_df])
        master_gacalcu_original_df = master_gacalcu_original_df[master_gacalcu_original_df[("Final","Date")].notnull()]
        master_gacalcu_original_df[("Final","Date")] = master_gacalcu_original_df[("Final","Date")].apply(lambda x: x.strftime("%m/%d/%Y"))
        master_gacalcu_original_df.reset_index(inplace=True,drop=True)
    ### add final revenue, order and session to old campaign in master file data tab
    if not master_df.empty:
        # old_campaign_master_df = master_df[master_df["Type 1"] == "Old Campaign"]
        ## remove the taken dataset
        ## create new Data frame for old campaign
        old_campaign_master_df = pd.DataFrame(old_campaign_data, columns=master_df.columns)
        old_campaign_master_sms_df = pd.DataFrame(old_campaign_data_sms, columns=master_df.columns)
        # old_campaign_master_df = pd.concat([old_campaign_master_df,old_campaign_data_df])
        old_campaign_master_df.drop(columns=["GA Revenue","GA Orders","GA Sessions"], inplace=True)
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        old_campaign_master_sms_df.drop(columns=["GA Revenue","GA Orders","GA Sessions"], inplace=True)
        old_campaign_master_sms_df.reset_index(inplace=True,drop=True)
        #pull the final data from ga calcu
        old_campaign_final_data_df.rename(columns={
            "Final|Revenue" : "GA Revenue",
            "Final|Order" : "GA Orders",
            "Final|Sessions":"GA Sessions",
            "Final|Date" :"Date"
        },inplace=True)
        old_campaign_master_df = pd.merge(old_campaign_master_df,old_campaign_final_data_df,on="Date",how="left")
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        master_df = pd.concat([master_df,old_campaign_master_df])
        ## for sms
        old_campaign_final_data_sms_df.rename(columns={
            "SMS|Revenue" : "GA Revenue",
            "SMS|Order" : "GA Orders",
            "SMS|Sessions":"GA Sessions",
            "SMS|Date" :"Date"
        },inplace=True)
        old_campaign_master_sms_df = pd.merge(old_campaign_master_sms_df,old_campaign_final_data_sms_df,on="Date",how="left")
        old_campaign_master_sms_df.reset_index(inplace=True,drop=True)
        master_df = pd.concat([master_df,old_campaign_master_sms_df])
        ###
        master_df.reset_index(inplace=True,drop=True)
        #update into master file
        
        master_original_df.reset_index(inplace=True,drop=True)
        master_original_df = pd.concat([master_original_df,master_df])
        # master_original_df.reset_index(inplace=True,drop=True)
        newfilename = 'Dooney_and_bourke-Dooney And Bourke Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer = pd.ExcelWriter(uri+newfilename, engine="xlsxwriter")
        master_original_df.to_excel(writer, sheet_name="Data",index=False)
        master_gacalcu_original_df.to_excel(writer, sheet_name="Old Campaign",index=True)
        workbook = writer.book
        worksheet_data = writer.sheets["Data"]
        worksheet_gacalcu = writer.sheets["Old Campaign"]
        # Add some cell formats.
        format1 = workbook.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_data.set_column(20, 20, None, format1)
        worksheet_data.set_column(27, 27, None, format1)
        worksheet_gacalcu.set_column(1, 1, None, format1)
        worksheet_gacalcu.set_column(4, 4, None, format1)
        worksheet_gacalcu.set_column(8, 8, None, format1)
        worksheet_gacalcu.set_column(11, 11, None, format1)
        worksheet_gacalcu.set_column(14, 14, None, format1)
        worksheet_gacalcu.set_column(18, 18, None, format1)
        writer.save()
        writer.close()
        ## send to dropbox
        print("going for dropbox")
        if os.environ['USEDROPBOX'] == 'yes':
            dropboxdf = master_original_df.copy()
            dropboxdf['Date'] = pd.to_datetime(dropboxdf['Date']).dt.strftime('%m/%d/%Y')
            main_module.to_dropbox(dropboxdf,dbx_path,client_name)

        ## insert data to bigquery
        col_rename = {
            "Type 2" :"Type_2",
            "Subject Line" : "Subject_Line",
            "Type 0" :"Type_0",
            "Type 1" :"Type_1",
            "Type 3" :"Type_3",
            "Total Bounce" : "Total_Bounces",
            "Google Analytics Campaign Name" :"Google_Analytics_Campaign_Name",
            "Google Analytics Campaign Content" : "Google_Analytics_Campaign_Content",
            "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Version" : "Listrak_Conversion_Analytics_Version",
            "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
            "GA Revenue" : "GA_Revenue",
            "GA Orders" : "GA_Orders",
            "GA Sessions" : "GA_Sessions",
            "Data Acquisitions":"Data_Acquisitions",
            "Mobile Originated Messages" :"Mobile_Originated_Messages"
        }
        master_original_df.rename(
            columns=col_rename,
            inplace=True
        )
        missing_str_cols = ["Offer","Test_Type","Unnamed_colon__30","Unnamed_colon__31","Unnamed_colon__32",'Segment_Engagement','Segment_BNB','Segment_Freq','Error']
        missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','Complaints']
        for i in missing_str_cols:
            master_original_df[i] = '-'
        for i in missing_int_cols:
            master_original_df[i] = 0.0
        master_original_df['Original_Segment'] = master_original_df['Segment']
        master_original_df['FISCAL_YEAR'] = pd.to_datetime(master_original_df['Date']).dt.year
        master_original_df['FISCAL_YEAR_START'] = master_original_df['Date']
        master_original_df['FISCAL_WEEK'] = pd.to_datetime(master_original_df['Date']).dt.isocalendar().week
        master_original_df['FISCAL_MONTH'] = pd.to_datetime(master_original_df['Date']).dt.month
        master_original_df['FISCAL_QUARTER'] = pd.to_datetime(master_original_df['Date']).dt.quarter
        cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","ESP","Type_3","Google_Analytics_Campaign_Name","Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name","Data_Acquisitions","Mobile_Originated_Messages","Unnamed_colon__30","Unnamed_colon__31","Unnamed_colon__32","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK","FISCAL_MONTH","FISCAL_QUARTER"]
        master_original_df = master_original_df[cols_all]
        master_original_df.to_csv(uri+'Masterlist.csv',index=False)
        # remove previous version of data  and insert new version of data
        main_module.delete_table(client_id)
        bigquery_insert(client_id)
        ### move the file to archive folder
        sourcelistfiles.append(newfilename)
        sourcelistfiles.append(newreportfilename)
        sourcelistfiles.append('Masterlist.csv')
        for sfile in sourcelistfiles:
            main_module.copy_blob('reporter-etl',client_id + '/' + sfile,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +'/'+ sfile)
            if newfilename != sfile:
                main_module.delete_blob('reporter-etl',client_id + '/' + sfile)
    print(">>>>>>>>>>>>>>>> end")
    return ""
def bigquery_insert(client_id):
    # Bigquery Schema
    print("goint to insert in bigquery")
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
        # bigquery.SchemaField('Name_Bkp', 'STRING'),
        # bigquery.SchemaField('Reads', 'STRING'),
        # bigquery.SchemaField('Total_Clicks', 'STRING'),
        # bigquery.SchemaField('Visits', 'STRING'),
        # bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Module_Name', 'STRING'),
        bigquery.SchemaField('Data_Acquisitions', 'STRING'),
        bigquery.SchemaField('Mobile_Originated_Messages', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__30', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__31', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__32', 'STRING'),
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
    return "ok"
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
        # bigquery.SchemaField('Reads', 'STRING'),
        # bigquery.SchemaField('Total_Clicks', 'STRING'),
        # bigquery.SchemaField('Visits', 'STRING'),
        # bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Module_Name', 'STRING'),
        bigquery.SchemaField('Data_Acquisitions', 'STRING'),
        bigquery.SchemaField('Mobile_Originated_Messages', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__30', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__31', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__32', 'STRING'),
        bigquery.SchemaField('Original_Segment', 'STRING'),
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