from base64 import encode
import numpy as np
import pandas as pd
import os
from ca_global_industrial import update_CA_globalindustrial
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
import datetime as dt
import requests

main_module = __import__('main')
client_name = 'Global Industrial'
dbx_path = '/Client/Global Industrial/Internal Files/Reporting/'

def update_masterlist(uri,client_id,client_esp):
    esp_module = __import__(client_esp)
    ####### Campaign
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    campaign_ad_rev_df = pd.DataFrame()
    campaign_ad_ses_df = pd.DataFrame()
    campaign_ad_analytics_data_df = pd.DataFrame()
    mailingtagging_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    ########trigger
    trigger_data_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame() 
    trigger_ga_rev_df = pd.DataFrame()
    trigger_ga_ses_df = pd.DataFrame()
    ########master
    master_df = pd.DataFrame()
    master_gacalcu_df = pd.DataFrame()
    master_gacalcu_original_df = pd.DataFrame()
    session_df = pd.DataFrame()
    revenue_df = pd.DataFrame()
    master_rev_df = pd.DataFrame()
    master_ses_df = pd.DataFrame()
    master_original_df = pd.DataFrame()
    #######sms
    keyword_dashboard_df = pd.DataFrame()
    broadcast_dashboard_df = pd.DataFrame()
    list_dashboard = pd.DataFrame()

    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    week_range=[]
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    # Read All files
    # Reading Campaign and Trigger Files
    ## check zip file
    # print("start zip check---------------")
    # archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    # main_module.checkZipFile(client_id,'ilovedooney-zipfile.zip',archive_datetime)
    print("start reading files----------------")
    # list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    uri = '/home/nav93/Downloads/AlchemyWroxFiles/Global_Industrial/2023_0820-0826/' #local
    list_file = os.listdir(uri)
    sourcelistfiles = []
    for file in list_file:
        # file_name = str(file.name).replace(client_id+'/','') # REMOTE DEPLOY
        file_name =  file # local
        if 'US-MessageActivity' in file_name:
            try:
                # df = pd.read_csv(uri + file_name) #remote
                df = pd.read_csv(uri + file_name ,encoding='unicode_escape') # local
                campaign_data_df = campaign_data_df.append(df)
                masterfile_filter_start_date = esp_module.get_trigger_dates(file_name,-2,"date")
                masterfile_filter_end_date = esp_module.get_trigger_dates(file_name,-1,"date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
                sourcelistfiles.append(file_name) 
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'US-rev-Campaign-ad' in file_name:
            try:
                campaign_ad_rev_df = pd.read_csv(uri + file_name,skiprows=6,encoding='unicode_escape',usecols=["Campaign","Purchase revenue","Conversions"],dtype= {'Campaign': str}) # local
                campaign_ad_rev_df.rename(columns={"Purchase revenue":"Total revenue"}, inplace=True)
                campaign_ad_rev_df = campaign_ad_rev_df.dropna()
                campaign_ad_rev_df.rename(
                    columns={
                        "Campaign":"Session campaign"
                    },
                    inplace=True
                )
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "Error"
        elif 'US-ses-campaign-ad' in file_name:
            try:
                campaign_ad_ses_df = pd.read_csv(uri + file_name,skiprows=6,encoding='unicode_escape',usecols=["Session campaign","Sessions"],dtype= {'Session campaign': str}) # local
                campaign_ad_ses_df.rename(columns={"Purchase revenue":"Total revenue"}, inplace=True)
                campaign_ad_ses_df = campaign_ad_ses_df.dropna()
                campaign_ad_ses_df.rename(
                    columns={
                        "Campaign":"Session campaign"
                    },
                    inplace=True
                )
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("US-ses-campaign-ad ",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "Error"
        elif 'US-rev-Campaign Analytics' in file_name:
            try:
                campaign_analytics_data_rev_df = pd.read_csv(uri + file_name,skiprows=6,encoding='unicode_escape',usecols=["Campaign","Purchase revenue","Conversions"],dtype= {'Campaign': str}) # local
                campaign_analytics_data_rev_df.rename(columns={"Purchase revenue":"Total revenue"}, inplace=True)
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
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "Error"
        elif 'US-ses-Campaign Analytics' in file_name:
            try:
                campaign_analytics_data_ses_df = pd.read_csv(uri + file_name,skiprows=6,encoding='unicode_escape',usecols=["Session campaign","Sessions"], dtype= {'Session campaign': str}) # local
                campaign_analytics_data_ses_df = campaign_analytics_data_ses_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "ses-Campaign error"
        elif "Mailing Tagging" in file_name:
            try:
                mailingtagging_df = pd.read_excel(uri + file_name) # local
                mailingtagging_df = mailingtagging_df[mailingtagging_df["Client"] == "Global Industrial US"]
                mailingtagging_df = mailingtagging_df.drop(columns=["Date","Client"])
                mailingtagging_df.reset_index(inplace=True)
                mailingtagging_df.rename(
                    columns={"Variant":"Variant2"},
                    inplace=True
                )
                # print("mailingtagging_df ",mailingtagging_df.columns)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'SplitTestActivity' in file_name:
            try:
                abTesting_df = pd.read_csv(uri + file_name,encoding='unicode_escape')
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name,encoding='unicode_escape') # local
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "US-TriggerGArev" in file_name:
            try:
                correct_filename = file_name
                print(file_name)
                correct_filename = correct_filename.replace(" ","")
                correct_filename = correct_filename.replace("-","_")
                correct_filename = correct_filename.replace("_","-",1)
                each_trigger_ga_df = pd.DataFrame()
                print("correct_filename :",correct_filename)
                trigger_date = esp_module.get_trigger_dates(correct_filename,-2,"date")
                print("trigger_date :",trigger_date)
                # get the trigger start date and end date 
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
                
                each_trigger_ga_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Purchase revenue","Conversions"])
                each_trigger_ga_df = each_trigger_ga_df.dropna()
                each_trigger_ga_df.rename(
                    columns={
                        "Conversions": "GA Orders",
                        "Purchase revenue" : "GA Revenue"
                    },
                    inplace=True
                )
                each_trigger_ga_df["triggerDate"] = trigger_date
                # each_trigger_ga_df.drop(each_trigger_ga_df[each_trigger_ga_df['Campaign'].isnull().values.any()].index, inplace = True)
                trigger_ga_rev_df = trigger_ga_rev_df.append(each_trigger_ga_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("trigger ga Error rev: ",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error : "+ str(e)
            
        elif "US-TriggerGAses" in file_name:
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
                print("trigger ga Error ses:",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'US-ConversationActivity' in file_name:
            trigger_date = esp_module.get_trigger_dates(file_name,-2,"date")
            try:
                # df = pd.read_csv(uri + file_name) #remote
                tdf = pd.read_csv(uri + file_name,encoding='unicode_escape')
                tdf['Date'] = trigger_date
                trigger_data_df = trigger_data_df.append(tdf)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'KeywordDashboard' in file_name:
            try:
                kd_df = pd.read_csv(uri + file_name,encoding='unicode_escape')
                keyword_dashboard_df = keyword_dashboard_df.append(kd_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
            
        elif 'BroadcastDashboard' in file_name:
            try:
                bd_df = pd.read_csv(uri + file_name,encoding='unicode_escape')
                broadcast_dashboard_df = broadcast_dashboard_df.append(bd_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'ListSubscriptionCampaignDashboard' in file_name:
            try:
                ld_df = pd.read_csv(uri + file_name,encoding='unicode_escape')
                list_dashboard = list_dashboard.append(ld_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "US-Old Campaign rev" in file_name:
            try:
                correct_filename = file_name
                correct_filename = correct_filename.replace(" ","")
                correct_filename = correct_filename.replace("-","_")
                correct_filename = correct_filename.replace("_","-",1)
                start_date = get_old_campaign_dates(correct_filename,-2,"date")
                end_date = get_old_campaign_dates(correct_filename,-1,"date")
                date_rng = {
                    "start_date":start_date,
                    "end_date":end_date
                }
                week_range.append(date_rng)
                try:
                    mrdf = pd.read_csv(uri + file_name,skiprows=5, usecols=['Week','Purchase revenue','Conversions'])
                    mrdf['Start Date'] = start_date
                    mrdf['End Date'] = end_date
                    revenue_df = revenue_df.append(mrdf)
                except Exception as e:
                    print(e)
                    return f"error {e}"
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr ====>",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "US-Old Campaign ses" in file_name:
            try:
                correct_filename = file_name
                correct_filename = correct_filename.replace(" ","")
                correct_filename = correct_filename.replace("-","_")
                correct_filename = correct_filename.replace("_","-",1)
                start_date = get_old_campaign_dates(correct_filename,-2,"date")
                end_date = get_old_campaign_dates(correct_filename,-1,"date")
                try:
                    msdf = pd.read_csv(uri + file_name,skiprows=6, usecols=['Week','Sessions'])
                    msdf['Start Date'] = start_date
                    msdf['End Date'] = end_date
                    session_df = session_df.append(msdf)
                except Exception as e:
                    print(e)
                    return f"error {e}"
                    
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr ====>",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "Global Industrial Masterfile" in file_name:
            try:
                # get the file name:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = esp_module.convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        master_df = pd.read_excel(xls, "Data")
                        master_gacalcu_df = pd.read_excel(xls, "Old Campaign US",header=[0, 1])
                        print(master_gacalcu_df.columns)
                    sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr ====>",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
    
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
    print("pass 2")
    ## attentive data
    print("EMAIL")
    matric_column = ["Sent","Delivered","Total Bounces","Unsub","Opens","Total Clicks","Clicks","Revenue","Orders","GA Revenue","GA Orders","GA Sessions","Data Acquisitions","Mobile Originated Messages"]
    # find the name of date column It will handle the two different column name like send date utc 04 and send date utc 05
    cam_col_list1 = campaign_data_df.columns
    cam_col_list = []
    for cam_col in cam_col_list1:
        if "Send Date" in cam_col:
            cam_col_list.append("Date")
        else:
            cam_col_list.append(cam_col)
    campaign_data_df = campaign_data_df.set_axis(cam_col_list,axis="columns")
    # remove time
    # campaign_data_df['Date'] = campaign_data_df['Date'].apply(lambda x: x.strip().split(" ")[0])
    campaign_data_df['Date'] = campaign_data_df['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
    # remove extra column (%)
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','Visits','AOV','Pass Along','Reads'])
    #print(campaign_data_df.columns)
    #rename the column
    rename_col ={
        "Campaign":"Name",
        "Subject":"Subject Line",
        "Delivered":"Delivered",
        "Bounces" :"Total Bounces",
        "Unsubs.":"Unsub",
        "Opens":"Opens",
        "Clicks" : "Total Clicks",
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

    ########### Campaign AD Processing  ###################
    print("PASS 500")
    campaign_ad_analytics_data_df = pd.merge(campaign_analytics_data_rev_df,campaign_analytics_data_ses_df, on="Session campaign",how="outer")
    campaign_ad_analytics_data_df = campaign_ad_analytics_data_df.fillna(0)
    ga_ad_rename_col ={
        "Session campaign" :"ga_campaign",
        "Total revenue":"GA Revenue",
        "Conversions": "GA Orders",
        "Sessions" : "GA Sessions"
    }
    campaign_ad_analytics_data_df.rename(
        columns=ga_ad_rename_col,
        inplace=True)
    print(campaign_ad_analytics_data_df.columns)
    campaign_data_df = pd.merge(campaign_data_df,campaign_ad_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    ############### Campaign Ad End ##############

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
    # campaign_data_df['Google Analytics Campaign Name'] = campaign_data_df['Google Analytics Campaign Name'].str.strip()
    # campaign_analytics_data_df['ga_campaign'] = campaign_analytics_data_df['ga_campaign'].str.strip()
    print(campaign_data_df.columns)
    campaign_data_df = pd.merge(campaign_data_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    # add mailing column
    campaign_data_df["Mailing"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]

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
        ab_rename_col["Clicks"] = "Clicks"
        ab_rename_col["Group Name"] = "Variant"

        abTesting_df.rename(
            columns=ab_rename_col,
            inplace=True
        )
        # pull the GA revenue,GA Order and GA Session data from GA TAB.
        abTesting_df = pd.merge(abTesting_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
        abTesting_df = abTesting_df.drop(columns=['ga_campaign'])
        # filter duplicate data and remove from original df
        abTesting_dup_df = abTesting_df.loc[abTesting_df.duplicated('Google Analytics Campaign Name', keep=False),:]
        abTesting_df.drop_duplicates('Google Analytics Campaign Name',inplace=True,keep=False)
        # Filter with variant B and put the value 0 of GA session, GA, order, GA revenue
        abTesting_dup_df[["Variant"]] = abTesting_dup_df[["Variant"]].fillna(value="")
        abTesting_dup_df.loc[abTesting_dup_df[abTesting_dup_df["Variant"] == "B"].index,["GA Revenue","GA Orders","GA Sessions"]] = [0,0,0]
        abTesting_df = pd.concat([abTesting_df,abTesting_dup_df])
        abTesting_df = abTesting_df.sort_index(ascending=True)
        # print(abTesting_dup_df)
        ## add A or B in Mailing name
        abTesting_df["Mailing"]=abTesting_df["Listrak Conversion Analytics Campaign Name"]
        
        abTesting_df["Mailing"] = abTesting_df.apply(lambda x :esp_module.concat_mailing_varient(x["Mailing"],x["Variant"]),axis=1 )
        #find dupliate in promo tab and remove and paste the data of ab testing to promo tab
        print("before drop_duplicates of Google Analytics Campaign Name: ",campaign_data_df.shape)
        campaign_data_df.drop_duplicates('Google Analytics Campaign Name',inplace=True,keep=False)
        print("after drop_duplicates of Google Analytics Campaign Name: ",campaign_data_df.shape)
        campaign_data_df.reset_index(inplace=True, drop=True)
        campaign_data_df = pd.concat([campaign_data_df,abTesting_df],ignore_index=True)
    campaign_data_df.reset_index(inplace=True, drop=True)
    print("Pass 600")
    campaign_data_df.reset_index(inplace=True, drop=True)
    campaign_data_df["Campaign"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"].str.split("_", n = 1, expand = True)[1].str.rstrip()
    campaign_data_df = pd.merge(campaign_data_df,mailingtagging_df,on="Mailing",how="left")
    campaign_data_df.loc[campaign_data_df["Variant"] == "",'Variant'] = campaign_data_df["Variant2"]
    campaign_data_df = campaign_data_df.drop(columns = ["Variant2"])
    campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df["Type 3"] = "US"
    campaign_data_df["Listrak Conversion Analytics Version"] = campaign_data_df["Segment"]
    campaign_data_df["Segment"] = '_'+campaign_data_df["Segment"]
    campaign_data_df["Segment"] = campaign_data_df["Segment"].str.split("_", n = 1, expand = True)[1].str.rstrip()
    # pull email link creatives data
    ## change variant, type 0 and type 1 based on Seed-list Segment value
    # campaign_data_df.loc[campaign_data_df["Segment"] == 'Seed-list',["Variant","Type 1"]] = ["Seed Test","Test"]
    campaign_data_df["Data Acquisitions"]="0"
    campaign_data_df["Mobile Originated Messages"]="0"
    campaign_data_df["Total Messages"]="0"
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
    trigger_data_df = trigger_data_df.drop(columns = ['%','%.1','%.2','%.3','%.4','Conversion Rate','AOV','Publish Date'])
    trigger_rename_col={
        "Thread / Step": "Name",
        "Subject": "Subject Line",
        "Bounces":"Total Bounces",
        "Unsubs." : "Unsub",
        "Opens": "Opens",
        "Clicks": "Total Clicks",
        "Clickers" : "Clicks",
        "Conversions": "Orders",
        "Listrak Conversion Analytics Version" : "Segment"
    }
    trigger_data_df.rename(
        columns=trigger_rename_col,
        inplace= True
    )
    # remove extra row having sent 0
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,["Sent"])
    trigger_data_df = trigger_data_df[trigger_data_df["Sent"] > 0]
    trigger_data_df["Delivered"] = trigger_data_df["Sent"]
    #pull the value of GA order, GA Session, GA Revenue
    print("trigger operation===========>")
    trigger_data_df_original = trigger_data_df.copy()
    trigger_data_df = pd.merge(trigger_data_df,trigger_ga_df,left_on="Google Analytics Campaign Name",right_on="Campaign", how="outer")
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
    # trigger_book_df.to_csv(uri + '/book_mytriggerd667.csv',index=False)
    trigger_book_df.drop_duplicates(["Google Analytics Campaign Name"],inplace=True,keep="last")
    print("Before ",trigger_data_df.columns)
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on=["Google Analytics Campaign Name"], how="left") ## made changes
    print("After ",trigger_data_df.columns)
    trigger_data_df["Campaign"]=""
    trigger_data_df["Mailing"]=""
    # trigger_data_df["Test Type"]=""
    trigger_data_df["Variant"]=""
    # trigger_data_df["Offer"]=""
    # trigger_data_df["ESP"]="Listrak"
    # trigger_data_df["Test Type"] =""
    # trigger_data_df["Variant"] =""
    # trigger_data_df["Offer"] =""
    trigger_data_df["Segment"] =""
    trigger_data_df["Listrak Conversion Analytics Module Name"] =""
    # trigger_data_df["Type 2"] = "Email"
    # trigger_data_df["Type 3"] = ""
    trigger_data_df["Data Acquisitions"]="0"
    trigger_data_df["Mobile Originated Messages"]="0"
    trigger_data_df["Total Messages"]="0"
    trigger_data_df["Listrak Conversion Analytics Version"] = trigger_data_df["Segment"]
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,matric_column)
    print("going to process sms data")
    ### broadcast_dashboard_df sms campaign
    if not broadcast_dashboard_df.empty:
        print("sms data is present")
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
            "Unique Clicks" : "Clicks",
            "Conversions":"Orders"
        }
        broadcast_dashboard_df.rename(
            columns=keydash_rename_col,
            inplace= True
        )
        # broadcast_dashboard_df["aa_data"]="sms | listrak | "+broadcast_dashboard_df["Name"]
        broadcast_dashboard_df =  pd.merge(broadcast_dashboard_df,campaign_analytics_data_df, left_on="Name", right_on="ga_campaign", how="left")
        print("PASS 3")
        broadcast_dashboard_df = broadcast_dashboard_df.drop(columns=["ga_campaign","Capped","Send Status"])
        broadcast_dashboard_df["ESP"]="Listrak SMS"
        broadcast_dashboard_df["Type 0"]="Campaign"
        broadcast_dashboard_df["Type 1"]="Promo"
        broadcast_dashboard_df["Type 2"]="SMS"
        broadcast_dashboard_df["Type 3"]=""
        ### segment data is present in Segment column.
        # broadcast_dashboard_df["Segment"]=broadcast_dashboard_df["Name"].str.rsplit("_", n = 1, expand = True)[1].str.strip()
        broadcast_dashboard_df["Mailing"]=broadcast_dashboard_df["Name"].str.rsplit("_", n = 1, expand = True)[0].str.strip()
        broadcast_dashboard_df["Campaign"] = broadcast_dashboard_df["Mailing"].str.split("_", n = 1, expand = True)[1].str.strip()
        broadcast_dashboard_df["Variant"]=""
        # broadcast_dashboard_df["Test Type"]=""
        # broadcast_dashboard_df["Offer"]=""
        broadcast_dashboard_df["Subject Line"]=""
        broadcast_dashboard_df["Sent"]= broadcast_dashboard_df["Delivered"]
        broadcast_dashboard_df["Total Bounces"]="0"
        broadcast_dashboard_df["Opens"]="0"
        broadcast_dashboard_df["Unsub"]="0"
        broadcast_dashboard_df["Visits"]="0"
        broadcast_dashboard_df["Reads"]="0"
        broadcast_dashboard_df["Google Analytics Campaign Name"]=""
        broadcast_dashboard_df["Google Analytics Campaign Content"]=""
        broadcast_dashboard_df["Listrak Conversion Analytics Campaign Name"]=""
        broadcast_dashboard_df["Listrak Conversion Analytics Version"]=""
        broadcast_dashboard_df["Listrak Conversion Analytics Module Name"]=""
        broadcast_dashboard_df["Workflow"]=""
        broadcast_dashboard_df["Entry Type"]=""
        broadcast_dashboard_df["Mobile Originated Messages"]="0"
        broadcast_dashboard_df["Data Acquisitions"]="0"
        broadcast_dashboard_df["Total Messages"]="0"
        broadcast_dashboard_df = esp_module.convert_str_to_num(broadcast_dashboard_df,matric_column)
        print("PASS 4")
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
        keyword_dashboard_df['Date'] = keyword_dashboard_df['Date'].apply(lambda x: esp_module.convert_str_to_date(x))
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
        print("PASS 5")
    #### list subscription Dashboard
    if not list_dashboard.empty:
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
            "Conversions":"Orders"
        }

        list_dashboard.rename(
            columns=listdash_rename_col,
            inplace= True
        )
        list_dashboard = esp_module.convert_str_to_num(list_dashboard,["Total Messages"])
        list_dashboard = list_dashboard[list_dashboard["Total Messages"] > 0]
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
        list_dashboard =  pd.merge(list_dashboard,trigger_book_df, on="Google Analytics Campaign Name", how="left")
        # list_dashboard["ESP"]=""
        # list_dashboard["Type 0"]=""
        # list_dashboard["Type 1"]=""
        # list_dashboard["Type 2"]="SMS"
        list_dashboard["Campaign"]=""
        list_dashboard["Mailing"]=""
        list_dashboard["Variant"]=""
        list_dashboard["Subject Line"]=""
        list_dashboard["Segment"]=""
        list_dashboard["Sent"]="0"
        list_dashboard["Total Bounces"]="0"
        list_dashboard["Opens"]="0"
        list_dashboard["Total Clicks"]="0"
        list_dashboard["Clicks"]="0"
        list_dashboard["Visits"]="0"
        list_dashboard["Reads"]="0"
        # list_dashboard["GA Revenue"]="0"
        # list_dashboard["GA Orders"]="0"
        # list_dashboard["GA Sessions"]="0"
        # list_dashboard["GA Sessions"]="0"
        list_dashboard["Google Analytics Campaign Content"]=""
        list_dashboard["Listrak Conversion Analytics Campaign Name"]=""
        list_dashboard["Listrak Conversion Analytics Version"]=""
        list_dashboard["Listrak Conversion Analytics Module Name"]=""
        list_dashboard["Workflow"]=""
        list_dashboard["Entry Type"]=""
        # list_dashboard["Type 3"] =""
        # list_dashboard["ESP"]="Listrak SMS"
        list_dashboard = esp_module.convert_str_to_num(list_dashboard,matric_column)
        print("PASS 6")
    ####
    BigQuery_lcol = ["Date","Name","Campaign","Mailing","Variant","Subject Line","Segment","Type 0","Type 1","Type 2","Offer",
                     "Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue",
                     "Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders",
                     "AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces",
                     "Total_Bounces","ESP","Type_3","Total_Opens","Total_Clicks","Spam_Complaints","Google_Analytics_Campaign_Name",
                     "Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version",
                     "Listrak_Conversion_Analytics_Module_Name","Assested_Sales","Assested_Order","Assested_Order_dot_1","Unnamed_colon__36",
                     "Unnamed_colon__37","Unnamed_colon__38","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK",
                     "FISCAL_MONTH","FISCAL_QUARTER"]
    
    excelcol = ["Name","Campaign","Mailing","Variant","Subject Line","Date","ESP","Type 0","Type 1","Type 2","Type 3",	
                "Sent","Delivered","Total Bounces","Unsub","Opens","Total Clicks","Clicks","Revenue","Orders",
                "Google Analytics Campaign Name","Google Analytics Campaign Content",
                "Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version","Listrak Conversion Analytics Module Name","Segment",
                "GA Revenue","GA Orders","GA Sessions","Data Acquisitions","Mobile Originated Messages","Total Messages"]
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    broadcast_dashboard_df = broadcast_dashboard_df[excelcol]
    list_dashboard = list_dashboard[excelcol]
    print("pass 7")

    # Getting CA DataFrame...
    CA_trigger_data = pd.DataFrame()
    CA_campaign_data_df = pd.DataFrame()
    CA_trigger_data, CA_campaign_data_df = update_CA_globalindustrial(uri,client_id,client_esp)
    
    # Concatenating CA & US Data...
    trigger_data_df = pd.concat([trigger_data_df,CA_trigger_data])
    trigger_data_df = trigger_data_df.sort_index(ascending=True)


    campaign_data_df = pd.concat([campaign_data_df,CA_campaign_data_df])
    campaign_data_df = campaign_data_df.sort_index(ascending=True)

    print("PASS 8")
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'US_globalIndustrial_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
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
        worksheet_promo.set_column(19, 19, None, format1)
        worksheet_promo.set_column(28, 28, None, format1)
        worksheet_trigger.set_column(19, 19, None, format1)
        worksheet_trigger.set_column(28, 28, None, format1)
        writer1.save()
        writer1.close()

      #  master_df = master_df[(master_df["Date"] >= masterfile_filter_start_date) & (master_df["Date"] <= masterfile_filter_end_date)]
    print("PASS 9")

    ###### master file ######
    if not master_df.empty:
        print(master_df.columns)
        master_df["Date"] = master_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x))
        master_original_df = master_df.copy()
        master_df = master_df[(master_df["Date"] >= masterfile_filter_start_date) & (master_df["Date"] <= masterfile_filter_end_date)]
        master_original_df.drop(master_original_df[(master_original_df['Date'] >= masterfile_filter_start_date) & (master_original_df['Date'] <= masterfile_filter_end_date)].index, inplace = True)
        ## calcu tab
        print("====> master_gacalcu_df ",master_gacalcu_df.shape)
        master_gacalcu_df[("Final","Date")] = master_gacalcu_df[("Final","Date")].apply(lambda x: esp_module.convert_str_to_date(x))
        master_gacalcu_df[("Final","End Date")] = master_gacalcu_df[("Final","End Date")].apply(lambda x: esp_module.convert_str_to_date(x))
        print(master_gacalcu_df)
        master_gacalcu_original_df = master_gacalcu_df.copy()
        print("master_gacalcu_original_df shape ",master_gacalcu_original_df.shape)
        print(masterfile_filter_start_date,'=========',masterfile_filter_end_date)
        master_gacalcu_df = master_gacalcu_df[(master_gacalcu_df["Final"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_df["Final"]["Date"] <= masterfile_filter_end_date)]
        master_gacalcu_original_df.drop(master_gacalcu_original_df[(master_gacalcu_original_df["Final"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_original_df["Final"]["Date"] <= masterfile_filter_end_date)].index, inplace = True)
        print("master_gacalcu_df shape1 ",master_gacalcu_df.shape)
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
    print("going for old campaign")
    old_campaign_data = []
    old_campaign_final_data_df = []
    print(master_gacalcu_df.shape)
    if not master_gacalcu_df.empty:
        mydata = []
        print(week_range)
        for x in week_range:
            print(x)
            # each_date = newsdate.strftime("%m/%d/%Y")
            #sum if date wise and name not equal to old campaign
            revenue_data = master_df.loc[(master_df['Date'] >= x["start_date"]) & (master_df['Date'] <= x["end_date"])& (master_df['Name'] != "Old Campaign") & (master_df['ESP'] != "Listrak SMS"), "GA Revenue"].sum()
            order_data = master_df.loc[(master_df['Date'] >= x["start_date"]) & (master_df['Date'] <= x["end_date"]) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak SMS"), "GA Orders"].sum()
            session_data = master_df.loc[(master_df['Date'] >= x["start_date"]) & (master_df['Date'] <= x["end_date"]) & (master_df['Name'] != "Old Campaign") &  (master_df['ESP'] != "Listrak SMS"), "GA Sessions"].sum()
            print("============> append")
            mydata.append([np.nan,x["start_date"],x["end_date"],0,0,0,revenue_data,order_data,session_data,0,0,0])
            old_campaign_data.append([x["start_date"],"Old Campaign",np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,'Campaign','Old Campaign',np.nan,np.nan,np.nan,'US',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan])
        print("going from create dataframe")
        master_gacalcu_df = pd.DataFrame(mydata, columns=master_gacalcu_df.columns)
        # master_gacalcu_df = pd.concat([master_gacalcu_df,new_gacalcu_df])
        master_gacalcu_df.reset_index(inplace=True, drop=True)
        ### read session and revenue file session_df , revenue_df
        print("=========> pass2")
        revenue_df = pd.merge(revenue_df,session_df, on=['Start Date','End Date', 'Week'], how="inner")
        revenue_df.dropna(subset=['Week'], inplace=True)
        # combine the session order and revenue data by date
        # revenue_df["Day Index"] = revenue_df["Day Index"].apply(lambda x: datetime.strptime(x,'%m/%d/%Y').date())
        print("going for rename")
        revenue_df.rename(
            columns={
                "Conversions":"GA|Orders",
                "Start Date":"Final|Date",
                "End Date":"Final|End Date",
                "Purchase revenue" : "GA|Revenue",
                "Sessions":"GA|Sessions"
            },
            inplace=True
        )
        revenue_df = esp_module.convert_str_to_num(revenue_df,["GA|Orders","GA|Revenue","GA|Sessions"])
        
        master_gacalcu_df.drop(columns=["GA"],inplace=True)
        master_gacalcu_df.columns = master_gacalcu_df.columns.map('|'.join).str.strip('|')
        master_gacalcu_df = pd.merge(master_gacalcu_df,revenue_df,on=["Final|Date","Final|End Date"],how="left")
        # master_gacalcu_df = pd.merge(master_gacalcu_df,attentive_rev_df,on="Final|Date",how="left")
        print("pass 4")
        #convert str to number
        master_gacalcu_df = esp_module.convert_str_to_num(master_gacalcu_df,["GA|Revenue","GA|Orders","GA|Sessions","ESP|Revenue","ESP|Orders","ESP|Sessions"])
        master_gacalcu_df["Final|Revenue"] = master_gacalcu_df["GA|Revenue"] - master_gacalcu_df["ESP|Revenue"]
        master_gacalcu_df["Final|Orders"] = master_gacalcu_df["GA|Orders"] - master_gacalcu_df["ESP|Orders"]
        master_gacalcu_df["Final|Sessions"] = master_gacalcu_df["GA|Sessions"] - master_gacalcu_df["ESP|Sessions"]
        ###
        old_campaign_final_data_df = master_gacalcu_df[["Final|Revenue","Final|Order","Final|Sessions","Final|Date"]].copy()
        # old_campaign_final_data_sms_df = master_gacalcu_df[["SMS|Revenue","SMS|Order","SMS|Sessions","SMS|Date"]].copy()
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
        # master_gacalcu_df = master_gacalcu_df[["ESP","GA","Final","ESPSMS","GASMS","SMS"]]
        # master_gacalcu_original_df = master_gacalcu_original_df[["ESP","GA","Final","ESPSMS","GASMS","SMS"]]
        # print(master_gacalcu_df)
        print("pass 6")
        master_gacalcu_original_df = pd.concat([master_gacalcu_original_df, master_gacalcu_df])
        master_gacalcu_original_df = master_gacalcu_original_df[master_gacalcu_original_df[("Final","Date")].notnull()]
        # master_gacalcu_original_df[("Final","Date")] = master_gacalcu_original_df[("Final","Date")].apply(lambda x: x.strftime("%m/%d/%Y"))
        master_gacalcu_original_df.reset_index(inplace=True,drop=True)
    ### add final revenue, order and session to old campaign in master file data tab
    if not master_df.empty:
        # old_campaign_master_df = master_df[master_df["Type 1"] == "Old Campaign"]
        ## remove the taken dataset
        ## create new Data frame for old campaign
        old_campaign_master_df = pd.DataFrame(old_campaign_data, columns=master_df.columns)
        # old_campaign_master_df = pd.concat([old_campaign_master_df,old_campaign_data_df])
        old_campaign_master_df.drop(columns=["GA Revenue","GA Orders","GA Sessions"], inplace=True)
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        #pull the final data from ga calcu
        old_campaign_final_data_df.rename(columns={
            "Final|Revenue" : "GA Revenue",
            "Final|Orders" : "GA Orders",
            "Final|Sessions":"GA Sessions",
            "Final|Start Date" :"Date"
        },inplace=True)
        old_campaign_master_df = pd.merge(old_campaign_master_df,old_campaign_final_data_df,on="Date",how="left")
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        master_df = pd.concat([master_df,old_campaign_master_df])
        #update into master file
        
        master_original_df.reset_index(inplace=True,drop=True)
        master_original_df = pd.concat([master_original_df,master_df])
        # master_original_df.reset_index(inplace=True,drop=True)
        newfilename = 'global_industrial-Global Industrial Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
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
    return ""

def get_old_campaign_dates(file_name, position=-1,datatype="String"):
    temp_file_name = file_name.split('_')
    temp_first_date = temp_file_name[position]
    first_date = temp_first_date[4:6] + '/' + temp_first_date[6:8] + '/' + temp_first_date[0:4]
    if datatype == "date":
        first_date = dt.datetime.strptime(first_date, "%m/%d/%Y")
        first_date = first_date.date()
    return(first_date)