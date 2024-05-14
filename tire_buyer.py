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
client_name = 'Tire Buyer'
dbx_path = '/Client/Tirebuyer/Internal Files/Reporting/'

def update_masterlist(uri,client_id,client_esp):
    esp_module = __import__(client_esp)
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    mailingtagging_df = pd.DataFrame()
    workflow_df = pd.DataFrame()
    merged_workflow_df = pd.DataFrame()
    email_links_df = pd.DataFrame()
    trigger_book_ga_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    tirebuyer_master_df = pd.DataFrame()
    tirebuyer_master_gacalcu_df = pd.DataFrame()
    tirebuyer_master_gacalcu_original_df = pd.DataFrame()
    tirebuyer_session_df = pd.DataFrame()
    tirebuyer_revenue_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    trigger_ga_rev_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    trigger_ga_ses_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    tirebuyer_master_original_df = pd.DataFrame()
    ### attentive data
    attentive_promo_df = pd.DataFrame()
    attentive_trigger_df = pd.DataFrame()
    attentive_campaign_rev_df = pd.DataFrame()
    attentive_campaign_ses_df = pd.DataFrame()
    attentive_campaign_df = pd.DataFrame()
    attentive_ses_df = pd.DataFrame()
    attentive_rev_df = pd.DataFrame()
    att_start_date = ""
    att_end_date = ""
    #######
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    # Read All files
    # Reading Campaign and Trigger Files
    ## check zip file
    print("start zip check---------------")
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_module.checkZipFile(client_id,'tire_buyer-zipfile.zip',archive_datetime)
    print("start reading files----------------")
    list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/Tire Buyer Sheets/2023_0625-0701/' #local
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
        elif "Mailing Tagging" in file_name:
            try:
                mailingtagging_df = pd.read_excel(uri + file_name,dtype={"Date": str}) # local
                mailingtagging_df = mailingtagging_df[mailingtagging_df["Client"] == "TireBuyer"]
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
        elif "JourneyMessageSummary" in file_name:
            try:
                each_merged_workflow_df = pd.DataFrame()
                workflow_df = pd.read_excel(uri + file_name) # local
                trigger_date = esp_module.get_trigger_dates(file_name,-2,"date")
                # workflow_df["Publish Date"] = trigger_date
                workflow_df = workflow_df.drop(columns=["Publish Date","Channel","Bounce/Failure %","Deliver %","Open %","Reads","Read %","Clicks","Clickers %","Click-To-Open %","Visits","Conversion %","Unsub %","Revenue Per Message"])
                workflow_df.rename(
                    columns={
                        "Journey":"Workflow",
                        "Bounced/Failed" : "Total Bounce",
                        "Unsubscribes" : "Unsub",
                        "Conversions":"Orders",
                        "Clickers":"Clicks"
                    },
                    inplace=True
                )
                each_merged_workflow_df = workflow_df.groupby(["Message Step Name","Subject","Workflow","Entry Type","List"])["Sent","Delivered","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders"].sum()
                each_merged_workflow_df = each_merged_workflow_df[each_merged_workflow_df["Sent"] > 0]
                each_merged_workflow_df.reset_index(inplace=True)
                each_merged_workflow_df["Date"] = trigger_date
                # print("each_merged_workflow_df",each_merged_workflow_df)
                merged_workflow_df = merged_workflow_df.append(each_merged_workflow_df)
                del workflow_df
                sourcelistfiles.append(file_name)
                # merged_workflow_df.to_excel(uri + 'mymergedworkflow.xlsx')
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
        elif "TriggerBookGA" in file_name:
            try:
                trigger_book_ga_df = pd.read_csv(uri + file_name) # local
                trigger_book_ga_df.rename(
                    columns={
                        "GA":"Campaign"
                    },
                    inplace=True
                )
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name) # local
                trigger_book_df = trigger_book_df.drop(columns = ["Listrak Conversion Analytics Module Name"])
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
        
        elif "Email links" in file_name:
            try:
                email_links_df = pd.read_excel(uri + file_name,usecols=["Mailing","Creatives"])
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
        elif "Tire Buyer" in file_name:
            try:
                ## get the file name:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = esp_module.convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        tirebuyer_master_df = pd.read_excel(xls, "Data")
                        tirebuyer_master_gacalcu_df = pd.read_excel(xls, "GA Calcu",header=[0, 1])
                    sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr ====>",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
        elif "ses.csv" in file_name:
            try:
                tirebuyer_session_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Date","Sessions"],dtype={"Date": str})
                tirebuyer_session_df.rename(
                    columns = {
                        "Date":"Day Index"
                    },inplace=True
                )
                tirebuyer_session_df = tirebuyer_session_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "rev.csv" in file_name:
            try:
                tirebuyer_revenue_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Date","Total revenue","Conversions"],dtype={"Date": str})
                tirebuyer_revenue_df.rename(
                    columns = {
                        "Date":"Day Index",
                        "Total revenue":"Revenue",
                        "Conversions" :"Transactions"
                    },inplace=True
                )
                tirebuyer_revenue_df = tirebuyer_revenue_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "ses_attentive.csv" in file_name:
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

        elif "rev_attentive.csv" in file_name:
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
        elif "attentivePromoAndTrigger" in file_name:
            try:
                att_filename = file_name
                att_start_date = esp_module.get_trigger_dates(att_filename,-2,"date")
                att_end_date = esp_module.get_trigger_dates(att_filename,-1,"date")
                with pd.ExcelFile(uri + file_name) as xls:
                    attentive_trigger_df  = pd.read_excel(xls, "Automated Messages with Copy")
                    attentive_promo_df = pd.read_excel(xls, "One-Time with Campaign Name, Se")
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr att ====>",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "rev-attentiveCampaignAnalytics" in file_name:
            try:
                attentive_campaign_rev_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Total revenue","Conversions"])
                attentive_campaign_rev_df = attentive_campaign_rev_df.dropna()
                attentive_campaign_rev_df.rename(
                    columns={
                        "Campaign":"Session campaign",
                        "Total revenue" :"Revenue",
                        "Conversions" : "Transactions"
                    },
                    inplace=True
                )
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "ses-attentiveCampaignAnalytics" in file_name:
            try:
                attentive_campaign_ses_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Session campaign","Sessions"])
                attentive_campaign_ses_df = attentive_campaign_ses_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
    #Operation on files
    matric_column = ["Sent","Delivered","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders","GA Revenue","GA Orders","GA Sessions"]
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
    if not attentive_promo_df.empty:
        attentive_promo_df = attentive_promo_df.drop(columns=['Send Time','Send Segment','Has Media','CTR (%)','CVR (%)','Opt Out Rate (%)'])
        attentive_promo_df["Date"] = attentive_promo_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x))
        attentive_promo_df = attentive_promo_df[(attentive_promo_df["Date"] >= att_start_date) & (attentive_promo_df["Date"] <= att_end_date)]
        ## rename columns
        att_promo_col_rename = {
            "Campaign Name" : "Name",
            "Message Copy" : "Subject Line",
            "Conversions"  : "Orders",
            "Revenue ($)" : "Revenue",
            "Unsubscribes" : "Unsub"
        }
        attentive_promo_df.rename(
            columns= att_promo_col_rename,
            inplace= True
        )
        
        ### merge rev and ses file for sms
        attentive_campaign_df = pd.merge(attentive_campaign_rev_df,attentive_campaign_ses_df, on="Session campaign",how="outer")
        attentive_campaign_df = attentive_campaign_df.fillna(0)
        print("attentive_campaign_df ",attentive_campaign_df.shape)
        ###
        attentive_camp_col_rename = {
            "Session campaign" : "GA Campaign",
            "Sessions" : "GA Sessions",
            "Transactions" : "GA Orders",
            "Revenue" :"GA Revenue"
        }
        attentive_campaign_df.rename(
            columns=attentive_camp_col_rename,
            inplace=True
        )
        ## merge with TriggerBookGA data to get the campaign name
        attentive_promo_df = pd.merge(attentive_promo_df,trigger_book_ga_df,left_on="Name", right_on="Name",how="left")
        ## replace space to -
        attentive_promo_df["Message Title"] = attentive_promo_df["Message Title"].str.strip()
        attentive_promo_df["Message Title"] = attentive_promo_df["Message Title"].str.replace(" ","-",regex=False)
        attentive_promo_df["Message Title"] = attentive_promo_df["Message Title"].str.replace("$","",regex=False)
        attentive_promo_df = pd.merge(attentive_promo_df,attentive_campaign_df,left_on="Message Title",right_on="GA Campaign",how="left")
        attentive_promo_df.drop(columns=["GA Campaign","Campaign"],inplace=True)
        attentive_promo_df[["GA Revenue","GA Orders","GA Sessions"]] = attentive_promo_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
        attentive_promo_df["Campaign"]=""
        attentive_promo_df["ESP"] = "Attentive"
        attentive_promo_df["Type 0"] = "Campaign"
        attentive_promo_df["Type 1"] = "Promo"
        attentive_promo_df["Sent"] = attentive_promo_df["Delivered"]
        attentive_promo_df["Mailing"] = ""
        attentive_promo_df["Test Type"] = ""
        attentive_promo_df["Variant"] = ""
        attentive_promo_df["Offer"] = ""
        attentive_promo_df["Type 2"] = "SMS"
        attentive_promo_df["Type 3"] = ""
        attentive_promo_df["Creatives"] = ""
        attentive_promo_df["Total Bounce"]=0
        attentive_promo_df["Opens"]=0
        attentive_promo_df["Google Analytics Campaign Name"]=""
        attentive_promo_df["Google Analytics Campaign Content"]=""
        attentive_promo_df["Listrak Conversion Analytics Campaign Name"]=""
        attentive_promo_df["Segment"]=""
        attentive_promo_df["Listrak Conversion Analytics Module Name"]=""
        
        attentive_promo_df = esp_module.convert_str_to_num(attentive_promo_df,matric_column)
    if not attentive_trigger_df.empty:
        attentive_trigger_df = attentive_trigger_df.drop(columns=["CTR (%)","CVR (%)","Opt Out Rate (%)"])
        att_tri_col_rename = {
            "Day" :"Date",
            "Message Name" : "Name",
            "Message Copy" : "Subject Line",
            "Conversions" : "Orders",
            "Revenue ($)" : "Revenue",
            "Unsubscribes" : "Unsub"
        }
        attentive_trigger_df.rename(
            columns=att_tri_col_rename,
            inplace=True
        )
        attentive_trigger_df["Date"] = attentive_trigger_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x))
        attentive_trigger_df = attentive_trigger_df[(attentive_trigger_df["Date"] >= trigger_start_date) & (attentive_trigger_df["Date"] <= trigger_end_date)]
        ## merge with TriggerBookGA data to get the campaign name
        attentive_trigger_df = pd.merge(attentive_trigger_df,trigger_book_ga_df,left_on="Name", right_on="Name",how="left")
        ## add ga revenue
        attentive_trigger_df_original = attentive_trigger_df.copy()
        attentive_trigger_df = pd.merge(attentive_trigger_df,trigger_ga_df,left_on="Campaign",right_on="Campaign",how="left")
        attentive_trigger_df_notnone = attentive_trigger_df[attentive_trigger_df[("triggerDate")].isnull()]
        attentive_trigger_df  = attentive_trigger_df[attentive_trigger_df[("triggerDate")].notnull()]
        attentive_trigger_df.drop(attentive_trigger_df[attentive_trigger_df['Date'] != attentive_trigger_df['triggerDate']].index, inplace = True)
        if not attentive_trigger_df_notnone.empty:
            attentive_trigger_df =  pd.concat([attentive_trigger_df,attentive_trigger_df_notnone])
            attentive_trigger_df = attentive_trigger_df.sort_index(ascending=True)
        ## add missing row from left table
        attentive_trigger_df = pd.concat([attentive_trigger_df,attentive_trigger_df_original])
        attentive_trigger_df.drop_duplicates(['Name','Subject Line','Campaign','Date','Delivered','Revenue','Orders'],inplace=True,keep="first")
        del attentive_trigger_df_original
        #reset index
        attentive_trigger_df.reset_index(inplace=True, drop=True)
        attentive_trigger_df[["GA Revenue","GA Orders","GA Sessions"]] = attentive_trigger_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
        attentive_trigger_df = pd.merge(attentive_trigger_df,trigger_book_df, left_on="Name",right_on="Google Analytics Campaign Name", how="left")
        attentive_trigger_df = attentive_trigger_df.drop(columns=["Google Analytics Campaign Name"])
        
        ## add missing columns
        attentive_trigger_df["Message Title"]=attentive_trigger_df["Campaign"]
        attentive_trigger_df["Sent"] = attentive_trigger_df["Delivered"]
        attentive_trigger_df["Campaign"]=""
        attentive_trigger_df["Mailing"]=""
        attentive_trigger_df["Test Type"]=""
        attentive_trigger_df["Variant"]=""
        attentive_trigger_df["Offer"]=""
        # attentive_trigger_df["Type 3"]=attentive_trigger_df["Type 2"]
        attentive_trigger_df["Type 3"]=""
        attentive_trigger_df["Type 2"] = "SMS"
        attentive_trigger_df["Total Bounce"]=0
        attentive_trigger_df["Opens"]=0
        attentive_trigger_df["Google Analytics Campaign Name"]=""
        attentive_trigger_df["Google Analytics Campaign Content"]=""
        attentive_trigger_df["Listrak Conversion Analytics Campaign Name"]=""
        attentive_trigger_df["Segment"]=""
        attentive_trigger_df["Listrak Conversion Analytics Module Name"]=""
        attentive_trigger_df["Creatives"]=""
        attentive_trigger_df = attentive_trigger_df.drop(columns=["triggerDate"])
        attentive_trigger_df = esp_module.convert_str_to_num(attentive_trigger_df,matric_column)
        # export to excel
        print("pass 2")
        attentivereportfilename = ""
        if not attentive_promo_df.empty or (not attentive_trigger_df.empty):
            excelcol = ["Name","Message Title","Campaign","Mailing","Test Type","Variant","Offer","Type 2","Type 3","Subject Line","Date","ESP",	
                "Type 0","Type 1","Sent","Delivered","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders",
                "Google Analytics Campaign Name","Google Analytics Campaign Content",
                "Listrak Conversion Analytics Campaign Name","Segment","Listrak Conversion Analytics Module Name",
                "GA Revenue","GA Orders","GA Sessions","Creatives"]
            #re-arrange the columns
            attentive_promo_df = attentive_promo_df[excelcol]
            attentive_trigger_df = attentive_trigger_df[excelcol]
            #####
            attentivereportfilename = 'Attentive_Tire_Buyer_'+str(att_start_date)+"_"+str(att_end_date)+".xlsx"
            writer1 = pd.ExcelWriter(uri+attentivereportfilename, engine="xlsxwriter")
            attentive_promo_df.to_excel(writer1, sheet_name="Promo",index=False)
            attentive_trigger_df.to_excel(writer1, sheet_name="Trigger",index=False)
            workbook1 = writer1.book
            worksheet_att_promo = writer1.sheets["Promo"]
            worksheet_att_trigger = writer1.sheets["Trigger"]
            # Add some cell formats.
            format1 = workbook1.add_format({"num_format": "$#,##0.00"})
            # # Set the column width and format.
            worksheet_att_promo.set_column(20, 20, None, format1)
            worksheet_att_promo.set_column(27, 27, None, format1)
            worksheet_att_trigger.set_column(20, 20, None, format1)
            worksheet_att_trigger.set_column(27, 27, None, format1)
            writer1.save()
            writer1.close()
            sourcelistfiles.append(attentivereportfilename)
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
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV','Pass Along','Capped','Clicks','Visits','Reads'])
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
        campaign_data_df.drop_duplicates('Google Analytics Campaign Name',inplace=True,keep=False)
        campaign_data_df.reset_index(inplace=True, drop=True)

        campaign_data_df = pd.concat([campaign_data_df,abTesting_df],ignore_index=True)
    campaign_data_df.reset_index(inplace=True, drop=True)
    campaign_data_df["Campaign"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"].str.split("-", n = 1, expand = True)[1].str.rstrip()
    # campaign_data_df["Mailing"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]
    # pull Test Type Variant Offer and Type 2 data from mailing tagging file
    campaign_data_df = pd.merge(campaign_data_df,mailingtagging_df,on="Mailing",how="left")
    campaign_data_df.loc[campaign_data_df["Variant"] == "",'Variant'] = campaign_data_df["Variant2"]
    campaign_data_df = campaign_data_df.drop(columns = ["Variant2"])
    campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df["Type 3"] = campaign_data_df["Type 2"]
    campaign_data_df["Type 2"] = "Email"
    campaign_data_df["Message Title"] = ""
    # pull email link creatives data
    if not email_links_df.empty:
        campaign_data_df = pd.merge(campaign_data_df, email_links_df, on="Mailing", how="left")
    else:
        campaign_data_df["Creatives"] = ""
    # change datatype of number columns from string to number
    ## remove extra data VA and VB
    campaign_data_df["Mailing"] = campaign_data_df["Mailing"].apply(lambda x :esp_module.remove_extra_data(x))
    campaign_data_df["Campaign"] = campaign_data_df["Campaign"].apply(lambda x :esp_module.remove_extra_data(x))
    
    campaign_data_df = esp_module.convert_str_to_num(campaign_data_df,matric_column)
    #workflow creation journeyMessageSummary file
    #get google analytics name from trigger book GA
    merged_workflow_df = pd.merge(merged_workflow_df,trigger_book_ga_df,left_on="Message Step Name",right_on="Name", how="left")
    merged_workflow_df = merged_workflow_df.drop(columns = ["Name"])
    #get the GA Order, GA Revenue, and GA Session from Trigger GA tab
    # print("merged_workflow_df ====>",merged_workflow_df["Campaign"])
    merged_workflow_df_original = merged_workflow_df.copy()
    merged_workflow_df = pd.merge(merged_workflow_df,trigger_ga_df,left_on="Campaign",right_on="Campaign", how="left")
    # print("merged_workflow_df",merged_workflow_df.loc[:, ["Message Step Name", "Date","triggerDate"]])
    #remove duplicate row
    # merged_workflow_df.drop(merged_workflow_df[(merged_workflow_df['Date'] != merged_workflow_df['triggerDate']) & (not merged_workflow_df['triggerDate'].isnull().values.any())].index, inplace = True)
    merged_workflow_df_notnone = merged_workflow_df[merged_workflow_df[("triggerDate")].isnull()]
    merged_workflow_df  = merged_workflow_df[merged_workflow_df[("triggerDate")].notnull()]
    merged_workflow_df.drop(merged_workflow_df[merged_workflow_df['Date'] != merged_workflow_df['triggerDate']].index, inplace = True)
    if not merged_workflow_df_notnone.empty:
        merged_workflow_df =  pd.concat([merged_workflow_df,merged_workflow_df_notnone])
        # merged_workflow_df.reset_index(inplace=True, drop=True)
        merged_workflow_df = merged_workflow_df.sort_index(ascending=True)
     ## add missing row from left table
    merged_workflow_df = pd.concat([merged_workflow_df,merged_workflow_df_original])
    merged_workflow_df.drop_duplicates(['Message Step Name','Subject','Workflow','Entry Type','List','Campaign','Date'],inplace=True,keep="first")
    del merged_workflow_df_original
    # print("merged_workflow_df",merged_workflow_df.loc[:, ["Message Step Name", "Date","triggerDate"]])
    #reset index
    merged_workflow_df.reset_index(inplace=True, drop=True)
    merged_workflow_df = merged_workflow_df.fillna(0)
    merged_workflow_df["Campaign"] =""
    merged_workflow_df["Mailing"] =""
    merged_workflow_df["Test Type"] =""
    merged_workflow_df["Variant"] =""
    merged_workflow_df["Offer"] =""
    merged_workflow_df["Segment"] =""
    merged_workflow_df["Listrak Conversion Analytics Module Name"] =""
    merged_workflow_df = pd.merge(merged_workflow_df,trigger_book_df, left_on="Message Step Name",right_on="Google Analytics Campaign Name", how="left")
    merged_workflow_df = merged_workflow_df.drop(columns=["Google Analytics Campaign Name"])
    # rename the workflow columns
    wf_rename_col= {
        "Message Step Name" : "Name",
        "Subject"  : "Subject Line",
        "Workflow" : "Google Analytics Campaign Name",
        "Entry Type" :"Google Analytics Campaign Content",
        "List" : "Listrak Conversion Analytics Campaign Name"
    }
    merged_workflow_df.rename(
        columns=wf_rename_col,
        inplace=True
    )
    merged_workflow_df["Creatives"] = ""
    merged_workflow_df["Type 3"] = merged_workflow_df["Type 2"]
    merged_workflow_df["Type 2"] = "Email"
    merged_workflow_df["Message Title"] = ""
    merged_workflow_df = esp_module.convert_str_to_num(merged_workflow_df,matric_column)
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
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on="Google Analytics Campaign Name", how="left")
    trigger_data_df["Campaign"]=""
    trigger_data_df["Mailing"]=""
    trigger_data_df["Test Type"]=""
    trigger_data_df["Variant"]=""
    trigger_data_df["Offer"]=""
    # trigger_data_df["Test Type"] =""
    # trigger_data_df["Variant"] =""
    # trigger_data_df["Offer"] =""
    trigger_data_df["Segment"] =""
    trigger_data_df["Listrak Conversion Analytics Module Name"] =""
    trigger_data_df["Creatives"] = ""
    trigger_data_df["Type 3"] = trigger_data_df["Type 2"]
    trigger_data_df["Type 2"] = "Email"
    trigger_data_df["Message Title"] = ""
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,matric_column)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    excelcol = ["Name","Message Title","Campaign","Mailing","Test Type","Variant","Offer","Type 2","Type 3","Subject Line","Date","ESP",	
                "Type 0","Type 1","Sent","Delivered","Total Bounce","Unsub","Opens","Clicks","Revenue","Orders",
                "Google Analytics Campaign Name","Google Analytics Campaign Content",
                "Listrak Conversion Analytics Campaign Name","Segment","Listrak Conversion Analytics Module Name",
                "GA Revenue","GA Orders","GA Sessions","Creatives"]
    #re-arrange the columns
    print("pass 3")
    campaign_data_df = campaign_data_df[excelcol]
    merged_workflow_df = merged_workflow_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'Tire_Buyer_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer1 = pd.ExcelWriter(uri+newreportfilename, engine="xlsxwriter")
        campaign_data_df.to_excel(writer1, sheet_name="Promo",index=False)
        merged_workflow_df.to_excel(writer1, sheet_name="Workflow",index=False)
        trigger_data_df.to_excel(writer1, sheet_name="Trigger",index=False)
        workbook1 = writer1.book
        worksheet_promo = writer1.sheets["Promo"]
        worksheet_workflow = writer1.sheets["Workflow"]
        worksheet_trigger = writer1.sheets["Trigger"]
        # Add some cell formats.
        format1 = workbook1.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_promo.set_column(18, 18, None, format1)
        worksheet_promo.set_column(25, 25, None, format1)
        worksheet_workflow.set_column(18, 18, None, format1)
        worksheet_workflow.set_column(25, 25, None, format1)
        worksheet_trigger.set_column(18, 18, None, format1)
        worksheet_trigger.set_column(25, 25, None, format1)
        writer1.save()
        writer1.close()
    ###### master file ######
    if not tirebuyer_master_df.empty:
        tirebuyer_master_df["Date"] = tirebuyer_master_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x))
        tirebuyer_master_original_df = tirebuyer_master_df.copy()
        tirebuyer_master_df = tirebuyer_master_df[(tirebuyer_master_df["Date"] >= masterfile_filter_start_date) & (tirebuyer_master_df["Date"] <= masterfile_filter_end_date)]
        tirebuyer_master_original_df.drop(tirebuyer_master_original_df[(tirebuyer_master_original_df['Date'] >= masterfile_filter_start_date) & (tirebuyer_master_original_df['Date'] <= masterfile_filter_end_date)].index, inplace = True)
        ## calcu tab
        tirebuyer_master_gacalcu_df[("Final","Date")] = tirebuyer_master_gacalcu_df[("Final","Date")].apply(lambda x: esp_module.convert_str_to_date(x))
        tirebuyer_master_gacalcu_df[("SMS","Date")] = tirebuyer_master_gacalcu_df[("SMS","Date")].apply(lambda x: esp_module.convert_str_to_date(x))
        tirebuyer_master_gacalcu_original_df = tirebuyer_master_gacalcu_df.copy()
        print("tirebuyer_master_gacalcu_original_df shape ",tirebuyer_master_gacalcu_original_df.shape)
        tirebuyer_master_gacalcu_df = tirebuyer_master_gacalcu_df[(tirebuyer_master_gacalcu_df["Final"]["Date"] >= masterfile_filter_start_date) & (tirebuyer_master_gacalcu_df["Final"]["Date"] <= masterfile_filter_end_date)]
        tirebuyer_master_gacalcu_original_df.drop(tirebuyer_master_gacalcu_original_df[(tirebuyer_master_gacalcu_original_df["Final"]["Date"] >= masterfile_filter_start_date) & (tirebuyer_master_gacalcu_original_df["Final"]["Date"] <= masterfile_filter_end_date)].index, inplace = True)
        print("tirebuyer_master_gacalcu_original_df shape1 ",tirebuyer_master_gacalcu_original_df.shape)
        ## filter with type 0 = Campaign and Type 1 = Promo
    
        tirebuyer_master_df.drop(tirebuyer_master_df[(tirebuyer_master_df["Type 0"] == "Campaign" ) & (tirebuyer_master_df["Type 1"] == "Promo")].index, inplace = True)
        tirebuyer_master_df.drop(tirebuyer_master_df[(tirebuyer_master_df["Type 1"] == "Old Campaign") | (tirebuyer_master_df["Type 1"] == "Old Campaign") ].index, inplace = True)
        #convert date type from string to date Old Campaign SMS
        # campaign_data_df["Date"] = campaign_data_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x,'%m/%d/%Y'))
        # merged_workflow_df["Date"] = merged_workflow_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x,'%m/%d/%Y'))
        # trigger_data_df["Date"] = trigger_data_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x,'%m/%d/%Y'))

        # paste the data from promo tab, workflow tab, and trigger tab to master file
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,campaign_data_df])
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,merged_workflow_df])
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,trigger_data_df])
        ## for sms
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,attentive_promo_df])
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,attentive_trigger_df])
        # tirebuyer_master_df = tirebuyer_master_df.drop(columns="triggerDate")
    # add seven row in ga calcu sheet
    #typecast the state date and end date to string
    old_campaign_data = []
    old_campaign_data_sms = []
    old_campaign_final_data_df = []
    old_campaign_final_data_sms_df = []
    if not tirebuyer_master_gacalcu_df.empty:
        mydata = []
        delta = masterfile_filter_end_date - masterfile_filter_start_date
        noofdays = delta.days
        print("no of days",noofdays)
        for x in range(noofdays+1):
            # newsdate =  trigger_start_date + timedelta(days=x)
            newsdate =  masterfile_filter_start_date + timedelta(days=x)
            # each_date = newsdate.strftime("%m/%d/%Y")
            #sum if date wise and name not equal to old campaign
            revenue_data = tirebuyer_master_df.loc[(tirebuyer_master_df['Date'] == newsdate) & (tirebuyer_master_df['Name'] != "Old Campaign") & (tirebuyer_master_df['Name'] != "Old Campaign SMS")& (tirebuyer_master_df['ESP'] != "Attentive"), "GA Revenue"].sum()
            order_data = tirebuyer_master_df.loc[(tirebuyer_master_df['Date'] == newsdate) & (tirebuyer_master_df['Name'] != "Old Campaign") & (tirebuyer_master_df['Name'] != "Old Campaign SMS")& (tirebuyer_master_df['ESP'] != "Attentive"), "GA Orders"].sum()
            session_data = tirebuyer_master_df.loc[(tirebuyer_master_df['Date'] == newsdate) & (tirebuyer_master_df['Name'] != "Old Campaign") & (tirebuyer_master_df['Name'] != "Old Campaign SMS")& (tirebuyer_master_df['ESP'] != "Attentive"), "GA Sessions"].sum()
            ## sms
            revenue_data_sms = tirebuyer_master_df.loc[(tirebuyer_master_df['Date'] == newsdate) & (tirebuyer_master_df['Name'] != "Old Campaign") & (tirebuyer_master_df['Name'] != "Old Campaign SMS")& (tirebuyer_master_df['ESP'] != "Listrak"), "GA Revenue"].sum()
            order_data_sms = tirebuyer_master_df.loc[(tirebuyer_master_df['Date'] == newsdate) & (tirebuyer_master_df['Name'] != "Old Campaign") & (tirebuyer_master_df['Name'] != "Old Campaign SMS")& (tirebuyer_master_df['ESP'] != "Listrak"), "GA Orders"].sum()
            session_data_sms = tirebuyer_master_df.loc[(tirebuyer_master_df['Date'] == newsdate) & (tirebuyer_master_df['Name'] != "Old Campaign") & (tirebuyer_master_df['Name'] != "Old Campaign SMS")& (tirebuyer_master_df['ESP'] != "Listrak"), "GA Sessions"].sum()
            ###
            mydata.append([np.nan,revenue_data,order_data,session_data,0,0,0,newsdate,0,0,0,revenue_data_sms,order_data_sms,session_data_sms,0,0,0,newsdate,0,0,0])
            old_campaign_data.append(["Old Campaign",np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,'Email',np.nan,np.nan,newsdate,np.nan,'Campaign','Old Campaign',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan])
            old_campaign_data_sms.append(["Old Campaign SMS",np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,'SMS',np.nan,np.nan,newsdate,np.nan,'Campaign','Old Campaign',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan])
        tirebuyer_master_gacalcu_df = pd.DataFrame(mydata, columns=tirebuyer_master_gacalcu_df.columns)
        # tirebuyer_master_gacalcu_df = pd.concat([tirebuyer_master_gacalcu_df,new_gacalcu_df])
        tirebuyer_master_gacalcu_df.reset_index(inplace=True, drop=True)
        ### read session and revenue file tirebuyer_session_df , tirebuyer_revenue_df
        print("=====> pass2")
        tirebuyer_revenue_df = tirebuyer_revenue_df.dropna()
        tirebuyer_session_df = tirebuyer_session_df.dropna()
        tirebuyer_revenue_df["Day Index"] = tirebuyer_revenue_df["Day Index"].apply(lambda x: esp_module.convert_str_to_date(str(x),'%Y%m%d'))
        tirebuyer_session_df["Day Index"] = tirebuyer_session_df["Day Index"].apply(lambda x: esp_module.convert_str_to_date(str(x),'%Y%m%d'))
        # combine the session order and revenue data by date
        tirebuyer_revenue_df = pd.merge(tirebuyer_revenue_df,tirebuyer_session_df,on="Day Index",how="inner")
        # tirebuyer_revenue_df["Day Index"] = tirebuyer_revenue_df["Day Index"].apply(lambda x: datetime.strptime(x,'%m/%d/%Y').date())
        tirebuyer_revenue_df = esp_module.convert_str_to_num(tirebuyer_revenue_df,["Transactions","Revenue","Sessions"])
        tirebuyer_revenue_df.rename(
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
        
        # tirebuyer_master_gacalcu_df.loc[tirebuyer_master_gacalcu_df[tirebuyer_master_gacalcu_df[("Final","Date")] == datetime.strptime("2022-10-01",'%Y-%m-%d').date()].index,("GA", "Revenue")]=6000
        # tirebuyer_master_gacalcu_df.loc[tirebuyer_master_gacalcu_df[tirebuyer_master_gacalcu_df[("Final","Date")] == datetime.strptime("2022-10-01",'%Y-%m-%d').date()].index,("GA", "Revenue")]=6000
        # the value of another DataFrame
        # tirebuyer_master_gacalcu_df[('GA','Revenue')] = tirebuyer_revenue_df[tirebuyer_revenue_df['Day Index'].eq(tirebuyer_master_gacalcu_df[("Final","Date")])]['Revenue'].values
        # tirebuyer_master_gacalcu_df = tirebuyer_revenue_df["Day Index"].apply(lambda x : tirebuyer_master_gacalcu_df.loc[tirebuyer_master_gacalcu_df[tirebuyer_master_gacalcu_df[("Final","Date")] == datetime.strptime(x,'%Y-%m-%d').date()].index,("GA", "Revenue")]= tirebuyer_revenue_df["Revenue"])
        # print(tirebuyer_master_gacalcu_df.loc[tirebuyer_master_gacalcu_df[tirebuyer_master_gacalcu_df[("Final","Date")] == datetime.strptime("2022-10-01",'%Y-%m-%d').date()].index,("GA", "Revenue")])
        # print(tirebuyer_master_gacalcu_df)
        # tuple1 = [("GA1","Day Index"),("GA1","Revenue"),("GA1","Transactions")]
        # mycol = pd.MultiIndex.from_tuples(tuple1)
        # tirebuyer_revenue_df = tirebuyer_revenue_df.set_axis(mycol,axis='columns')
        
        tirebuyer_master_gacalcu_df.drop(columns=["GA","GASMS"],inplace=True)
        tirebuyer_master_gacalcu_df.columns = tirebuyer_master_gacalcu_df.columns.map('|'.join).str.strip('|')
        tirebuyer_master_gacalcu_df = pd.merge(tirebuyer_master_gacalcu_df,tirebuyer_revenue_df,on="Final|Date",how="left")
        tirebuyer_master_gacalcu_df = pd.merge(tirebuyer_master_gacalcu_df,attentive_rev_df,on="Final|Date",how="left")
        print("pass 4")
        #convert str to number
        tirebuyer_master_gacalcu_df = esp_module.convert_str_to_num(tirebuyer_master_gacalcu_df,["GA|Revenue","GA|Order","GA|Sessions","ESP|Revenue","ESP|Order","ESP|Sessions","GASMS|Revenue","GASMS|Order","GASMS|Sessions","ESPSMS|Revenue","ESPSMS|Order","ESPSMS|Sessions"])
        tirebuyer_master_gacalcu_df["Final|Revenue"] = tirebuyer_master_gacalcu_df["GA|Revenue"] - tirebuyer_master_gacalcu_df["ESP|Revenue"]
        tirebuyer_master_gacalcu_df["Final|Order"] = tirebuyer_master_gacalcu_df["GA|Order"] - tirebuyer_master_gacalcu_df["ESP|Order"]
        tirebuyer_master_gacalcu_df["Final|Sessions"] = tirebuyer_master_gacalcu_df["GA|Sessions"] - tirebuyer_master_gacalcu_df["ESP|Sessions"]
        ## sms
        tirebuyer_master_gacalcu_df["SMS|Revenue"] = tirebuyer_master_gacalcu_df["GASMS|Revenue"] - tirebuyer_master_gacalcu_df["ESPSMS|Revenue"]
        tirebuyer_master_gacalcu_df["SMS|Order"] = tirebuyer_master_gacalcu_df["GASMS|Order"] - tirebuyer_master_gacalcu_df["ESPSMS|Order"]
        tirebuyer_master_gacalcu_df["SMS|Sessions"] = tirebuyer_master_gacalcu_df["GASMS|Sessions"] - tirebuyer_master_gacalcu_df["ESPSMS|Sessions"]
        ###
        old_campaign_final_data_df = tirebuyer_master_gacalcu_df[["Final|Revenue","Final|Order","Final|Sessions","Final|Date"]].copy()
        old_campaign_final_data_sms_df = tirebuyer_master_gacalcu_df[["SMS|Revenue","SMS|Order","SMS|Sessions","SMS|Date"]].copy()
        mycol = tirebuyer_master_gacalcu_df.columns
        tuple_col =[]
        for c in mycol:
            newcol = c.split("|")
            newcol_tup = tuple(newcol)
            tuple_col.append(newcol_tup)
        # create multi index dataframe
        mycol1 = pd.MultiIndex.from_tuples(tuple_col)
        print("pass 5")
        tirebuyer_master_gacalcu_df = tirebuyer_master_gacalcu_df.set_axis(mycol1,axis='columns')
        # reording columns
        tirebuyer_master_gacalcu_df = tirebuyer_master_gacalcu_df[["ESP","GA","Final","ESPSMS","GASMS","SMS"]]
        tirebuyer_master_gacalcu_original_df = tirebuyer_master_gacalcu_original_df[["ESP","GA","Final","ESPSMS","GASMS","SMS"]]
        # print(tirebuyer_master_gacalcu_df)
        print("pass 6")
        tirebuyer_master_gacalcu_original_df = pd.concat([tirebuyer_master_gacalcu_original_df, tirebuyer_master_gacalcu_df])
        tirebuyer_master_gacalcu_original_df = tirebuyer_master_gacalcu_original_df[tirebuyer_master_gacalcu_original_df[("Final","Date")].notnull()]
        tirebuyer_master_gacalcu_original_df[("Final","Date")] = tirebuyer_master_gacalcu_original_df[("Final","Date")].apply(lambda x: x.strftime("%m/%d/%Y"))
        tirebuyer_master_gacalcu_original_df.reset_index(inplace=True,drop=True)
    ### add final revenue, order and session to old campaign in master file data tab
    if not tirebuyer_master_df.empty:
        # old_campaign_tirebuyer_master_df = tirebuyer_master_df[tirebuyer_master_df["Type 1"] == "Old Campaign"]
        ## remove the taken dataset
        # tirebuyer_master_df.drop(tirebuyer_master_df[tirebuyer_master_df["Type 1"] == "Old Campaign"].index, inplace = True)
        # tirebuyer_master_df.reset_index(inplace=True,drop=True)
        ## create new Data frame for old campaign
        print(tirebuyer_master_df.columns)
        old_campaign_tirebuyer_master_df = pd.DataFrame(old_campaign_data, columns=tirebuyer_master_df.columns)
        old_campaign_tirebuyer_master_sms_df = pd.DataFrame(old_campaign_data_sms, columns=tirebuyer_master_df.columns)
        # old_campaign_tirebuyer_master_df = pd.concat([old_campaign_tirebuyer_master_df,old_campaign_data_df])
        old_campaign_tirebuyer_master_df.drop(columns=["GA Revenue","GA Orders","GA Sessions","Creatives"], inplace=True)
        old_campaign_tirebuyer_master_df.reset_index(inplace=True,drop=True)
        old_campaign_tirebuyer_master_sms_df.drop(columns=["GA Revenue","GA Orders","GA Sessions","Creatives"], inplace=True)
        old_campaign_tirebuyer_master_sms_df.reset_index(inplace=True,drop=True)
        #pull the final data from ga calcu
        old_campaign_final_data_df.rename(columns={
            "Final|Revenue" : "GA Revenue",
            "Final|Order" : "GA Orders",
            "Final|Sessions":"GA Sessions",
            "Final|Date" :"Date"
        },inplace=True)
        old_campaign_tirebuyer_master_df = pd.merge(old_campaign_tirebuyer_master_df,old_campaign_final_data_df,on="Date",how="left")
        old_campaign_tirebuyer_master_df.reset_index(inplace=True,drop=True)
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,old_campaign_tirebuyer_master_df])
        ## for sms
        old_campaign_final_data_sms_df.rename(columns={
            "SMS|Revenue" : "GA Revenue",
            "SMS|Order" : "GA Orders",
            "SMS|Sessions":"GA Sessions",
            "SMS|Date" :"Date"
        },inplace=True)
        old_campaign_tirebuyer_master_sms_df = pd.merge(old_campaign_tirebuyer_master_sms_df,old_campaign_final_data_sms_df,on="Date",how="left")
        old_campaign_tirebuyer_master_sms_df.reset_index(inplace=True,drop=True)
        tirebuyer_master_df = pd.concat([tirebuyer_master_df,old_campaign_tirebuyer_master_sms_df])
        ###
        tirebuyer_master_df.reset_index(inplace=True,drop=True)
        #update into master file
        
        tirebuyer_master_original_df.reset_index(inplace=True,drop=True)
        tirebuyer_master_original_df = pd.concat([tirebuyer_master_original_df,tirebuyer_master_df])
        # tirebuyer_master_original_df.reset_index(inplace=True,drop=True)
        newfilename = 'tire_buyer-Tire Buyer Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer = pd.ExcelWriter(uri+newfilename, engine="xlsxwriter")
        tirebuyer_master_original_df.to_excel(writer, sheet_name="Data",index=False)
        tirebuyer_master_gacalcu_original_df.to_excel(writer, sheet_name="GA Calcu",index=True)
        workbook = writer.book
        worksheet_data = writer.sheets["Data"]
        worksheet_gacalcu = writer.sheets["GA Calcu"]
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
            dropboxdf = tirebuyer_master_original_df.copy()
            dropboxdf['Date'] = pd.to_datetime(dropboxdf['Date']).dt.strftime('%m/%d/%Y')
            main_module.to_dropbox(dropboxdf,dbx_path,client_name)

        ## insert data to bigquery
        col_rename = {
            "Test Type" :"Test_Type",
            "Type 2" :"Type_2",
            "Subject Line" : "Subject_Line",
            "Type 0" :"Type_0",
            "Type 1" :"Type_1",
            "Type 3" :"Type_3",
            "Message Title" : "Message_Title",
            "Total Bounce" : "Total_Bounces",
            "Google Analytics Campaign Name" :"Google_Analytics_Campaign_Name",
            "Google Analytics Campaign Content" : "Google_Analytics_Campaign_Content",
            "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
            "GA Revenue" : "GA_Revenue",
            "GA Orders" : "GA_Orders",
            "GA Sessions" : "GA_Sessions"
        }
        tirebuyer_master_original_df.rename(
            columns=col_rename,
            inplace=True
        )
        missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error']
        missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','Complaints']
        for i in missing_str_cols:
            tirebuyer_master_original_df[i] = '-'
        for i in missing_int_cols:
            tirebuyer_master_original_df[i] = 0.0
        tirebuyer_master_original_df['Original_Segment'] = tirebuyer_master_original_df['Segment']
        tirebuyer_master_original_df['FISCAL_YEAR'] = pd.to_datetime(tirebuyer_master_original_df['Date']).dt.year
        tirebuyer_master_original_df['FISCAL_YEAR_START'] = tirebuyer_master_original_df['Date']
        tirebuyer_master_original_df['FISCAL_WEEK'] = pd.to_datetime(tirebuyer_master_original_df['Date']).dt.isocalendar().week
        tirebuyer_master_original_df['FISCAL_MONTH'] = pd.to_datetime(tirebuyer_master_original_df['Date']).dt.month
        tirebuyer_master_original_df['FISCAL_QUARTER'] = pd.to_datetime(tirebuyer_master_original_df['Date']).dt.quarter
        cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","Message_Title","Type_3","ESP","Google_Analytics_Campaign_Name","Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Module_Name","Creatives","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK","FISCAL_MONTH","FISCAL_QUARTER"]
        tirebuyer_master_original_df = tirebuyer_master_original_df[cols_all]
        tirebuyer_master_original_df.to_csv(uri+'Masterlist.csv',index=False)
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
        bigquery.SchemaField('Message_Title', 'STRING'),
        bigquery.SchemaField('Type_3', 'STRING'),
        # bigquery.SchemaField('Name_Bkp', 'STRING'),
        bigquery.SchemaField('ESP', 'STRING'),
        # bigquery.SchemaField('Reads', 'STRING'),
        # bigquery.SchemaField('Total_Clicks', 'STRING'),
        # bigquery.SchemaField('Visits', 'STRING'),
        # bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
        # bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Module_Name', 'STRING'),
        bigquery.SchemaField('Creatives', 'STRING'),
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