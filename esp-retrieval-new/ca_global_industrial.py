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

def update_CA_globalindustrial(uri,client_id,client_esp):
    esp_module = __import__(client_esp)
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    trigger_ga_rev_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    trigger_ga_ses_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    # Read All files
    # Reading Campaign and Trigger Files
    ## check zip file
    # print("start zip check---------------")
    # archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    # main_module.checkZipFile(client_id,'ilovedooney-zipfile.zip',archive_datetime)
    # print("start reading files----------------")
    # list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    # uri = '/home/nav182/Downloads/AlchemyWrox/2023_0820-0826/' #local
    list_file = os.listdir(uri)
    sourcelistfiles = []
    for file in list_file:
        # file_name = str(file.name).replace(client_id+'/','') # REMOTE DEPLOY
        file_name =  file # local
        if 'CA-MessageActivity' in file_name:
            try:
                # df = pd.read_csv(uri + file_name) #remote
                df = pd.read_csv(uri + file_name ,encoding='unicode_escape', ) # local
                campaign_data_df = campaign_data_df.append(df)
                masterfile_filter_start_date = esp_module.get_trigger_dates(file_name,-2,"date")
                masterfile_filter_end_date = esp_module.get_trigger_dates(file_name,-1,"date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
                sourcelistfiles.append(file_name) 
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'CA-rev-Campaign Analytics' in file_name:
            try:
                # campaign_analytics_data_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                campaign_analytics_data_rev_df = pd.read_csv(uri + file_name,skiprows=6,encoding='unicode_escape',usecols=["Campaign","Total revenue","Conversions"],dtype= {'Campaign': str, 'Sessions': str}) # local
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
                return "rev-Campaign Error"
        elif 'CA-ses-Campaign Analytics' in file_name:
            try:
                # campaign_analytics_data_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                campaign_analytics_data_ses_df = pd.read_csv(uri + file_name,skiprows=6,encoding='unicode_escape',usecols=["Session campaign","Sessions"], dtype= {'Session campaign': str, 'Sessions': str}) # local
                campaign_analytics_data_ses_df = campaign_analytics_data_ses_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "ses-Campaign error"

        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name,encoding='unicode_escape') # local
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "CA-TriggerGArev" in file_name:
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
                print("trigger ga Error rev: ",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error : "+ str(e)
            
        elif "CA-TriggerGAses" in file_name:
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
        elif 'CA-ConversationActivity' in file_name:
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



    trigger_ga_df = pd.merge(trigger_ga_rev_df,trigger_ga_ses_df, on="Campaign",how="outer")
    print("PASS 1")
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
    print("PASS 500")

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

    campaign_data_df = pd.merge(campaign_data_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")

    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    print(campaign_data_df.columns)
    # add mailing column
    campaign_data_df["Mailing"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]
    print("Pass 600")
    campaign_data_df.reset_index(inplace=True, drop=True)
    campaign_data_df["Campaign"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"].str.split("_", n = 1, expand = True)[1].str.rstrip()
    campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df["Type 3"] = "CA"
    campaign_data_df["Type 2"] = "Email"
    campaign_data_df["Listrak Conversion Analytics Version"] = campaign_data_df["Segment"]

    # pull email link creatives data
    ## change variant, type 0 and type 1 based on Seed-list Segment value
    # campaign_data_df.loc[campaign_data_df["Segment"] == 'Seed-list',["Variant","Type 1"]] = ["Seed Test","Test"]
    matric_column = ["Sent","Delivered","Total Bounces","Unsub","Opens","Total Clicks","Clicks","Revenue","Orders","GA Revenue","GA Orders","GA Sessions","Data Acquisitions","Mobile Originated Messages"]
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
    # trigger_data_df.to_csv(uri + '/temp_mytriggerd667.csv',index=False)
    # trigger_book_df.to_csv(uri + '/temp_book_mytriggerd667.csv',index=False)
    trigger_book_df.drop_duplicates(["Google Analytics Campaign Name"],inplace=True,keep="last")
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on=["Google Analytics Campaign Name"], how="left") ## made changes
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
    # trigger_data_df["Type 3"] = ""
    # trigger_data_df["Type 2"] = "Email"
    trigger_data_df["Data Acquisitions"]="0"
    trigger_data_df["Mobile Originated Messages"]="0"
    trigger_data_df["Total Messages"]="0"
    trigger_data_df["Listrak Conversion Analytics Version"] = trigger_data_df["Segment"]
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,matric_column)
    print("going to process sms data")

    excelcol = ["Name","Campaign","Mailing","Variant","Subject Line","Date","ESP","Type 0","Type 1","Type 2","Type 3",	
                "Sent","Delivered","Total Bounces","Unsub","Opens","Total Clicks","Clicks","Revenue","Orders",
                "Google Analytics Campaign Name","Google Analytics Campaign Content",
                "Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version","Listrak Conversion Analytics Module Name","Segment",
                "GA Revenue","GA Orders","GA Sessions","Data Acquisitions","Mobile Originated Messages","Total Messages"]
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    print("pass 7")
    '''
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'CA_globalIndustrial_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
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
        '''

    return (trigger_data_df,campaign_data_df)