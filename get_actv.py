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
main_module = __import__('main')
client_name = 'GetACTV'
dbx_path = '/Client/Gaiam/Internal Files/Reporting/Tableau/GetACTV/'
def update_masterlist(uri,client_id,client_esp):
    print("getactv started")
    campaign_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    master_df = pd.DataFrame()
    master_gacalcu_df = pd.DataFrame()
    master_gacalcu_original_df = pd.DataFrame()
    session_df = pd.DataFrame()
    revenue_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame()
    trigger_ga_rev_df = pd.DataFrame()
    trigger_ga_ses_df = pd.DataFrame()
    master_original_df = pd.DataFrame()
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    #read the files from google bucket
    list_file = main_module.list_blobs(client_id)
    sourcelistfiles = []
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/getACTV/2023_0709-0715/' #local
    # list_file = os.listdir(uri)
    for file in list_file:
        file_name = str(file.name).replace(client_id+'/','')
        # file_name =  file # local
        if 'MessageActivity' in file_name:
            try:
                # df = pd.read_csv(uri + file_name) #remote
                df = pd.read_csv(uri + file_name) # local
                campaign_data_df = campaign_data_df.append(df)
                masterfile_filter_start_date = get_trigger_dates(file_name,-2,"date")
                masterfile_filter_end_date = get_trigger_dates(file_name,-1,"date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
                sourcelistfiles.append(file_name) 
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'SplitTestActivity' in file_name:
            try:
                abTesting_df = pd.read_csv(uri + file_name,usecols=["Group Name","Subject","Sent","Delivered","Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version"])
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'rev-Campaign Analytics' in file_name:
            try:
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
                campaign_analytics_data_ses_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Session campaign","Sessions"]) # local
                campaign_analytics_data_ses_df = campaign_analytics_data_ses_df.dropna()
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name,usecols=["Name","ESP","Type 0","Type 1"]) # local
                # trigger_book_df = trigger_book_df.drop(columns = ["Listrak Conversion Analytics Module Name"])

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
                trigger_date = get_trigger_dates(correct_filename,-2,"date")
                #get the trigger start date and end date
                s1 = get_trigger_dates(correct_filename,-2,"date")
                s2 = get_trigger_dates(correct_filename,-1,"date")
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
                trigger_date = get_trigger_dates(correct_filename,-2,"date")
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
            trigger_date = get_trigger_dates(file_name,-2,"date")
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
        elif "GetACTV Masterfile" in file_name:
            try:
                ## get the file name:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        master_df = pd.read_excel(xls, "Data")
                        master_gacalcu_df = pd.read_excel(xls, "OldCampaign",header=[0,1] )
                        master_gacalcu_df.drop(columns = master_gacalcu_df.columns[0], axis = 1, inplace= True)
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
        
    #Operation on files
    matric_column = ["Sent","Delivered","Hard Bounces","Unsub","Opens","Clicks","Revenue","Orders","GA Revenue","GA Orders","GA Sessions"]
    ## trigger merge
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
    print("trigger ga ==>",trigger_ga_df.shape)
    ###
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
    campaign_data_df["Date"] = campaign_data_df["Date"].apply(lambda x: convert_str_to_date(x))
    # remove extra column (%)
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV','Pass Along','Clicks','Visits','Reads'])
    #rename the column
    rename_col ={
        "Campaign":"Name",
        "Bounces" :"Hard Bounces",
        "Unsubs.":"Unsub",
        "Unique Clickers":"Clicks",
        "Revenue":"Revenue",
        "Conversions":"Orders",
        "Subject":"Subject Line",
        "Listrak Conversion Analytics Version":"Segment"
    }
    campaign_data_df.rename(
        columns=rename_col,
        inplace=True)
    
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
    # print(" columns",campaign_analytics_data_df.columns)
    campaign_data_df = pd.merge(campaign_data_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    # add campaign and mailing column
    campaign_data_df["Mailing"]=campaign_data_df["Name"]
    campaign_data_df["Campaign"]=campaign_data_df["Name"].str.split("-", n = 1, expand = True)[1].str.rstrip()
    #drop duplicates
    #campaign_data_df.drop_duplicates('Google Analytics Campaign Name',inplace=True,keep=False)
    ## put the value 0 for next duplicate
    campaign_data_df.reset_index(inplace=True, drop=True)
    ## add split testing data
    if not abTesting_df.empty:
        rename_col_split = {
            "Group Name":"Variant",
            "Subject" : "TestSubject",
            "Sent" :"TestSent",
            "Delivered" : "TestDelivered",
            "Listrak Conversion Analytics Campaign Name" : "Test Listrak Conversion Analytics Campaign Name",
            "Listrak Conversion Analytics Version" : "Test Listrak Conversion Analytics Version"
        }
        abTesting_df.rename(
            columns=rename_col_split,
            inplace=True
        )
        left_on = ["Subject Line","Sent","Delivered","Listrak Conversion Analytics Campaign Name","Segment"]
        right_on = ["TestSubject","TestSent","TestDelivered","Test Listrak Conversion Analytics Campaign Name","Test Listrak Conversion Analytics Version"]
        campaign_data_df = pd.merge(campaign_data_df,abTesting_df, left_on=left_on, right_on=right_on,how="left")
        campaign_data_df.drop(columns=right_on,inplace=True)
    else:
        campaign_data_df["Variant"]=""
    ## end
    ## find duplicate
    campaign_data_dup_df = campaign_data_df.loc[campaign_data_df.duplicated(['Google Analytics Campaign Name','Date'], keep="first"),:]
    campaign_data_df.drop_duplicates(['Google Analytics Campaign Name','Date'],inplace=True,keep="first")
    # FIll the value 0 of GA session, GA, order, GA revenue for duplicate row
    campaign_data_dup_df.loc[:,["GA Revenue","GA Orders","GA Sessions"]] = [0,0,0]
    campaign_data_df = pd.concat([campaign_data_df,campaign_data_dup_df])
    campaign_data_df = campaign_data_df.sort_index(ascending=True)
    campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df[["GA Revenue","GA Orders","GA Sessions"]] = campaign_data_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
    #extra fields
    campaign_data_df["Test_Type"]=""
    # campaign_data_df["Variant"]=""
    campaign_data_df["Type 2"]=""
    campaign_data_df["Offer"]=""
    campaign_data_df = convert_str_to_num(campaign_data_df,matric_column)
    # campaign_data_df.to_csv(uri+"promo.csv",index=False)
    print("pass 1")
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
        "Bounces":"Hard Bounces",
        "Unsubs." : "Unsub",
        "Opens": "Opens",
        "Clickers" : "Clicks",
        "Conversions": "Orders",
        "Subject" : "Subject Line",
        "Listrak Conversion Analytics Version":"Segment"
    }
    trigger_data_df.rename(
        columns=trigger_rename_col,
        inplace= True
    )
    # remove row having sent value 0
    trigger_data_df = convert_str_to_num(trigger_data_df,["Sent"])
    trigger_data_df = trigger_data_df[trigger_data_df["Sent"] > 0]
    
    trigger_data_df["Delivered"] = trigger_data_df["Sent"]
    trigger_data_df_original = trigger_data_df.copy()
    trigger_data_df = pd.merge(trigger_data_df,trigger_ga_df,left_on="Google Analytics Campaign Name",right_on="Campaign", how="left")
    #remove duplicate row
    # trigger_data_df.drop(trigger_data_df[(trigger_data_df['Date'] != trigger_data_df['triggerDate']) & (not trigger_data_df['triggerDate'].isnull().values.any())].index, inplace = True)

    # print(trigger_data_df.loc[:,["Name","Date","triggerDate"]])
    trigger_data_df_isnull = trigger_data_df[trigger_data_df["triggerDate"].isnull()]
    trigger_data_df  = trigger_data_df[trigger_data_df["triggerDate"].notnull()]
    trigger_data_df.drop(trigger_data_df[trigger_data_df['Date'] != trigger_data_df['triggerDate']].index, inplace = True)
    if not trigger_data_df_isnull.empty:
        trigger_data_df =  pd.concat([trigger_data_df,trigger_data_df_isnull])
        # trigger_data_df.reset_index(inplace=True, drop=True)
        trigger_data_df = trigger_data_df.sort_index(ascending=True)
    ## add missing row from left table
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_df_original])
    trigger_data_df.drop_duplicates(['Name','Subject Line','Google Analytics Campaign Name','Date','Sent','Revenue','Orders','Segment'],inplace=True,keep="first")
    del trigger_data_df_original
    #reset index
    trigger_data_df.reset_index(inplace=True, drop=True)
    trigger_data_df[["GA Revenue","GA Orders","GA Sessions"]] = trigger_data_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
    trigger_data_dup_df = trigger_data_df.loc[trigger_data_df.duplicated(['Google Analytics Campaign Name','Date'], keep="first"),:]
    print("pass 2")
    trigger_data_df.drop_duplicates(['Google Analytics Campaign Name','Date'],inplace=True,keep="first")
    # FIll the value 0 of GA session, GA, order, GA revenue for duplicate row
    trigger_data_dup_df.loc[:,["GA Revenue","GA Orders","GA Sessions"]] = [0,0,0]
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_dup_df])
    trigger_data_df = trigger_data_df.sort_index(ascending=True)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on="Name", how="left")
    trigger_data_df.drop(columns=["triggerDate","Campaign"],inplace=True)
    trigger_data_df["Campaign"]=""
    trigger_data_df["Mailing"]=""
    trigger_data_df["Test_Type"]=""
    trigger_data_df["Offer"]=""
    trigger_data_df["Variant"]=""
    trigger_data_df["Type 2"] =""
    # # remove row having sent value 0
    # trigger_data_df = convert_str_to_num(trigger_data_df,["Sent"])
    # trigger_data_df = trigger_data_df[trigger_data_df["Sent"] > 0]
    trigger_data_df = convert_str_to_num(trigger_data_df,matric_column)
    # trigger_data_df.to_csv(uri+"mytrigger.csv",index=False)

    excelcol = ["Name","Campaign","Mailing","Variant","Subject Line","Date","ESP","Type 0","Type 1","Type 2","Offer","Test_Type",
                "Sent","Delivered","Hard Bounces","Unsub","Opens","Clicks","Revenue","Orders",
                "GA Revenue","GA Orders","GA Sessions","Google Analytics Campaign Name","Google Analytics Campaign Content",
                "Listrak Conversion Analytics Campaign Name","Segment","Listrak Conversion Analytics Module Name"]
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    print("pass 3")
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'GetACTV_Book_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer1 = pd.ExcelWriter(uri+newreportfilename, engine="xlsxwriter")
        campaign_data_df.to_excel(writer1, sheet_name="Promo",index=False)
        trigger_data_df.to_excel(writer1, sheet_name="Trigger",index=False)
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
        master_df["Date"] = master_df["Date"].apply(lambda x: convert_str_to_date(x))
        master_original_df = master_df.copy()
        master_df = master_df[(master_df["Date"] >= masterfile_filter_start_date) & (master_df["Date"] <= masterfile_filter_end_date)]
        master_original_df.drop(master_original_df[(master_original_df['Date'] >= masterfile_filter_start_date) & (master_original_df['Date'] <= masterfile_filter_end_date)].index, inplace = True)
        #gacalcu tab
        master_gacalcu_df[("Master","Date")] = master_gacalcu_df[("Master","Date")].apply(lambda x:convert_str_to_date(x))
        master_gacalcu_original_df = master_gacalcu_df.copy()
        master_gacalcu_df = master_gacalcu_df[(master_gacalcu_df["Master"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_df["Master"]["Date"] <= masterfile_filter_end_date)]
        master_gacalcu_original_df.drop(master_gacalcu_original_df[(master_gacalcu_original_df["Master"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_original_df["Master"]["Date"] <= masterfile_filter_end_date)].index, inplace = True)

        ## filter with type 0 = Campaign and Type 1 = Promo
    
        master_df.drop(master_df[(master_df["Type 0"] == "Campaign" ) & (master_df["Type 1"] == "Promo")].index, inplace = True)
        master_df.drop(master_df[master_df["Type 1"] == "Old Campaign" ].index, inplace = True)
        #convert date type from string to date
        campaign_data_df["Date"] = campaign_data_df["Date"].apply(lambda x: convert_str_to_date(x,'%m/%d/%Y'))
        trigger_data_df["Date"] = trigger_data_df["Date"].apply(lambda x: convert_str_to_date(x,'%m/%d/%Y'))
        # paste the data from promo tab and trigger tab to master file
        master_df = pd.concat([master_df,campaign_data_df])
        master_df = pd.concat([master_df,trigger_data_df])
    # add seven row in ga calcu sheet
    #typecast the state date and end date to string
    old_campaign_data = []
    old_campaign_Master_data_df = []
    master_gacalcu_old_df = master_gacalcu_df.copy()
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
            revenue_data = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign"), "GA Revenue"].sum()
            order_data = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign"), "GA Orders"].sum()
            session_data = master_df.loc[(master_df['Date'] == newsdate) & (master_df['Name'] != "Old Campaign"), "GA Sessions"].sum()
            mydata.append([revenue_data,order_data,session_data,0,0,0,newsdate,0,0,0])
            old_campaign_data.append(["Old Campaign",np.nan,np.nan,np.nan,np.nan,newsdate,np.nan,'Campaign','Old Campaign',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan,np.nan,np.nan,np.nan,np.nan])
        
        master_gacalcu_df = pd.DataFrame(mydata, columns=master_gacalcu_df.columns)
        # master_gacalcu_df = pd.concat([master_gacalcu_df,new_gacalcu_df])
        master_gacalcu_df.reset_index(inplace=True, drop=True)
        ### read session and revenue file session_df , revenue_df
        print("=====> pass4")
        revenue_df = revenue_df.dropna()
        session_df = session_df.dropna()
        revenue_df["Day Index"] = revenue_df["Day Index"].apply(lambda x: convert_str_to_date(x,'%Y%m%d'))
        session_df["Day Index"] = session_df["Day Index"].apply(lambda x: convert_str_to_date(x,'%Y%m%d'))
        # combine the session order and revenue data by date
        revenue_df = pd.merge(revenue_df,session_df,on="Day Index",how="inner")
        # revenue_df["Day Index"] = revenue_df["Day Index"].apply(lambda x: datetime.strptime(x,'%m/%d/%Y').date())
        revenue_df = convert_str_to_num(revenue_df,["Transactions","Revenue","Sessions"])
        revenue_df.rename(
            columns={
                "Transactions":"GA|Orders",
                "Day Index":"Master|Date",
                "Revenue" : "GA|Revenue",
                "Sessions":"GA|Sessions"
            },
            inplace=True
        )
        
        # master_gacalcu_df.drop(columns="GA",inplace=True)
        # master_gacalcu_df["GA"] = ga_col_data
        master_gacalcu_df.columns = master_gacalcu_df.columns.map('|'.join).str.strip('|')
        master_gacalcu_old_df.columns = master_gacalcu_old_df.columns.map('|'.join).str.strip('|')
        master_gacalcu_old_df = master_gacalcu_old_df[["Master|Date","GA|Orders","GA|Revenue","GA|Sessions"]]
        # master_gacalcu_df = pd.merge(master_gacalcu_df,revenue_df,on="Master|Date",how="left")
        master_gacalcu_old_df = master_gacalcu_old_df.set_index("Master|Date")
        master_gacalcu_df = master_gacalcu_df.set_index("Master|Date")
        revenue_df = revenue_df.set_index("Master|Date")
        master_gacalcu_df.update(revenue_df)
        master_gacalcu_df.update(master_gacalcu_old_df)
        master_gacalcu_df.reset_index(inplace=True)
        print("pass 5")
        #convert str to number
        master_gacalcu_df = convert_str_to_num(master_gacalcu_df,["GA|Revenue","GA|Orders","GA|Sessions","ESP|Revenue","ESP|Orders","ESP|Sessions"])
        master_gacalcu_df["Master|Revenue"] = master_gacalcu_df["GA|Revenue"] - master_gacalcu_df["ESP|Revenue"]
        master_gacalcu_df["Master|Orders"] = master_gacalcu_df["GA|Orders"] - master_gacalcu_df["ESP|Orders"]
        master_gacalcu_df["Master|Sessions"] = master_gacalcu_df["GA|Sessions"] - master_gacalcu_df["ESP|Sessions"]
        old_campaign_Master_data_df = master_gacalcu_df[["Master|Revenue","Master|Orders","Master|Sessions","Master|Date"]].copy()
        mycol = master_gacalcu_df.columns
        tuple_col =[]
        for c in mycol:
            newcol = c.split("|")
            newcol_tup = tuple(newcol)
            tuple_col.append(newcol_tup)
        # create multi index dataframe
        mycol1 = pd.MultiIndex.from_tuples(tuple_col)
        print("pass 6")
        master_gacalcu_df = master_gacalcu_df.set_axis(mycol1,axis='columns')
        # reording columns
        master_gacalcu_df = master_gacalcu_df[["ESP","GA","Master"]]
        master_gacalcu_original_df = master_gacalcu_original_df[["ESP","GA","Master"]]
        # print(master_gacalcu_df)
        print("pass 7")
        master_gacalcu_original_df = pd.concat([master_gacalcu_original_df, master_gacalcu_df])
        master_gacalcu_original_df = master_gacalcu_original_df[master_gacalcu_original_df[("Master","Date")].notnull()]
        master_gacalcu_original_df[("Master","Date")] = master_gacalcu_original_df[("Master","Date")].apply(lambda x: x.strftime("%m/%d/%Y"))
        master_gacalcu_original_df.reset_index(inplace=True,drop=True)
    ### add Master revenue, order and session to old campaign in master file data tab
    if not master_df.empty:
        ## remove the taken dataset
        ## create new Data frame for old campaign
        old_campaign_master_df = pd.DataFrame(old_campaign_data, columns=master_df.columns)
        # old_campaign_master_df = pd.concat([old_campaign_master_df,old_campaign_data_df])
        old_campaign_master_df.drop(columns=["GA Revenue","GA Orders","GA Sessions"], inplace=True)
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        #pull the Master data from ga calcu
        old_campaign_Master_data_df.rename(columns={
            "Master|Revenue" : "GA Revenue",
            "Master|Orders" : "GA Orders",
            "Master|Sessions":"GA Sessions",
            "Master|Date" :"Date"
        },inplace=True)
        old_campaign_master_df = pd.merge(old_campaign_master_df,old_campaign_Master_data_df,on="Date",how="left")
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        # print(old_campaign_master_df.loc[:,["Name","Date","GA Revenue","GA Orders","GA Sessions"]])
        master_df = pd.concat([master_df,old_campaign_master_df])
        master_df.reset_index(inplace=True,drop=True)
        #update into master file
    
        master_original_df.reset_index(inplace=True,drop=True)
        master_original_df = pd.concat([master_original_df,master_df])
        # master_original_df.reset_index(inplace=True,drop=True)
        newfilename = 'get_actv-GetACTV Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer = pd.ExcelWriter(uri+newfilename, engine="xlsxwriter")
        master_original_df.to_excel(writer, sheet_name="Data",index=False)
        master_gacalcu_original_df.to_excel(writer, sheet_name="OldCampaign",index=True)
        workbook = writer.book
        worksheet_data = writer.sheets["Data"]
        worksheet_gacalcu = writer.sheets["OldCampaign"]
        # Add some cell formats.
        format1 = workbook.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_data.set_column(18, 18, None, format1)
        worksheet_data.set_column(25, 25, None, format1)
        worksheet_gacalcu.set_column(1, 1, None, format1)
        worksheet_gacalcu.set_column(4, 4, None, format1)
        worksheet_gacalcu.set_column(8, 8, None, format1)
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
            "Hard Bounces" : "Hard_Bounces",
            "Google Analytics Campaign Name" :"Google_Analytics_Campaign_Name",
            "Google Analytics Campaign Content" : "Google_Analytics_Campaign_Content",
            "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
            "GA Revenue" : "GA_Revenue",
            "GA Orders" : "GA_Orders",
            "GA Sessions" : "GA_Sessions"
        }
        master_original_df.rename(
            columns=col_rename,
            inplace=True
        )
        missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error']
        missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Total_Bounces','Soft_Bounces','Complaints']
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
        cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","ESP","Google_Analytics_Campaign_Name","Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Module_Name","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK","FISCAL_MONTH","FISCAL_QUARTER"]
        master_original_df = master_original_df[cols_all]
        master_original_df.to_csv(uri+'Masterlist.csv',index=False)
        # remove previous version of data  and insert new version of data
        main_module.delete_table(client_id)
        bigquery_insert(client_id)
        ### move the file to archive folder
        sourcelistfiles.append(newfilename)
        sourcelistfiles.append(newreportfilename)
        sourcelistfiles.append('Masterlist.csv')
        archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    for sfile in sourcelistfiles:
        main_module.copy_blob('reporter-etl',client_id + '/' + sfile,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +'/'+ sfile)
        if newfilename != sfile:
            main_module.delete_blob('reporter-etl',client_id + '/' + sfile)
    print(">>>>>>>>>>>>>>>> end")
    return ""


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
    return ""



######### amit sinha ####################
def convert_str_to_num(df,metric_columns):
    df[metric_columns] =  df[metric_columns].fillna('0')
    df[metric_columns] = df[metric_columns].replace('|'.join([',', '\$']), '',regex=True)
    for col in metric_columns:
        df[col] = pd.to_numeric(df[col])
    return df

def convert_str_to_date(data,format='%m/%d/%Y'):
    mydate = None
    try:
        mydate = data.date()
    except:
        data  = str(data)
        try:
            data = data.split(" ")[0]
        except:
            pass
        if(len(data) > 5):
            if("-" in data):
                newformat = data.split("-")
                if(len(newformat[0]) == 4):
                    format = "%Y-%m-%d"
                elif (len(newformat[-1]) == 4):
                    format = "%m-%d-%Y"
                else:
                    format = "%d-%m-%Y"
            mydate = dt.datetime.strptime(data,format)
            mydate = mydate.date()
    return mydate

def get_trigger_dates(file_name, position=-1,datatype="String"):
    temp_file_name = file_name.split('_')
    temp_first_date = temp_file_name[position]
    first_date = temp_first_date[4:6] + '/' + temp_first_date[6:8] + '/' + temp_first_date[0:4]
    if datatype == "date":
        first_date = dt.datetime.strptime(first_date, "%m/%d/%Y")
        first_date = first_date.date()
    return(first_date)