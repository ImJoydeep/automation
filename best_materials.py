import numpy as np
import pandas as pd
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import datetime as dt

main_module = __import__('main')
client_name = 'Best Materials'
dbx_path = '/Client/Best Materials/Internal Files/Reporting/Tableau/'

def update_masterlist(uri,client_id,client_esp):
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    mailingtagging_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    master_df = pd.DataFrame()
    master_gacalcu_df = pd.DataFrame()
    master_gacalcu_original_df = pd.DataFrame()
    session_df = pd.DataFrame()
    revenue_df = pd.DataFrame()
    trigger_ga_rev_df = pd.DataFrame()
    trigger_ga_ses_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    master_original_df = pd.DataFrame()
    #######
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    print("start zip check---------------")
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    main_module.checkZipFile(client_id,'best_materials-zipfile.zip',archive_datetime)
    print("start reading files----------------")
    # Read All files
    # Reading Campaign and Trigger Files
    list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/best_materials/2023_0709-0715/' #local
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
                masterfile_filter_start_date = get_trigger_dates(file_name,-2,"date")
                masterfile_filter_end_date = get_trigger_dates(file_name,-1,"date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
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
                mailingtagging_df = mailingtagging_df[mailingtagging_df["Client"] == "Best Materials"]
                mailingtagging_df = mailingtagging_df.drop(columns=["Date","Client"])
                mailingtagging_df.reset_index(inplace=True)
                # mailingtagging_df.rename(
                #     columns={"Variant":"Variant2"},
                #     inplace=True
                # )
                # print("mailingtagging_df ",mailingtagging_df.columns)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name) # local
                # trigger_book_df = trigger_book_df.drop(columns = ["Listrak Conversion Analytics Module Name"])
                trigger_book_df.rename(
                    columns= {
                        "Google Analytics Campaign Name": "GA Campaign"
                    },
                    inplace=True
                )
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
                        "Conversions": "GA Order",
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
                tdf['Send Date'] = trigger_date
                trigger_data_df = trigger_data_df.append(tdf)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
        elif "Best Materials Masterfile" in file_name:
            try:
                ## get the file name:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        master_df = pd.read_excel(xls, "Data")
                        master_gacalcu_df = pd.read_excel(xls, "Old Campaign",header=[0,1])
                        if master_gacalcu_df.columns[0][0] == "Unnamed: 0_level_0":
                            master_gacalcu_df.drop(columns = master_gacalcu_df.columns[0], axis = 1, inplace= True)
                    sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr msg ====>",e)
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
                print("old campaign sess",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
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
                print("old campaign rev",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
    #Operation on files
    matric_column = ["Sent","Delivered","Total Bounces","Unsubs.","Opens","Clicks","Revenue","Conversions","GA Revenue","GA Order","GA Sessions"]
    ## trigger merge
    trigger_ga_df = pd.merge(trigger_ga_rev_df,trigger_ga_ses_df, on="Campaign",how="outer")
    trigger_ga_df.loc[trigger_ga_df["triggerDate"].isnull(),'triggerDate'] = trigger_ga_df["triggerDateses"]
    trigger_ga_dup1_df = trigger_ga_df.loc[trigger_ga_df.duplicated(['Campaign','GA Revenue','GA Order','triggerDate'], keep="first"),:]
    trigger_ga_df.drop_duplicates(['Campaign','GA Revenue','GA Order','triggerDate'],inplace=True,keep="first")
    trigger_ga_dup2_df = trigger_ga_df.loc[trigger_ga_df.duplicated(['Campaign','GA Sessions','triggerDateses'], keep="first"),:]
    trigger_ga_df.drop_duplicates(['Campaign','GA Sessions','triggerDateses'],inplace=True,keep="first")
    trigger_ga_dup1_df.loc[:,["GA Revenue","GA Order"]] = [0,0]
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
            cam_col_list.append("Send Date")
        else:
            cam_col_list.append(cam_col)
            
    # print(cam_col_list)
    campaign_data_df = campaign_data_df.set_axis(cam_col_list,axis="columns")
    # remove time
    # campaign_data_df['Date'] = campaign_data_df['Date'].apply(lambda x: x.strip().split(" ")[0])
    campaign_data_df['Send Date'] = campaign_data_df['Send Date'].apply(lambda x: convert_str_to_date(x))
    # remove extra column (%)
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV','Pass Along'])
    #print(campaign_data_df.columns)
    #### pull GA Revenue, GA Order, and GA Session data from GA TAB campaign_analytics_data_df Campaign Analytics.csv file
    campaign_analytics_data_df = pd.merge(campaign_analytics_data_rev_df,campaign_analytics_data_ses_df, on="Session campaign",how="outer")
    campaign_analytics_data_df = campaign_analytics_data_df.fillna(0)
    ga_rename_col ={
        "Session campaign" :"ga_campaign",
        "Total revenue":"GA Revenue",
        "Conversions": "GA Order",
        "Sessions" : "GA Sessions"
    }
    campaign_analytics_data_df.rename(
        columns=ga_rename_col,
        inplace=True)
    campaign_data_df = pd.merge(campaign_data_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
    print("pass 1")
    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    # prepare AB Testing df
    if not abTesting_df.empty:
        # find the name of date column
        ab_col_list1 = abTesting_df.columns
        ab_col_list = []
        for cam_col in ab_col_list1:
            if "Send Date" in cam_col:
               ab_col_list.append("Send Date")
            else:
                ab_col_list.append(cam_col)
        abTesting_df = abTesting_df.set_axis(ab_col_list,axis="columns")
        # remove time
        # abTesting_df['Date'] = abTesting_df['Date'].apply(lambda x: x.strip().split(" ")[0])
        abTesting_df['Send Date'] = abTesting_df['Send Date'].apply(lambda x: convert_str_to_date(x))
        # remove extra column (%)
        abTesting_df = abTesting_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV','Pass Along'])
        #rename column of AB Testing
        ab_rename_col = {
            "Bounces" :"Total Bounces",
        }
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
        abTesting_dup_df.loc[abTesting_dup_df[abTesting_dup_df["Variant"] == "B"].index,["GA Revenue","GA Order","GA Sessions"]] = [0,0,0]
        abTesting_df = pd.concat([abTesting_df,abTesting_dup_df])
        abTesting_df = abTesting_df.sort_index(ascending=True)
        # print(abTesting_dup_df)
        ## add A or B in Mailing name
        abTesting_df["Mailing"]=abTesting_df["Listrak Conversion Analytics Campaign Name"]
        # abTesting_df["Mailing"] = abTesting_df.apply(lambda x :concat_mailing_varient(x["Mailing"],x["Variant"]),axis=1 )
        #find dupliate in promo tab and remove and paste the data of ab testing to promo tab
        campaign_data_df.drop_duplicates('Google Analytics Campaign Name',inplace=True,keep=False)
        campaign_data_df.reset_index(inplace=True, drop=True)
        campaign_data_df = pd.concat([campaign_data_df,abTesting_df],ignore_index=True)
    
    # add by variant column
    campaign_data_df["By Variant"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]
    campaign_data_df.reset_index(inplace=True, drop=True)
    # campaign_data_df["Campaign"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"].str.split("-", n = 1, expand = True)[1].str.rstrip()
    # campaign_data_df["Mailing"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]
    # pull Test Type Variant Offer and Type 2 data from mailing tagging file
    campaign_data_df = pd.merge(campaign_data_df,mailingtagging_df,left_on ="By Variant" ,right_on="Mailing",how="left")
    # campaign_data_df.loc[campaign_data_df["Variant"] == "",'Variant'] = campaign_data_df["Variant2"]
    # campaign_data_df = campaign_data_df.drop(columns = ["Variant2"])
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    #rename the column
    rename_col ={
        "Bounces" :"Total Bounces",
        "Test Type" : "Test_Type"
    }
    campaign_data_df.rename(
        columns=rename_col,
        inplace=True)
    # change datatype of number columns from string to number
    campaign_data_df = convert_str_to_num(campaign_data_df,matric_column)
    #trigger tab data
    trig_col_list1 = trigger_data_df.columns
    trig_col_list = []
    for trig_col in trig_col_list1:
        if "Publish Date" in trig_col:
            trig_col_list.append("Publish Date")
        else:
            trig_col_list.append(trig_col)
    
    trigger_data_df = trigger_data_df.set_axis(trig_col_list,axis="columns")
    trigger_data_df = trigger_data_df.drop(columns = ['%','%.1','%.2','%.3','%.4','Conversion Rate','AOV','Pass Along','Publish Date'])
    trigger_rename_col={
        "Thread / Step": "Campaign",
        "Bounces":"Total Bounces",
        "Clickers" : "Unique Clickers"
    }
    trigger_data_df.rename(
        columns=trigger_rename_col,
        inplace= True
    )
    trigger_data_df["Delivered"] = trigger_data_df["Sent"]
    #pull the value of GA order, GA Session, GA Revenue
    print("trigger operation===========>")
    trigger_data_df_original = trigger_data_df.copy()
    trigger_ga_df.rename(
        columns={
            "Campaign":"TriggerCampaign"
        },inplace=True
    )
    trigger_data_df = pd.merge(trigger_data_df,trigger_ga_df,left_on="Google Analytics Campaign Name",right_on="TriggerCampaign", how="left")
    print("pass 3",trigger_data_df.shape)
    #remove duplicate row
    # trigger_data_df.drop(trigger_data_df[(trigger_data_df['Date'] != trigger_data_df['triggerDate']) & (not trigger_data_df['triggerDate'].isnull().values.any())].index, inplace = True)

    # print(trigger_data_df.loc[:,["Name","Date","triggerDate"]])
    trigger_data_df_isnull = trigger_data_df[trigger_data_df[("triggerDate")].isnull()]
    trigger_data_df  = trigger_data_df[trigger_data_df[("triggerDate")].notnull()]
    trigger_data_df.drop(trigger_data_df[trigger_data_df['Send Date'] != trigger_data_df['triggerDate']].index, inplace = True)
    if not trigger_data_df_isnull.empty:
        trigger_data_df =  pd.concat([trigger_data_df,trigger_data_df_isnull])
        # trigger_data_df.reset_index(inplace=True, drop=True)
        trigger_data_df = trigger_data_df.sort_index(ascending=True)
    ## add missing row from left table
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_df_original])
    trigger_data_df.drop_duplicates(['Campaign','Subject','Google Analytics Campaign Name','Send Date','Sent','Revenue','Conversions'],inplace=True,keep="first")
    del trigger_data_df_original
    #reset index
    trigger_data_df.reset_index(inplace=True, drop=True)
    trigger_data_df[["GA Revenue","GA Order","GA Sessions"]] = trigger_data_df[["GA Revenue","GA Order","GA Sessions"]].fillna(value=0)
    trigger_data_dup_df = trigger_data_df.loc[trigger_data_df.duplicated(['Google Analytics Campaign Name','Send Date'], keep="first"),:]
    trigger_data_df.drop_duplicates(['Google Analytics Campaign Name','Send Date'],inplace=True,keep="first")
    # FIll the value 0 of GA session, GA, order, GA revenue for duplicate row
    trigger_data_dup_df.loc[:,["GA Revenue","GA Order","GA Sessions"]] = [0,0,0]
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_dup_df])
    trigger_data_df = trigger_data_df.sort_index(ascending=True)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, left_on="Campaign",right_on="GA Campaign", how="left")
    ## remove GA Campaign
    # print("pass 8")
    # trigger_data_df["Campaign"]=""
    # trigger_data_df["Mailing"]=""
    trigger_data_df["Test_Type"]=""
    trigger_data_df["Variant"]=""
    trigger_data_df["Offer"]=""
    trigger_data_df["By Variant"] = ""
    trigger_data_df = convert_str_to_num(trigger_data_df,matric_column)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    excelcol = ["Send Date","Campaign","By Variant","Variant","Test_Type","Offer","Subject","Type 0","Type 1",	
                "Type 2","Sent","Delivered","Total Bounces","Unsubs.","Opens","Reads","Clicks","Unique Clickers","Revenue","Visits","Conversions",
                "Google Analytics Campaign Name","Listrak Conversion Analytics Version",
                "Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Module Name",
                "GA Revenue","GA Order","GA Sessions"]
    #re-arrange the columns
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    print("pass 4")
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'Best_Materials_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
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
    print("pass 5")
    ###### master file ######
    if not master_df.empty:
        master_df["Send Date"] = master_df["Send Date"].apply(lambda x: convert_str_to_date(x))
        master_original_df = master_df.copy()
        master_df = master_df[(master_df["Send Date"] >= masterfile_filter_start_date) & (master_df["Send Date"] <= masterfile_filter_end_date)]
        master_original_df.drop(master_original_df[(master_original_df["Send Date"] >= masterfile_filter_start_date) & (master_original_df["Send Date"] <= masterfile_filter_end_date)].index, inplace = True)
        master_original_df = convert_str_to_num(master_original_df,["GA Revenue"])
        master_df = convert_str_to_num(master_df,["GA Revenue"])
        #gacalcu tab
        master_gacalcu_df[("ESP Revenue","Date")] = master_gacalcu_df[("ESP Revenue","Date")].apply(lambda x:convert_str_to_date(x))
        master_gacalcu_original_df = master_gacalcu_df.copy()
        master_gacalcu_df = master_gacalcu_df[(master_gacalcu_df["ESP Revenue"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_df["ESP Revenue"]["Date"] <= masterfile_filter_end_date)]
        master_gacalcu_original_df.drop(master_gacalcu_original_df[(master_gacalcu_original_df["ESP Revenue"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_original_df["ESP Revenue"]["Date"] <= masterfile_filter_end_date)].index, inplace = True)

        ## filter with type 0 = Campaign and Type 1 = Promo
    
        master_df.drop(master_df[(master_df["Type 0"] == "Campaign" ) & (master_df["Type 1"] == "Promo")].index, inplace = True)
        master_df.drop(master_df[master_df["Type 1"] == "Old Campaign" ].index, inplace = True)
        # #convert date type from string to date
        # campaign_data_df["Date"] = campaign_data_df["Date"].apply(lambda x: convert_str_to_date(x,'%m/%d/%Y'))
        # trigger_data_df["Date"] = trigger_data_df["Date"].apply(lambda x: convert_str_to_date(x,'%m/%d/%Y'))
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
            revenue_data = master_df.loc[(master_df['Send Date'] == newsdate) & (master_df['Campaign'] != "Old Campaign"), "GA Revenue"].sum()
            order_data = master_df.loc[(master_df['Send Date'] == newsdate) & (master_df['Campaign'] != "Old Campaign"), "GA Order"].sum()
            session_data = master_df.loc[(master_df['Send Date'] == newsdate) & (master_df['Campaign'] != "Old Campaign"), "GA Sessions"].sum()
            mydata.append([0,0,0,newsdate,revenue_data,order_data,session_data,0,0,0])
            old_campaign_data.append([newsdate,"Old Campaign",np.nan,np.nan,np.nan,np.nan,np.nan,'Campaign','Old Campaign',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0])
        
        master_gacalcu_df = pd.DataFrame(mydata, columns=master_gacalcu_df.columns)
        # master_gacalcu_df = pd.concat([master_gacalcu_df,new_gacalcu_df])
        master_gacalcu_df.reset_index(inplace=True, drop=True)
        ### read session and revenue file session_df , revenue_df
        print("=====> pass6")
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
                "Transactions":"GA Revenue|Order",
                "Day Index":"ESP Revenue|Date",
                "Revenue" : "GA Revenue|Revenue",
                "Sessions":"GA Revenue|Session"
            },
            inplace=True
        )
        
        # master_gacalcu_df.drop(columns="GA",inplace=True)
        # master_gacalcu_df["GA"] = ga_col_data
        master_gacalcu_df.columns = master_gacalcu_df.columns.map('|'.join).str.strip('|')
        master_gacalcu_old_df.columns = master_gacalcu_old_df.columns.map('|'.join).str.strip('|')
        master_gacalcu_old_df = master_gacalcu_old_df[["ESP Revenue|Date","GA Revenue|Order","GA Revenue|Revenue","GA Revenue|Session"]]
        # master_gacalcu_df = pd.merge(master_gacalcu_df,revenue_df,on="Old Campaign|Date",how="left")
        master_gacalcu_old_df = master_gacalcu_old_df.set_index("ESP Revenue|Date")
        master_gacalcu_df = master_gacalcu_df.set_index("ESP Revenue|Date")
        revenue_df = revenue_df.set_index("ESP Revenue|Date")
        master_gacalcu_df.update(revenue_df)
        master_gacalcu_df.update(master_gacalcu_old_df)
        master_gacalcu_df.reset_index(inplace=True)
        print("pass 6.1")
        
        #convert str to number
        master_gacalcu_df = convert_str_to_num(master_gacalcu_df,["GA Revenue|Revenue","GA Revenue|Order","GA Revenue|Session","ESP Revenue|Revenue","ESP Revenue|Order","ESP Revenue|Session"])
        print("going for calc")
        master_gacalcu_df["Old Campaign|Revenue"] = master_gacalcu_df["GA Revenue|Revenue"] - master_gacalcu_df["ESP Revenue|Revenue"]
        master_gacalcu_df["Old Campaign|Order"] = master_gacalcu_df["GA Revenue|Order"] - master_gacalcu_df["ESP Revenue|Order"]
        master_gacalcu_df["Old Campaign|Session"] = master_gacalcu_df["GA Revenue|Session"] - master_gacalcu_df["ESP Revenue|Session"]
        old_campaign_Master_data_df = master_gacalcu_df[["Old Campaign|Revenue","Old Campaign|Order","Old Campaign|Session","ESP Revenue|Date"]].copy()
        mycol = master_gacalcu_df.columns
        tuple_col =[]
        for c in mycol:
            newcol = c.split("|")
            newcol_tup = tuple(newcol)
            tuple_col.append(newcol_tup)
        # create multi index dataframe
        mycol1 = pd.MultiIndex.from_tuples(tuple_col)
        print("pass 7")
        master_gacalcu_df = master_gacalcu_df.set_axis(mycol1,axis='columns')
        # reording columns
        master_gacalcu_df = master_gacalcu_df[["GA Revenue","ESP Revenue","Old Campaign"]]
        master_gacalcu_original_df = master_gacalcu_original_df[["GA Revenue","ESP Revenue","Old Campaign"]]
        # print(master_gacalcu_df)
        print("pass 8")
        master_gacalcu_original_df = pd.concat([master_gacalcu_original_df, master_gacalcu_df])
        master_gacalcu_original_df = master_gacalcu_original_df[master_gacalcu_original_df[("ESP Revenue","Date")].notnull()]
        master_gacalcu_original_df[("ESP Revenue","Date")] = master_gacalcu_original_df[("ESP Revenue","Date")].apply(lambda x: x.strftime("%m/%d/%Y"))
        master_gacalcu_original_df.reset_index(inplace=True,drop=True)
    ### add Master revenue, order and session to old campaign in master file data tab
    if not master_df.empty:
        ## remove the taken dataset
        ## create new Data frame for old campaign
        old_campaign_master_df = pd.DataFrame(old_campaign_data, columns=master_df.columns)
        # old_campaign_master_df = pd.concat([old_campaign_master_df,old_campaign_data_df])
        old_campaign_master_df.drop(columns=["GA Revenue","GA Order","GA Sessions"], inplace=True)
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        #pull the Master data from ga calcu
        old_campaign_Master_data_df.rename(columns={
            "Old Campaign|Revenue" : "GA Revenue",
            "Old Campaign|Order" : "GA Order",
            "Old Campaign|Session":"GA Sessions",
            "ESP Revenue|Date" :"Send Date"
        },inplace=True)
        print("pass 9")
        old_campaign_master_df = pd.merge(old_campaign_master_df,old_campaign_Master_data_df,on="Send Date",how="left")
        old_campaign_master_df.reset_index(inplace=True,drop=True)
        # print(old_campaign_master_df.loc[:,["Name","Date","GA Revenue","GA Order","GA Sessions"]])
        master_df = pd.concat([master_df,old_campaign_master_df])
        master_df.reset_index(inplace=True,drop=True)
        #update into master file
    
        master_original_df.reset_index(inplace=True,drop=True)
        master_original_df = pd.concat([master_original_df,master_df])
        # master_original_df.reset_index(inplace=True,drop=True)
        newfilename = 'best_materials-Best Materials Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer = pd.ExcelWriter(uri+newfilename, engine="xlsxwriter")
        master_original_df.to_excel(writer, sheet_name="Data",index=False)
        master_gacalcu_original_df.to_excel(writer, sheet_name="Old Campaign",index=True)
        workbook = writer.book
        worksheet_data = writer.sheets["Data"]
        worksheet_gacalcu = writer.sheets["Old Campaign"]
        # Add some cell formats.
        format1 = workbook.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_data.set_column(18, 18, None, format1)
        worksheet_data.set_column(25, 25, None, format1)
        worksheet_gacalcu.set_column(1, 1, None, format1)
        worksheet_gacalcu.set_column(5, 5, None, format1)
        worksheet_gacalcu.set_column(8, 8, None, format1)
        writer.save()
        writer.close()
        ## send to dropbox
        print("going for dropbox")
        if os.environ['USEDROPBOX'] == 'yes':
            dropboxdf = master_original_df.copy()
            dropboxdf['Send Date'] = pd.to_datetime(dropboxdf['Send Date']).dt.strftime('%m/%d/%Y')
            main_module.to_dropbox(dropboxdf,dbx_path,client_name)
        ## insert data to bigquery
        col_rename = {
            "Send Date":"Date",
            "Campaign":"Name",
            "By Variant" : "Mailing",
            "Type 2" :"Type_2",
            "Subject" : "Subject_Line",
            "Type 0" :"Type_0",
            "Type 1" :"Type_1",
            "Total Bounces" : "Total_Bounces",
            "Conversions":"Orders",
            "Unsubs.":"Unsub",
            "Google Analytics Campaign Name" :"Google_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Version":"Listrak_Conversion_Analytics_Version",
            "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
            "GA Revenue" : "GA_Revenue",
            "GA Order" : "GA_Orders",
            "GA Sessions" : "GA_Sessions"
        }
        master_original_df.rename(
            columns=col_rename,
            inplace=True
        )
        missing_str_cols = ['Segment_Engagement','Segment_BNB','Segment_Freq','Error','VariantUnused']
        missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','Complaints','Total_Clicks']
        for i in missing_str_cols:
            master_original_df[i] = '-'
        for i in missing_int_cols:
            master_original_df[i] = 0.0
        master_original_df['Segment'] = master_original_df["Listrak_Conversion_Analytics_Version"]
        master_original_df['Campaign'] = master_original_df["Mailing"]
        master_original_df['Original_Segment'] = np.nan
        master_original_df['FISCAL_YEAR'] = pd.to_datetime(master_original_df["Date"]).dt.year
        master_original_df['FISCAL_YEAR_START'] = master_original_df["Date"]
        master_original_df['FISCAL_WEEK'] = pd.to_datetime(master_original_df["Date"]).dt.isocalendar().week
        master_original_df['FISCAL_MONTH'] = pd.to_datetime(master_original_df["Date"]).dt.month
        master_original_df['FISCAL_QUARTER'] = pd.to_datetime(master_original_df["Date"]).dt.quarter
        cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","VariantUnused","Reads","Total_Clicks","Visits","Google_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Module_Name","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK","FISCAL_MONTH","FISCAL_QUARTER"]
        master_original_df = master_original_df[cols_all]
        master_original_df.to_csv(uri+'Masterlist.csv',index=False)
        sourcelistfiles.append('Masterlist.csv')
        # remove previous version of data  and insert new version of data
        main_module.delete_table(client_id)
        bigquery_insert(client_id)
        ### move the file to archive folder
        sourcelistfiles.append(newfilename)
        sourcelistfiles.append(newreportfilename)
        # archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
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
        bigquery.SchemaField('VariantUnused', 'STRING'),
        # bigquery.SchemaField('Type_3', 'STRING'),
        # bigquery.SchemaField('Name_Bkp', 'STRING'),
        # bigquery.SchemaField('ESP', 'STRING'),
        bigquery.SchemaField('Reads', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Visits', 'STRING'),
        # bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        # bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
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
    return "ok"



######### amit sinha ####################
def convert_str_to_num(df,metric_columns):
    df[metric_columns] = df[metric_columns].fillna('0')
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
def concat_mailing_varient(mailing,varient):
    if(isinstance(varient, str) and len(varient) > 0):
        mailing = mailing +"-"+varient
    return mailing

def remove_extra_data(data):
    data = data.split("-")
    if(data[-1] == "VA" or data[-1] == "VB"):
        del data[-1]
    data = '-'.join(data)
    return data
def get_trigger_dates(file_name, position=-1,datatype="String"):
    temp_file_name = file_name.split('_')
    temp_first_date = temp_file_name[position]
    first_date = temp_first_date[4:6] + '/' + temp_first_date[6:8] + '/' + temp_first_date[0:4]
    if datatype == "date":
        first_date = dt.datetime.strptime(first_date, "%m/%d/%Y")
        first_date = first_date.date()
    return(first_date)