import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
# import datetime as dt

main_module = __import__('main')
client_name = 'Home Warranty of America'
dbx_path = '/Client/Home Warranty of America/Internal Files/Reporting/'

def update_masterlist(uri,client_id,client_esp):
    esp_module = __import__(client_esp)
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    campaign_analytics_data_rev_df = pd.DataFrame()
    campaign_analytics_data_ses_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
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
    # Read All files
    # Reading Campaign and Trigger Files
    ## check zip file
    # print("start zip check---------------")
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    # main_module.checkZipFile(client_id,'home_warranty_of_america-zipfile.zip',archive_datetime)
    print("start reading files----------------")
    # list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    uri = '/home/nav93/Downloads/AlchemyWroxFiles/home_wrranty_america/2023_0730-0805/' #local
    list_file = os.listdir(uri)
    sourcelistfiles = []
    for file in list_file:
        # file_name = str(file.name).replace(client_id+'/','') # REMOTE DEPLOY
        file_name =  file # local
        if 'MessageActivity' in file_name:
            try:
                df = pd.read_csv(uri + file_name) # local
                campaign_data_df = campaign_data_df.append(df)
                masterfile_filter_start_date = esp_module.get_trigger_dates(file_name,-2,"date")
                masterfile_filter_end_date = esp_module.get_trigger_dates(file_name,-1,"date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
                sourcelistfiles.append(file_name) 
            except Exception as e:
                print("MessageActivity Error",e)
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
        elif "Triggerbook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name)
                trigger_book_df.rename(columns={"Trigger":"Name", "Type 2": "Type 1"}, inplace=True)
                sourcelistfiles.append(file_name)
            except:
                try:
                    with pd.ExcelFile(uri + file_name) as xls:
                        trigger_book_df = pd.read_excel(xls, "Triggerbook")
                        trigger_book_df.rename(columns={"Trigger":"Name", "Type 2": "Type 1"}, inplace=True)
                        sourcelistfiles.append(file_name)
                except Exception as e:
                    print("Triggerbook Error",e)
                    main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                    return "error"

        elif "TriggerGArev" in file_name:
            print("TriggerGArev")
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
            print("TriggerGAses")
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
                tdf = pd.read_csv(uri + file_name)
                tdf['Date'] = trigger_date
                trigger_data_df = trigger_data_df.append(tdf)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("ConversationActivity Error",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "Home Warranty of America Masterfile" in file_name:
            try:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = esp_module.convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        master_df = pd.read_excel(xls, "Data")
                        try:
                            master_gacalcu_df = pd.read_excel(xls, "Old Campaign",header=[0, 1])
                        except:
                            pass
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
    matric_column = ["Sent","Delivered","Total Bounces","Unsub","Opens","Reads","Total Clicks","Clicks","Revenue","Visits","Orders"]
    ## merge the trigger google analytics data
    print("pass 1")
    if not (trigger_ga_rev_df.empty or trigger_ga_ses_df.empty):
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
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV'])
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
    if not (campaign_analytics_data_rev_df.empty or campaign_analytics_data_ses_df.empty):
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
    campaign_data_df.reset_index(inplace=True, drop=True)
    campaign_data_df["Campaign"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"].str.split("-", n = 1, expand = True)[1].str.rstrip()
    campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df["Type 3"] = ""
    campaign_data_df["Type 2"] = "Email"
    campaign_data_df["Listrak Conversion Analytics Version"] = campaign_data_df["Segment"]
    campaign_data_df["Google Analytics Campaign Name"]=""
    print("Pass 2")
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
    trigger_data_df["Delivered"] = trigger_data_df["Sent"]
    #pull the value of GA order, GA Session, GA Revenue
    print("trigger operation===========>")
    if not trigger_ga_df.empty:
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
    # # temporarily setting to "Post Purchase" because trigger_book_df data is not in the folder.
    # trigger_data_df["Type 1"]="Post Purchase"

    trigger_book_df.drop_duplicates(["Name"],inplace=True,keep="last")
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on="Name", how="left") ## made changes
    
    trigger_data_df["Campaign"]=""
    trigger_data_df["Mailing"]=""
    # trigger_data_df["Test Type"]=""
    trigger_data_df["Variant"]=""
    # trigger_data_df["Offer"]=""
    trigger_data_df["ESP"]="Listrak"
    trigger_data_df["Type 0"]="Trigger"
    trigger_data_df["Segment"] =""
    trigger_data_df["Listrak Conversion Analytics Module Name"] =""
    trigger_data_df["Type 3"] = ""
    trigger_data_df["Type 2"] = "Email"
    trigger_data_df["Listrak Conversion Analytics Version"] = trigger_data_df["Segment"]
    trigger_data_df["Google Analytics Campaign Name"]=""
    trigger_data_df = esp_module.convert_str_to_num(trigger_data_df,matric_column)
    
    excelcol = ["Name","Campaign","Mailing","Variant","Subject Line","Date","ESP","Type 0","Type 1","Type 2","Sent","Orders",
                    "Revenue","Opens","Reads","Clicks","Total Clicks","Unsub","Visits","Delivered","Total Bounces","Google Analytics Campaign Name",
                    "Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version","Listrak Conversion Analytics Module Name"]
    #re-arrange the columns
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    print("Pass 3")
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'home_warranty_of_america book' + str(masterfile_filter_start_date) + "_" + str(masterfile_filter_end_date) + ".xlsx"
        try:
            writer1 = pd.ExcelWriter(uri + "/archive/" + newreportfilename, engine="xlsxwriter")
            campaign_data_df.to_excel(writer1, sheet_name="Promo", index=False)
            trigger_data_df.to_excel(writer1, sheet_name="Trigger", index=False)
            workbook1 = writer1.book
            worksheet_promo = writer1.sheets["Promo"]
            worksheet_trigger = writer1.sheets["Trigger"]
            # Add some cell formats.
            format1 = workbook1.add_format({"num_format": "$#,##0.00"})
            # Set the column width and format.
            worksheet_promo.set_column(15, 15, None, format1)
            # worksheet_promo.set_column(25, 25, None, format1)
            worksheet_trigger.set_column(15, 15, None, format1)
            # worksheet_trigger.set_column(25, 25, None, format1)
            writer1.save()
            writer1.close()
        except Exception as e:
            print(f"Error: {e}")
    ###### master file ######
    if not master_df.empty:
        master_df["Date"] = master_df["Date"].apply(lambda x: esp_module.convert_str_to_date(x))
        master_original_df = master_df.copy()
        master_df = master_df[(master_df["Date"] >= masterfile_filter_start_date) & (master_df["Date"] <= masterfile_filter_end_date)]
        master_original_df.drop(master_original_df[(master_original_df['Date'] >= masterfile_filter_start_date) & (master_original_df['Date'] <= masterfile_filter_end_date)].index, inplace = True)
        ## calcu tab
        if not master_gacalcu_df.empty:
            master_gacalcu_df[("Final","Date")] = master_gacalcu_df[("Final","Date")].apply(lambda x: esp_module.convert_str_to_date(x))
            master_gacalcu_original_df = master_gacalcu_df.copy()
            master_gacalcu_df = master_gacalcu_df[(master_gacalcu_df["Final"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_df["Final"]["Date"] <= masterfile_filter_end_date)]
            master_gacalcu_original_df.drop(master_gacalcu_original_df[(master_gacalcu_original_df["Final"]["Date"] >= masterfile_filter_start_date) & (master_gacalcu_original_df["Final"]["Date"] <= masterfile_filter_end_date)].index, inplace = True)
        ## filter with type 0 = Campaign and Type 1 = Promo
        master_df.drop(master_df[(master_df["Type 0"] == "Campaign" ) & (master_df["Type 1"] == "Promo")].index, inplace = True)
        master_df.drop(master_df[(master_df["Type 1"] == "Old Campaign") | (master_df["Type 1"] == "Old Campaign") ].index, inplace = True)
        # paste the data from promo tab, workflow tab, and trigger tab to master file
        master_df = pd.concat([master_df,campaign_data_df])
        master_df = pd.concat([master_df,trigger_data_df])

    old_campaign_data = []
    old_campaign_final_data_df = []

    if not master_gacalcu_df.empty:
        print('master_gacalcu_df is present...................>>>>>>>>')
        mydata = []
        delta = masterfile_filter_end_date - masterfile_filter_start_date
        noofdays = delta.days
        print("no of days",noofdays)
        # for x in range(noofdays+1):
            # newsdate =  trigger_start_date + timedelta(days=x)
            # newsdate =  masterfile_filter_start_date + timedelta(days=x)
            # each_date = newsdate.strftime("%m/%d/%Y")
            #sum if date wise and name not equal to old campaign
            # old_campaign_data.append(["Old Campaign",np.nan,np.nan,np.nan,np.nan,newsdate,np.nan,'Campaign','Old Campaign',np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,0,0,0,np.nan,np.nan,np.nan])
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
        #convert str to number
        master_gacalcu_df = esp_module.convert_str_to_num(master_gacalcu_df,["GA|Revenue","GA|Order","GA|Sessions","ESP|Revenue","ESP|Order","ESP|Sessions","GASMS|Revenue","GASMS|Order","GASMS|Sessions","ESPSMS|Revenue","ESPSMS|Order","ESPSMS|Sessions"])
        master_gacalcu_df["Final|Revenue"] = master_gacalcu_df["GA|Revenue"] - master_gacalcu_df["ESP|Revenue"]
        master_gacalcu_df["Final|Order"] = master_gacalcu_df["GA|Order"] - master_gacalcu_df["ESP|Order"]
        master_gacalcu_df["Final|Sessions"] = master_gacalcu_df["GA|Sessions"] - master_gacalcu_df["ESP|Sessions"]
        print("pass 6")
        master_gacalcu_original_df = pd.concat([master_gacalcu_original_df, master_gacalcu_df])
        master_gacalcu_original_df = master_gacalcu_original_df[master_gacalcu_original_df[("Final","Date")].notnull()]
        master_gacalcu_original_df[("Final","Date")] = master_gacalcu_original_df[("Final","Date")].apply(lambda x: x.strftime("%m/%d/%Y"))
        master_gacalcu_original_df.reset_index(inplace=True,drop=True)
    ### add final revenue, order and session to old campaign in master file data tab
    ### add final revenue, order and session to old campaign in master file data tab
    if not master_df.empty:
        #update into master file
        master_original_df = pd.concat([master_original_df,master_df])
        newfilename = 'Home Warranty of America Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer = pd.ExcelWriter(uri+newfilename, engine="xlsxwriter")
        master_original_df.to_excel(writer, sheet_name="Data",index=False)
        master_gacalcu_original_df.to_excel(writer, sheet_name="Old Campaign",index=True)
        workbook = writer.book
        worksheet_data = writer.sheets["Data"]
        worksheet_gacalcu = writer.sheets["Old Campaign"]
        # Add some cell formats.
        format1 = workbook.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_data.set_column(14, 14, None, format1)
        # worksheet_gacalcu.set_column(1, 1, None, format1)
        # worksheet_gacalcu.set_column(4, 4, None, format1)
        # worksheet_gacalcu.set_column(8, 8, None, format1)
        # worksheet_gacalcu.set_column(11, 11, None, format1)
        # worksheet_gacalcu.set_column(14, 14, None, format1)
        # worksheet_gacalcu.set_column(18, 18, None, format1)
        writer.save()
        writer.close()
        ## send to dropbox
        # print("going for dropbox")
        # if os.environ['USEDROPBOX'] == 'yes':
        #     dropboxdf = master_original_df.copy()
        #     dropboxdf['Date'] = pd.to_datetime(dropboxdf['Date']).dt.strftime('%m/%d/%Y')
        #     main_module.to_dropbox(dropboxdf,dbx_path,client_name)

        ## insert data to bigquery
        col_rename = {
            "Type 2" :"Type_2",
            "Subject Line" : "Subject_Line",
            "Type 0" :"Type_0",
            "Type 1" :"Type_1",
            "Total Bounces" : "Total_Bounces",
            "Total Opens":"Total_Opens", 
            "Total Clicks":"Total_Clicks",
            "Campaign ID":"Campaign_ID",
            "Google Analytics Campaign Name" :"Google_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Version" : "Listrak_Conversion_Analytics_Version",
            "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
        }
        master_original_df.rename(
            columns=col_rename,
            inplace=True
        )
        missing_str_cols = ["Offer","Test_Type",'Segment_Engagement','Segment_BNB','Segment_Freq','Error']
        missing_int_cols =['GA_Revenue','GA_Orders','GA_Sessions','AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','Complaints','Send_Weekday']
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
        cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer",
                    "Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type",
                    "Sent","Delivered","Opens","Clicks","Revenue","Orders",
                    "GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","ESP","Send_Weekday","Total_Opens","Reads","Total_Clicks","Visits","Campaign_ID","Google_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK","FISCAL_MONTH","FISCAL_QUARTER"]
        master_original_df = master_original_df[cols_all]
        master_original_df.to_csv(uri+'Masterlist.csv',index=False)
       # remove previous version of data  and insert new version of data
        # main_module.delete_table(client_id)
        # bigquery_insert(client_id) #remote
        ### move the file to archive folder
        sourcelistfiles.append(newfilename)
        sourcelistfiles.append(newreportfilename)
        sourcelistfiles.append('Masterlist.csv')
        print('happy ending')
        # for sfile in sourcelistfiles:
        #     main_module.copy_blob('reporter-etl',client_id + '/' + sfile,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +'/'+ sfile)
        #     if newfilename != sfile:
        #         main_module.delete_blob('reporter-etl',client_id + '/' + sfile)
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
        bigquery.SchemaField('Send_Weekday', 'STRING'),
        bigquery.SchemaField('Total_Opens', 'STRING'),
        bigquery.SchemaField('Reads', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Visits', 'STRING'),
        bigquery.SchemaField('Campaign_ID', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
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

