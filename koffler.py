import numpy as np
import pandas as pd
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
import datetime as dt

main_module = __import__('main')
client_name = 'Koffler'
dbx_path = '/Client/Koffler Sales/Internal Files/Reporting/Tableau/'
def update_masterlist(uri,client_id,client_esp):
    campaign_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    master_df = pd.DataFrame()
    master_gacalcu_df = pd.DataFrame()
    master_gacalcu_original_df = pd.DataFrame()
    session_df = pd.DataFrame()
    revenue_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame()
    master_original_df = pd.DataFrame()
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    #read the files from google bucket
    list_file = main_module.list_blobs(client_id)
    sourcelistfiles = []
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/koffler/07-06-2023NewFolder/' #local
    # list_file = os.listdir(uri)
    for file in list_file:
        file_name = str(file.name).replace(client_id+'/','')
        # file_name =  file # local
        if 'MessageActivity' in file_name:
            try:
                # df = pd.read_csv(uri + file_name) #remote
                df = pd.read_csv(uri + file_name) # local
                campaign_data_df = campaign_data_df.append(df)
                masterfile_filter_start_date = get_trigger_dates(file_name,-2,"Date")
                masterfile_filter_end_date = get_trigger_dates(file_name,-1,"Date") 
                print("master file",masterfile_filter_start_date ,'===',masterfile_filter_end_date, 'type ',type(masterfile_filter_start_date))
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
                mailingtagging_df = mailingtagging_df[mailingtagging_df["Client"] == "Koffler Sales"]
                mailingtagging_df = mailingtagging_df.drop(columns=["Date","Client"])
                mailingtagging_df.reset_index(inplace=True)
                mailingtagging_df.rename(
                    columns={"Variant":"Variant2","Mailing":"By Mailing"},
                    inplace=True
                )
                # print("mailingtagging_df ",mailingtagging_df.columns)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
            
        elif 'Campaign Analytics' in file_name:
            try:
                campaign_analytics_data_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "TriggerBook" in file_name:
            try:
                trigger_book_df = pd.read_csv(uri + file_name,usecols=["Campaign","Type 0","Type 1","Type 2","Type 3"]) # local
                # trigger_book_df = trigger_book_df.drop(columns = ["Listrak Conversion Analytics Module Name"])

                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "TriggerGA" in file_name:
            try:
                correct_filename = file_name
                correct_filename = correct_filename.replace(" ","")
                correct_filename = correct_filename.replace("-","_")
                correct_filename = correct_filename.replace("_","-",1)
                each_trigger_ga_df = pd.DataFrame()
                trigger_date = get_trigger_dates(correct_filename,-2,"Date")
                #get the trigger start date and end date
                s1 = get_trigger_dates(correct_filename,-2,"Date")
                s2 = get_trigger_dates(correct_filename,-1,"Date")
                if trigger_start_date == "":
                    trigger_start_date = s1
                elif trigger_start_date > s1:
                    trigger_start_date = s1
                ## end date for multiple trigger file
                if trigger_end_date == "":
                    trigger_end_date = s2
                elif trigger_end_date < s2:
                    trigger_end_date = s2
                
                each_trigger_ga_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                each_trigger_ga_df = each_trigger_ga_df.dropna()
                each_trigger_ga_df.rename(
                    columns={
                        "Sessions" :"GA Sessions",
                        "Transactions": "GA Orders",
                        "Revenue" : "GA Revenue",
                        "Campaign": "GA Campaign"
                    },
                    inplace=True
                )
                each_trigger_ga_df["triggerDate"] = trigger_date
                # each_trigger_ga_df.drop(each_trigger_ga_df[each_trigger_ga_df['Campaign'].isnull().values.any()].index, inplace = True)
                trigger_ga_df = trigger_ga_df.append(each_trigger_ga_df)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("trigger ga Error",e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif 'ConversationActivity' in file_name:
            trigger_date = get_trigger_dates(file_name,-2,"Date")
            try:
                # df = pd.read_csv(uri + file_name) #remote
                tdf = pd.read_csv(uri + file_name)
                tdf["Send Date"] = trigger_date
                trigger_data_df = trigger_data_df.append(tdf)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        elif "Koffler Masterfile" in file_name:
            try:
                ## get the file name:
                mstrfilename = file_name
                mstrfilelastdate = mstrfilename.split("_")[-1]
                mstrfilelastdate = mstrfilelastdate.split(".")[0]
                mstrfilelastdateformat = convert_str_to_date(mstrfilelastdate)
                if(mstrfilelastdateformat != masterfile_filter_end_date):
                    with pd.ExcelFile(uri + file_name) as xls:
                        master_df = pd.read_excel(xls, "Data")
                sourcelistfiles.append(file_name)
            except Exception as e:
                print("errrrrr ====>",e)
                # main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
        
    #Operation on files
    matric_column = ["Sent","Delivered","Total Bounce","Unsubs.","Opens","Clicks","Unique Clickers","Reads","Revenue","Conversions","GA Revenue","GA Orders","GA Sessions"]
   
    # find the name of date column It will handle the two different column name like send date utc 04 and send date utc 05
    cam_col_list1 = campaign_data_df.columns
    cam_col_list = []
    for cam_col in cam_col_list1:
        if "Send Date" in cam_col: # "Send Date (UTC4)"
            cam_col_list.append("Send Date")
        else:
            cam_col_list.append(cam_col)
            
    # print(cam_col_list)
    campaign_data_df = campaign_data_df.set_axis(cam_col_list,axis="columns")
    # remove time
    campaign_data_df["Send Date"] = campaign_data_df["Send Date"].apply(lambda x: convert_str_to_date(x))
    # remove extra column (%)
    campaign_data_df = campaign_data_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV'])
    #rename the column
    rename_col ={
        "Bounces" :"Total Bounce",
    }
    campaign_data_df.rename(
        columns=rename_col,
        inplace=True)
    
    #### pull GA Revenue, GA Order, and GA Session data from GA TAB campaign_analytics_data_df Campaign Analytics.csv file
    ga_rename_col ={
        "Campaign" :"ga_campaign",
        "Revenue":"GA Revenue",
        "Transactions": "GA Orders",
        "Sessions" : "GA Sessions"
    }
    campaign_analytics_data_df.rename(
        columns=ga_rename_col,
        inplace=True)
    # remove Winback to another dataframe
    winback_df = campaign_data_df.loc[campaign_data_df["Campaign"].str.contains("Winback")== True,:].copy()
    campaign_data_df = campaign_data_df.loc[campaign_data_df["Campaign"].str.contains("Winback")== False,:].copy()
    if not winback_df.empty:
        winback_df = winback_df[(winback_df["Send Date"] >= trigger_start_date) & (winback_df["Send Date"] <= trigger_end_date)]
    # print(" columns",campaign_analytics_data_df.columns)
    campaign_data_df = pd.merge(campaign_data_df,campaign_analytics_data_df,left_on='Google Analytics Campaign Name',right_on="ga_campaign",how="left")
    campaign_data_df = campaign_data_df.drop(columns=['ga_campaign'])
    # add campaign and mailing column
    campaign_data_df["By Mailing"]=campaign_data_df["Listrak Conversion Analytics Campaign Name"]
    campaign_data_df["filename"] = "messageActivity"
    ## put the value 0 for next duplicate
    campaign_data_df.reset_index(inplace=True, drop=True)
    print("campaign_data_df shape",campaign_data_df.shape)
    ## add split testing data
    if not abTesting_df.empty:
        ab_col_list1 = abTesting_df.columns
        ab_col_list = []
        for cam_col in ab_col_list1:
            if "Send Date" in cam_col:
                ab_col_list.append("Send Date")
            else:
                ab_col_list.append(cam_col)
        abTesting_df = abTesting_df.set_axis(ab_col_list,axis="columns")
        # remove time
        abTesting_df["Send Date"] = abTesting_df["Send Date"].apply(lambda x: convert_str_to_date(x))
        # remove extra column (%)
        abTesting_df = abTesting_df.drop(columns=['%','%.1','%.2','%.3','%.4','CTOR','Conversion Rate','AOV'])
        #rename column of AB Testing
        ab_rename_col = rename_col.copy()
        ab_rename_col["Split Test"] = "Campaign"
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
        ## add A or B in Mailing name
        abTesting_df["By Mailing"]=abTesting_df["Listrak Conversion Analytics Campaign Name"]
        abTesting_df["filename"] = "splitTest"
        #find dupliate in promo tab and remove and paste the data of ab testing to promo tab
        campaign_data_df = pd.concat([campaign_data_df,abTesting_df],ignore_index=True)
        print("inside ab shape2",campaign_data_df.shape)
    else:
        campaign_data_df["Variant"] = ""
    ## remove duplicate. keep last because need to remove row from message activity not from split testing file
    campaign_data_dup_df = campaign_data_df.loc[campaign_data_df.duplicated('Google Analytics Campaign Name', keep=False),:]
    campaign_data_df.drop_duplicates('Google Analytics Campaign Name',inplace=True,keep=False)
    campaign_data_dup_df = campaign_data_dup_df[campaign_data_dup_df["filename"] == "splitTest"]
    campaign_data_df = pd.concat([campaign_data_df,campaign_data_dup_df])
    del campaign_data_dup_df
    campaign_data_df.reset_index(inplace=True, drop=True)
    # remove VA and VB
    campaign_data_df["By Mailing"] = campaign_data_df["By Mailing"].apply(lambda x : remove_extra_data(x))
    campaign_data_df["By Campaign"]=campaign_data_df["By Mailing"].str.split("-", n = 1, expand = True)[1].str.rstrip()
    ## end
    campaign_data_df = campaign_data_df.sort_index(ascending=True)
    # campaign_data_df["ESP"] ="Listrak"
    campaign_data_df["Type 0"] ="Campaign"
    campaign_data_df["Type 1"] ="Promo"
    campaign_data_df["Type 3"] =""
    campaign_data_df.loc[campaign_data_df["Campaign"].str.contains("Evergreen")== True,'Type 1'] = "Evergreen"
    campaign_data_df[["GA Revenue","GA Orders","GA Sessions"]] = campaign_data_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
    #extra fields
    campaign_data_df = pd.merge(campaign_data_df,mailingtagging_df,on="By Mailing",how="left")
    campaign_data_df = campaign_data_df.drop(columns = ["Variant2"])
    campaign_data_df.rename(columns={"Test Type":"Test_Type","Variant":"By Variant"},inplace=True)
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
    trigger_data_df = trigger_data_df.drop(columns = ['%','%.1','%.2','%.3','%.4','Conversion Rate','AOV','Publish Date'])
    trigger_rename_col={
        "Thread / Step": "Campaign",
        "Bounces":"Total Bounce",
        "Opens": "Opens",
        "Clickers" : "Unique Clickers"
    }
    trigger_data_df.rename(
        columns=trigger_rename_col,
        inplace= True
    )
    trigger_data_df["Delivered"] = trigger_data_df["Sent"]
    reorder_col = ['Campaign', 'Subject', "Send Date", 'Sent', 'Delivered', 'Total Bounce',
       'Unsubs.', 'Opens', 'Clicks', 'Revenue', 'Visits', 'Conversions', 'Pass Along',
       'Google Analytics Campaign Name', 'Google Analytics Campaign Content',
       'Listrak Conversion Analytics Campaign Name',
       'Listrak Conversion Analytics Version',
       'Listrak Conversion Analytics Module Name']
    trigger_data_df = trigger_data_df[reorder_col]
    if not winback_df.empty:
        trigger_data_df = pd.concat([trigger_data_df,winback_df])
    ######    
    trigger_data_df_original = trigger_data_df.copy()
    trigger_data_df = pd.merge(trigger_data_df,trigger_ga_df,left_on="Google Analytics Campaign Name",right_on="GA Campaign", how="left")
    trigger_data_df.drop(columns="GA Campaign",inplace=True)
    #remove duplicate row
    # trigger_data_df.drop(trigger_data_df[(trigger_data_df["Send Date"] != trigger_data_df['triggerDate']) & (not trigger_data_df['triggerDate'].isnull().values.any())].index, inplace = True)

    # print(trigger_data_df.loc[:,["Name","Send Date","triggerDate"]])
    trigger_data_df_isnull = trigger_data_df[trigger_data_df["triggerDate"].isnull()]
    trigger_data_df  = trigger_data_df[trigger_data_df["triggerDate"].notnull()]
    trigger_data_df.drop(trigger_data_df[trigger_data_df["Send Date"] != trigger_data_df['triggerDate']].index, inplace = True)
    if not trigger_data_df_isnull.empty:
        trigger_data_df =  pd.concat([trigger_data_df,trigger_data_df_isnull])
        # trigger_data_df.reset_index(inplace=True, drop=True)
        trigger_data_df = trigger_data_df.sort_index(ascending=True)
    ## add missing row from left table
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_df_original])
    trigger_data_df.drop_duplicates(['Campaign','Subject','Google Analytics Campaign Name',"Send Date",'Sent','Revenue','Conversions','Listrak Conversion Analytics Version'],inplace=True,keep="first")
    del trigger_data_df_original
    #reset index
    trigger_data_df.reset_index(inplace=True, drop=True)
    trigger_data_df[["GA Revenue","GA Orders","GA Sessions"]] = trigger_data_df[["GA Revenue","GA Orders","GA Sessions"]].fillna(value=0)
    trigger_data_dup_df = trigger_data_df.loc[trigger_data_df.duplicated(['Google Analytics Campaign Name',"Send Date"], keep="first"),:]
    print("pass 2")
    trigger_data_df.drop_duplicates(['Google Analytics Campaign Name',"Send Date"],inplace=True,keep="first")
    # FIll the value 0 of GA session, GA, order, GA revenue for duplicate row
    trigger_data_dup_df.loc[:,["GA Revenue","GA Orders","GA Sessions"]] = [0,0,0]
    trigger_data_df = pd.concat([trigger_data_df,trigger_data_dup_df])
    trigger_data_df = trigger_data_df.sort_index(ascending=True)
    # trigger_data_df.to_csv(uri + '/mytriggerd667.csv',index=False)
    trigger_data_df =  pd.merge(trigger_data_df,trigger_book_df, on="Campaign", how="left")
    trigger_data_df.drop(columns=["triggerDate"],inplace=True)
    trigger_data_df["By Campaign"]=""
    trigger_data_df["By Mailing"]=""
    trigger_data_df["Test_Type"]=""
    trigger_data_df["Offer"]=""
    trigger_data_df["By Variant"]=""
    trigger_data_df["Type 2"] =""
    # remove row having sent value 0
    # trigger_data_df = trigger_data_df[trigger_data_df["Sent"] > 0]
    trigger_data_df = convert_str_to_num(trigger_data_df,matric_column)
    # trigger_data_df.to_csv(uri+"mytrigger.csv",index=False)
    excelcol = ["Send Date","Campaign","Subject","By Campaign","By Mailing","By Variant","Type 0","Type 1","Type 2","Offer","Test_Type","Type 3",
                "Sent","Delivered","Total Bounce","Unsubs.","Opens","Reads","Clicks","Unique Clickers","Revenue","Visits","Conversions","Pass Along",
                "Google Analytics Campaign Name","Google Analytics Campaign Content","Listrak Conversion Analytics Campaign Name","Listrak Conversion Analytics Version",
                "Listrak Conversion Analytics Module Name","GA Revenue","GA Orders","GA Sessions"]
    campaign_data_df = campaign_data_df[excelcol]
    trigger_data_df = trigger_data_df[excelcol]
    print("pass 3")
    # export to excel
    newreportfilename = ""
    if not campaign_data_df.empty:
        newreportfilename = 'KofflerSales_Book_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
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
    print("pass 4")
    ###### master file ######
    if not master_df.empty:
        print("master file shape 1",master_df.shape)
        master_df["Send Date"] = master_df["Send Date"].apply(lambda x: convert_str_to_date(x))
        master_original_df = master_df.copy()
        master_df = master_df[(master_df["Send Date"] >= masterfile_filter_start_date) & (master_df["Send Date"] <= masterfile_filter_end_date)]
        master_original_df.drop(master_original_df[(master_original_df["Send Date"] >= masterfile_filter_start_date) & (master_original_df["Send Date"] <= masterfile_filter_end_date)].index, inplace = True)

        master_df.drop(master_df[master_df["Type 0"] == "Campaign"].index, inplace = True)
        # master_df.drop(master_df[(master_df["Type 0"] == "Campaign" ) & (master_df["Type 1"] == "Evergreen")].index, inplace = True)
        #convert date type from string to date
        # campaign_data_df["Send Date"] = campaign_data_df["Send Date"].apply(lambda x: convert_str_to_date(x,'%m/%d/%Y'))
        # trigger_data_df["Send Date"] = trigger_data_df["Send Date"].apply(lambda x: convert_str_to_date(x,'%m/%d/%Y'))
        # paste the data from promo tab and trigger tab to master file
        master_df = pd.concat([master_df,campaign_data_df])
        master_df = pd.concat([master_df,trigger_data_df])
        
        # add seven row in ga calcu sheet
        #typecast the state date and end date to string
        master_original_df = pd.concat([master_original_df,master_df])
        # master_original_df.reset_index(inplace=True,drop=True)
        newfilename = 'koffler-Koffler Masterfile_'+str(masterfile_filter_start_date)+"_"+str(masterfile_filter_end_date)+".xlsx"
        writer = pd.ExcelWriter(uri+newfilename, engine="xlsxwriter")
        master_original_df.to_excel(writer, sheet_name="Data",index=False)
        workbook = writer.book
        worksheet_data = writer.sheets["Data"]
        # Add some cell formats.
        format1 = workbook.add_format({"num_format": "$#,##0.00"})
        # Set the column width and format.
        worksheet_data.set_column(20, 20, None, format1)
        worksheet_data.set_column(29, 29, None, format1)
        writer.save()
        writer.close()
        ## send to dropbox
        print("going for dropbox")
        if os.environ['USEDROPBOX'] == 'yes':
            dropboxdf = master_original_df.copy()
            dropboxdf["Send Date"] = pd.to_datetime(dropboxdf["Send Date"]).dt.strftime('%m/%d/%Y')
            main_module.to_dropbox(dropboxdf,dbx_path,"new "+client_name)
        
        ## insert data to bigquery
        col_rename = {
            "Send Date":"Date",
            "Campaign":"Name",
            "By Campaign":"Campaign",
            "By Mailing":"Mailing",
            "By Variant": "Variant",
            "Pass Along":"Pass_Along",
            "Type 2" :"Type_2",
            "Subject" : "Subject_Line",
            "Type 0" :"Type_0",
            "Type 1" :"Type_1",
            "Type 3" : "Type_3",
            "Unsubs." : "Unsub",
            "Conversions" : "Orders",
            "Total Bounce" : "Total_Bounces",
            "Google Analytics Campaign Name" :"Google_Analytics_Campaign_Name",
            "Google Analytics Campaign Content" : "Google_Analytics_Campaign_Content",
            "Listrak Conversion Analytics Campaign Name" : "Listrak_Conversion_Analytics_Campaign_Name",
            "Listrak Conversion Analytics Version" : "Listrak_Conversion_Analytics_Version",
            "Listrak Conversion Analytics Module Name" : "Listrak_Conversion_Analytics_Module_Name",
            "GA Revenue" : "GA_Revenue",
            "GA Orders" : "GA_Orders",
            "GA Sessions" : "GA_Sessions"
        }
        master_original_df.rename(
            columns=col_rename,
            inplace=True
        )
        print("going to create masterfile for bigquery")
        missing_str_cols = ['Total_Clicks','Unnamed_colon__32','Segment','Segment_Engagement','Segment_BNB','Segment_Freq','Error']
        missing_int_cols =['AA_Visits','AA_Revenue','AA_SFCC_Demand','AA_Orders','AA_Mobile_Visits','Units','Product_Margin','Total_Margin','Hard_Bounces','Soft_Bounces','Complaints']
        for i in missing_str_cols:
            master_original_df[i] = '-'
        for i in missing_int_cols:
            master_original_df[i] = 0.0
        master_original_df['Original_Segment'] = master_original_df['Segment']
        master_original_df['FISCAL_YEAR'] = pd.to_datetime(master_original_df["Date"]).dt.year
        master_original_df['FISCAL_YEAR_START'] = master_original_df["Date"]
        master_original_df['FISCAL_WEEK'] = pd.to_datetime(master_original_df["Date"]).dt.isocalendar().week
        master_original_df['FISCAL_MONTH'] = pd.to_datetime(master_original_df["Date"]).dt.month
        master_original_df['FISCAL_QUARTER'] = pd.to_datetime(master_original_df["Date"]).dt.quarter
        cols_all = ["Date","Name","Campaign","Mailing","Variant","Subject_Line","Segment","Type_0","Type_1","Type_2","Offer","Segment_Engagement","Segment_BNB","Segment_Freq","Test_Type","Sent","Delivered","Opens","Clicks","Revenue","Orders","GA_Revenue","GA_Orders","GA_Sessions","AA_Visits","AA_Revenue","AA_SFCC_Demand","AA_Orders","AA_Mobile_Visits","Units","Product_Margin","Total_Margin","Unsub","Complaints","Hard_Bounces","Soft_Bounces","Total_Bounces","Type_3","Reads","Visits","Pass_Along","Total_Clicks","Google_Analytics_Campaign_Name","Google_Analytics_Campaign_Content","Listrak_Conversion_Analytics_Campaign_Name","Listrak_Conversion_Analytics_Version","Listrak_Conversion_Analytics_Module_Name","Unnamed_colon__32","Error","Original_Segment","FISCAL_YEAR","FISCAL_YEAR_START","FISCAL_WEEK","FISCAL_MONTH","FISCAL_QUARTER"]
        master_original_df = master_original_df[cols_all]
        print("going to create masterfile final")
        master_original_df.to_csv(uri+'Masterlist.csv',index=False)
        print("masterfile created")
        sourcelistfiles.append('Masterlist.csv')
        # remove previous version of data  and insert new version of data
        main_module.delete_table(client_id)
        print("database table data removed")
        bigquery_insert(client_id)
        print("insertion is going on")
        ### move the file to archive folder
        sourcelistfiles.append(newfilename)
        sourcelistfiles.append(newreportfilename)
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
        bigquery.SchemaField("Date", "Date"),
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
        bigquery.SchemaField('Type_3', 'STRING'),
        # bigquery.SchemaField('Name_Bkp', 'STRING'),
        # bigquery.SchemaField('ESP', 'STRING'),
        bigquery.SchemaField('Reads', 'STRING'),
        bigquery.SchemaField('Total_Clicks', 'STRING'),
        bigquery.SchemaField('Visits', 'STRING'),
        bigquery.SchemaField('Pass_Along', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Google_Analytics_Campaign_Content', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Campaign_Name', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Version', 'STRING'),
        bigquery.SchemaField('Listrak_Conversion_Analytics_Module_Name', 'STRING'),
        bigquery.SchemaField('Unnamed_colon__32', 'STRING'),
        bigquery.SchemaField('Error', 'STRING'),
        bigquery.SchemaField('Original_Segment', 'STRING'),
        bigquery.SchemaField('FISCAL_YEAR', 'INTEGER'),
        bigquery.SchemaField('FISCAL_YEAR_START', "Date"),
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
                elif (int(newformat[0]) <= 12):
                    format = "%d-%m-%Y"
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
    if datatype == "Date":
        first_date = dt.datetime.strptime(first_date, "%m/%d/%Y")
        first_date = first_date.date()
    return(first_date)

def remove_extra_data(data):
    mydata = data[-2] + data[-1]
    if(mydata == "VA" or mydata== "VB"):
        strlen = len(data)
        data = data[:strlen-2]
    return data