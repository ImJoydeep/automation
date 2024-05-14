import numpy as np
import pandas as pd
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import datetime as dt
import zipfile
main_module = __import__('main')
client_name = "Mattress Warehouse"
dbx_path = '/Client/Mattress Warehouse/Internal Files/Reporting/Tableau/'

def update_masterlist(uri,client_id,client_esp):
    campaign_data_df = pd.DataFrame()
    campaign_analytics_data_df = pd.DataFrame()
    trigger_data_df = pd.DataFrame()
    abTesting_df = pd.DataFrame()
    mailingtagging_df = pd.DataFrame()
    trigger_book_df = pd.DataFrame()
    master_df = pd.DataFrame()
    master_gacalcu_df = pd.DataFrame()
    master_gacalcu_original_df = pd.DataFrame()
    session_df = pd.DataFrame()
    revenue_df = pd.DataFrame()
    trigger_ga_df = pd.DataFrame() #Analytics Raw Data Campaigns 20221023-20221029
    master_original_df = pd.DataFrame()
    #######
    masterfile_filter_start_date=""
    masterfile_filter_end_date=""
    trigger_start_date=""
    trigger_end_date=""
    dt_start = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
    archive_datetime = datetime.now().strftime("%d-%m-%Y %I:%M:%S")
    # Read All files
    # Reading Campaign and Trigger Files
    ## check zip file
    print("start zip check")
    main_module.checkZipFile(client_id,'mattress_warehous-zipfile.zip',archive_datetime)
    ##read files
    print("start download files")
    list_file = main_module.list_blobs(client_id) # REMOTE DEPLOY
    # uri = '/home/nav93/Downloads/AlchemyWroxFiles/cookieskids/2023_0528-0603/' #local
    # list_file = os.listdir(uri)
    sourcelistfiles = []
    for file in list_file:
        file_name = str(file.name).replace(client_id+'/','') # REMOTE DEPLOY
        print("final file name ",file_name)
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
        elif 'Campaign Analytics' in file_name:
            try:
                campaign_analytics_data_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
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

        elif "TriggerGA" in file_name:
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
                
                each_trigger_ga_df = pd.read_csv(uri + file_name,skiprows=6,usecols=["Campaign","Sessions","Transactions","Revenue"]) # local
                each_trigger_ga_df = each_trigger_ga_df.dropna()
                each_trigger_ga_df.rename(
                    columns={
                        "Sessions" :"GA Session",
                        "Transactions": "GA Order",
                        "Revenue" : "GA Revenue",
                        "Campaign":"TriggerCampaign"
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
        
        elif "Mattress Warehouse Masterfile" in file_name:
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
                session_df = pd.read_csv(uri + file_name,skiprows=10)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"

        elif "rev.csv" in file_name:
            try:
                revenue_df = pd.read_csv(uri + file_name,skiprows=10)
                sourcelistfiles.append(file_name)
            except Exception as e:
                print(e)
                main_module.logs(dt_start,file_name, '0 rows', '0 rows', 'failed: '+str(e),client_id,client_esp)
                return "error"
    #Operation on files
    
    ### move the file to archive folder
    print(sourcelistfiles)
    
    for sfile in sourcelistfiles:
        main_module.copy_blob('reporter-etl',client_id + '/' + sfile,'reporter-etl',client_id + '/' +'archive/' + archive_datetime +'/'+ sfile)
        # if newfilename != sfile:
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



######### amit sinha ####################
def convert_str_to_num(df,metric_columns):
    df[metric_columns].fillna('0', inplace=True)
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