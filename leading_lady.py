import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime
from google.cloud import storage
import datetime as dt

from bs4 import BeautifulSoup
from requests.models import Response
from requests_html import HTMLSession
import pandas as pd
from datetime import datetime

main_module = __import__('main')
client_name = 'Ashnyc'
dbx_path = '/Client/Ash NYC/Internal Files/History/Ash/'

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
    }
login_data = {
    'utf8': '✓',
    'admin_user[email]':'teamaccess@alchemyworx.com',
    'admin_user[password]':'Welcome1234567&',
    'commit': 'Sign in'
}

accounts = {
    "hold_peter_&_paul":"gk995g",
    "the_dean_hotel":"g9kmmg",
    "the_siren_hotel":"g3m88g",
    
}
#"Alexander_Inn":"gb57kg",


def update_masterlist(uri,client_id,client_esp):
    
    s = HTMLSession()
    url = 'https://guestfolio.net/admin/login/'
    r = s.get(url, headers=headers)
    soup = BeautifulSoup(r.content, 'html5lib')
    login_data['authenticity_token'] = soup.find('input', attrs={'name': 'authenticity_token'})['value']
    r = s.post(url, data=login_data, headers=headers)
    print(r.html)

    for key in accounts:
        account_select = s.get('https://guestfolio.net/admin?utf8=%E2%9C%93&account_id='+ accounts[key] +'#fndtn-latest-activity')
        pages = s.get('https://guestfolio.net/admin/newsletters/campaigns')
        page_string = pages.html.find('.last', first=True)
        page = int(str(page_string.find('a')[0]).split('=')[2][:1]) + 1
        
        main_df = pd.DataFrame({'send_date':[],'name':[],'subject_line':[], 'segment':[], 'sent':[],'opens':[],'clicks':[],'bounce':[],'unsub':[],'spam':[],'failed':[],'revenue':[],'orders':[]})
        p = 1
        #for p in range(1, page):
        main_page = s.get('https://guestfolio.net/admin/newsletters/campaigns?page='+str(p))
        campaign_list = main_page.html.find('.newsletter_campaign')
        #print(datetime.strptime('Jun 1 2005', '%b %d %Y').date())
        print("page:"+str(p))
        for campaign in campaign_list:
            campaign_info = campaign.find('.name', first=True)
            campaign_name = campaign_info.text
            segment = campaign.find('.list', first=True).text
            send_date = campaign.find('time', first=True).text
            campaign_id = str(campaign.find('a', first=True).attrs).split('/')[4][:-2]
            subject_get = s.get('https://guestfolio.net/admin/newsletters/campaigns/'+ campaign_id)
            subject_line = subject_get.html.find('.field')[5].text
            campaign_metric = s.get('https://guestfolio.net/admin/newsletters/campaigns/'+ campaign_id + '/reports')
            metrics_1 = campaign_metric.html.find('.inline-list')[0]
            sent = metrics_1.find('dd')[0].text
            opens = metrics_1.find('dd')[2].text
            clicks = metrics_1.find('dd')[3].text
            revenue = metrics_1.find('dd')[1].text
            metrics_2 = campaign_metric.html.find('.inline-list')[1]
            bounce = metrics_2.find('dd')[0].text
            unsub = metrics_2.find('dd')[1].text
            failed = metrics_2.find('dd')[2].text
            spam = metrics_2.find('dd')[3].text

            orders = 0
            if revenue != '$0.00':
                #print("i made it here")
                campaign_metric_orders = s.get('https://guestfolio.net/admin/newsletters/campaigns/'+ campaign_id + '/reports/conversions')
                order_list = campaign_metric_orders.html.find('.display_name')
                orders = len(order_list)

            each_record = pd.DataFrame({'send_date':[send_date],'name':[campaign_name],'subject_line':[subject_line], 'segment':[segment], 'sent':[sent],'opens':[opens],'clicks':[clicks],'bounce':[bounce],'unsub':[unsub],'spam':[spam],'failed':[failed],'revenue':[revenue],'orders':[orders]})
            main_df = main_df.append(each_record,ignore_index = True)

            print(campaign_name)
                
        main_df['sent'] = main_df['sent'].str.split(" ", expand=True)[0]
        main_df['opens'] = main_df['opens'].str.split(" ", expand=True)[0]
        main_df['clicks'] = main_df['clicks'].str.split(" ", expand=True)[0]
        main_df['bounce'] = main_df['bounce'].str.split('\\n', expand=True)[0]
        main_df['unsub'] = main_df['unsub'].str.split(" ", expand=True)[0]
        main_df['spam'] = main_df['spam'].str.split(" ", expand=True)[0]
        main_df['failed'] = main_df['failed'].str.split(" ", expand=True)[0]
        print(main_df)
        main_module.to_ash_nyc(main_df,dbx_path,key + '-campaign')
        #main_df.to_csv(key +'.csv', index=False)
    first_week(uri,client_id)
    try:
        second_week(uri,client_id)
    except:
        print('no second week')
    return('Completed')


def first_week(uri,client_id):
    ash_settings = pd.read_csv(uri + client_id +'-ash-settings.csv')
    ash_settings['First Date'] = pd.to_datetime(ash_settings['First Date']).dt.strftime('%Y-%m-%d')
    ash_settings['Last Date'] = pd.to_datetime(ash_settings['Last Date']).dt.strftime('%Y-%m-%d')
    
    s = HTMLSession()
    url = 'https://guestfolio.net/admin/login/'
    r = s.get(url, headers=headers)
    soup = BeautifulSoup(r.content, 'html5lib')
    login_data['authenticity_token'] = soup.find('input', attrs={'name': 'authenticity_token'})['value']
    r = s.post(url, data=login_data, headers=headers)
    print(r.html)

    #Tobe set
    first_date = str(ash_settings['First Date'].values[0])
    last_date = str(ash_settings['Last Date'].values[0])

    for key in accounts:
        account_select = s.get('https://guestfolio.net/admin?utf8=%E2%9C%93&account_id='+ accounts[key] +'#fndtn-latest-activity')
        trigger_page = s.get('https://guestfolio.net/admin/reports/email-messages?report%5Bfrom%5D='+ first_date +'&amp;report%5Bto%5D='+ last_date)
        subjects = trigger_page.html.find('.subject')
        print(len(subjects))
        main_df = pd.DataFrame({'send_date':[],'subject_line':[], 'sent':[],'delivered':[],'opens':[],'clicks':[]})
        for i in range(1,len(subjects)):
            subject_line = subjects[i].text
            sent = trigger_page.html.find('.sent_count')[i].text
            delivered = trigger_page.html.find('.delivered_count')[i].text
            opens = trigger_page.html.find('.opened_count')[i].text
            clicks = trigger_page.html.find('.clicked_count')[i].text
            print(sent)

            each_record = pd.DataFrame({'send_date':[first_date],'subject_line':[subject_line], 'sent':[sent],'delivered':[delivered],'opens':[opens],'clicks':[clicks],})
            main_df = main_df.append(each_record,ignore_index = True)

        main_module.to_ash_nyc(main_df,dbx_path,key + '-trigger-' + first_date)
        #main_df.to_csv(key +'-Trigger.csv', index=False)

def second_week(uri,client_id):
    ash_settings = pd.read_csv(uri + client_id +'-ash-settings.csv')
    ash_settings['First Date'] = pd.to_datetime(ash_settings['First Date']).dt.strftime('%Y-%m-%d')
    ash_settings['Last Date'] = pd.to_datetime(ash_settings['Last Date']).dt.strftime('%Y-%m-%d')
    
    s = HTMLSession()
    url = 'https://guestfolio.net/admin/login/'
    r = s.get(url, headers=headers)
    soup = BeautifulSoup(r.content, 'html5lib')
    login_data['authenticity_token'] = soup.find('input', attrs={'name': 'authenticity_token'})['value']
    r = s.post(url, data=login_data, headers=headers)
    print(r.html)

    #Tobe set
    first_date = str(ash_settings['First Date'].values[1])
    last_date = str(ash_settings['Last Date'].values[1])

    for key in accounts:
        account_select = s.get('https://guestfolio.net/admin?utf8=%E2%9C%93&account_id='+ accounts[key] +'#fndtn-latest-activity')
        trigger_page = s.get('https://guestfolio.net/admin/reports/email-messages?report%5Bfrom%5D='+ first_date +'&amp;report%5Bto%5D='+ last_date)
        subjects = trigger_page.html.find('.subject')
        print(len(subjects))
        main_df = pd.DataFrame({'send_date':[],'subject_line':[], 'sent':[],'delivered':[],'opens':[],'clicks':[]})
        for i in range(1,len(subjects)):
            subject_line = subjects[i].text
            sent = trigger_page.html.find('.sent_count')[i].text
            delivered = trigger_page.html.find('.delivered_count')[i].text
            opens = trigger_page.html.find('.opened_count')[i].text
            clicks = trigger_page.html.find('.clicked_count')[i].text
            print(sent)

            each_record = pd.DataFrame({'send_date':[first_date],'subject_line':[subject_line], 'sent':[sent],'delivered':[delivered],'opens':[opens],'clicks':[clicks],})
            main_df = main_df.append(each_record,ignore_index = True)

        main_module.to_ash_nyc(main_df,dbx_path,key + '-trigger-' + first_date)