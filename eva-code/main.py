#Глобальные настройки

import pandas as pd
from pandas.io.json import json_normalize
import json
import requests
import numpy as np
from datetime import datetime, timedelta
import requests 
from time import sleep
import time

DBG = True

with open('./tokens_config.json', 'r') as file:
    tokens_json = json.loads(file.read())

API_TOKEN, OAUTH_TOKEN, SAVED_CHAT_ID = tokens_json['API_TOKEN'], tokens_json['OAUTH_TOKEN'], tokens_json['SAVED_CHAT_ID']

# Прочие вспомогательные функции 

from mainlib import * 
from main_time import * 


## Извлечение конфига
with open("./config.json", 'rb') as file:
    config_json = json.loads(file.read().decode('utf-8')) 

default_receivers = [SAVED_CHAT_ID[i] for i in SAVED_CHAT_ID]

time_now = datetime.now().strftime('%H:%M')

config_json_backup = config_json 

for i, _ in enumerate(config_json):
    config_json[i]['receiver_list'] = config_json[i].get('receiver_list', default_receivers)
    config_json[i]['accuracy'] = config_json[i].get('accuracy', 'full')
    config_json[i]['mute_na'] = config_json[i].get('mute_na', '0')
    config_json[i]['ACTIVATED'] = True if time_now >= config_json[i]["activation_time"] else False 
## /Извлечение конфига
 
RESET_TIME = "23:59"

sched = pd.DataFrame(columns=['action_time','check_name','is_processed','conf_json_i','check_type'])
for i, _ in enumerate(config_json):
    init_time=0
    print(config_json[i]['check_name'])
    
    for k in range(config_json[i]['check_period']):
        z=k+config_json[i]['delay_h']
        if z>24: z-=24
        m = config_json[i]['delay_m']
        if z<10: 
            if m<10:
                t='0'+f'{z}'+':0'+f'{m}'
            else:
                t='0'+f'{z}'+':'+f'{m}'
        else:            
            if m<10:
                t=f'{z}'+':0'+f'{m}'
            else:
                t=f'{z}'+':'+f'{m}'
        if config_json[i]['check_period']>1:
            check_type=1 #проверка каждый час
        else:
            check_type=0 #проверка раз в сутки
        r={'action_time':[t],'check_name':[config_json[i]['check_name']],'is_processed':False,'conf_json_i':i,'check_type':check_type}
        sched = sched.append(pd.DataFrame(r))
print(sched)
if DBG:
    sched.to_excel("init.xlsx")
                   
ACTIVE = True
while ACTIVE:
    time_now = datetime.now().strftime('%H:%M')
    if len(sched[sched.is_processed==False])==0 or time_now==RESET_TIME:
        sched.is_processed=False
        print("###########################################################")
        print("#################TAGS WIPED (NEW CYCLE)####################")
        print("###########################################################")


    for index,row in sched[sched.action_time==time_now].iterrows():
        if DBG:
            sched.to_excel(time_now+"schd.xlsx")
            
        if row.is_processed==False:
 #           if row.check_name == 'sberbank.ru submit form' or row.check_name == 'corporate: Эвотор. Заявка на покупку':
            if row.check_type == 0:
                print("started: sberbank.ru submit form")

                sched.loc[(sched.action_time==time_now) & (sched.check_name==row.check_name),'is_processed'] = True
                check= config_json_backup[row.conf_json_i] 
                try:
                    check_and_send_message(name=check['url'], check_name=check['check_name'], url=check['url'], counter=check['counter_id'], day_window=check['day_window'], top_count=check['top_count'], sensivity=check['sensivity'], accuracy=check['accuracy'], metrics=check['metrics'], dimensions=check['dimensions'], filters=check['filters'], validity_function=check['validity_function'], SAVED_CHAT_ID=SAVED_CHAT_ID, OAUTH_TOKEN=OAUTH_TOKEN, API_TOKEN=API_TOKEN, receiver_list=check['receiver_list'],warn_receivers=check['warn_receivers'], mute_na=check['mute_na'])
                except:
                    receiver_list = check['receiver_list']
#                        print(receiver_list)
                    API_TOKEN = '1979869998:AAFS2m8yCDPQpWRMZK3aQq9wzib6A4fFsAg'
                    client = TelegramClient(API_TOKEN)
                    message_list = []
#                    check= config_json_backup[row.conf_json_i] 
                    message_list.append('По результатам суточной  проверки ' + check["check_name"]+' аномалий не обнаружено')
                    for receiver in receiver_list:
                        for i in zip(message_list, file_list):
                            print(i)
                            client.sendMessage(receiver, i[0])
                            client.sendDocument(receiver, i[1])
                    f = open("check_type0.txt","a")
                    f.write(time_now)
                    f.write(check['check_name'])
                    f.write(check['validity_function'])
                    f.write('--------------------')
                    f.close()
#            if row.check_name == 'submit form SITE_Corporate_open-accounts':
            if row.check_type == 1:

                sched.loc[(sched.action_time==time_now) & (sched.check_name==row.check_name),'is_processed'] = True


                test_check = config_json_backup[row.conf_json_i] 
                test_check["check_period"]="1"

                df = get_parametrized_metrics_csv_hourwise(**retreive_args(test_check))
   #             print(df)
                if DBG:
                    df.to_excel(time_now+"df.xlsx")
                if len(df)>0:
                   
                    df_aggr = get_aggr_by_date(df)
                    #df_aggr.values

                    lowerbound,upperbound =outlier_treatment_sigma(df_aggr[df_aggr.columns[-1]].values, test_check['sensivity'])

                    now = max(df_aggr['date'])
                    ab_dates = get_abnormal_dates_hourwise(df, test_check['sensivity'])
                    ab_dates_str = ab_dates['date'].dt.strftime('%Y-%m-%d').values

                    if now.strftime('%Y-%m-%d') in ab_dates_str:
            
                        print('Есть аномалия за сегодня')
                        ab_df = df[df['date'].dt.strftime('%Y-%m-%d') == now.strftime('%Y-%m-%d')]
        # на вход переименовать test_check
                        process_arguements = {
                            "abnormal_df" : ab_df,
                            "abn_date" : now,
                            "day_window" : test_check['day_window'],
                            "top_abn" : 0,
                            "sensivity" : test_check['sensivity'],
                            "window_df" : df,
                            "val_name" : None,
                            'val_f' : None,
                            'url' : test_check['url'],
                            'name' : test_check['check_name'],
                            "check_period" : test_check['check_period']
                        }

                        message_list = []
                        file_list = []

                        message = process_abnormality_time(**process_arguements)    
                        message_list.append(message)

                        name = test_check['name']
                        saved_filename = './{name}_{date}.xls'.format(name=name, date=now.strftime('%Y-%m-%d'))
                        date_df_dict = split_df_by_date(ab_df)
                        abnormal_date = now.strftime('%Y-%m-%d')
                        validate_f = None

                        date_df_dict[abnormal_date]['date'] = date_df_dict[abnormal_date]['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

                        if not(validate_f is None):
                            date_df_dict[abnormal_date]['status'] = date_df_dict[abnormal_date]['URL'].apply(lambda x: validate_f(x))

                        metric = [i for i in ["pageviews", "visits"] if i in date_df_dict[abnormal_date].columns]    
                        date_df_dict[abnormal_date].sort_values(metric, ascending=False).to_excel(saved_filename, index=False)
                        file_list.append(saved_filename)


                        receiver_list = test_check['receiver_list']
#                        print(receiver_list)
                        API_TOKEN = '1979869998:AAFS2m8yCDPQpWRMZK3aQq9wzib6A4fFsAg'
                        client = TelegramClient(API_TOKEN)

                        for receiver in receiver_list:
                            for i in zip(message_list, file_list):
                                print(i)
                                client.sendMessage(receiver, i[0])
                                client.sendDocument(receiver, i[1])
                    else:
                        
                        
        # на вход переименовать test_check
                        ab_df = df
                        process_arguements = {
                            "abnormal_df" : ab_df,
                            "abn_date" : now,
                            "day_window" : test_check['day_window'],
                            "top_abn" : 0,
                            "sensivity" : test_check['sensivity'],
                            "window_df" : df,
                            "val_name" : None,
                            'val_f' : None,
                            'url' : test_check['url'],
                            'name' : test_check['check_name'],
                            "check_period" : test_check['check_period']
                        }

                        message_list = []
                        file_list = []

                        message_list.append('По результатам проверки '+test_check["check_name"]+' аномалий не обнаружено')
                        

                        message = process_abnormality_time(**process_arguements)    
                        message_list.append(message)

                        name = test_check['name'] + "_" +test_check['check_name']
                        saved_filename = './{name}_{date}.xls'.format(name=name, date=now.strftime('%Y-%m-%d'))
         
                        date_df_dict = split_df_by_date(ab_df)
                        abnormal_date = now.strftime('%Y-%m-%d')
                        validate_f = None

                        date_df_dict[abnormal_date]['date'] = date_df_dict[abnormal_date]['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

                        if not(validate_f is None):
                            date_df_dict[abnormal_date]['status'] = date_df_dict[abnormal_date]['URL'].apply(lambda x: validate_f(x))

                        metric = [i for i in ["pageviews", "visits"] if i in date_df_dict[abnormal_date].columns]    
                        date_df_dict[abnormal_date].sort_values(metric, ascending=False).to_excel(saved_filename, index=False)
                        file_list.append(saved_filename)


                        receiver_list = test_check['receiver_list']
                        print(receiver_list)
                        API_TOKEN = '1979869998:AAFS2m8yCDPQpWRMZK3aQq9wzib6A4fFsAg'
                        client = TelegramClient(API_TOKEN)

                        for receiver in receiver_list:
                            for i in zip(message_list, file_list):
                                print(i)
                                client.sendMessage(receiver, i[0])
                                client.sendDocument(receiver, i[1])

    sleep(5)
