import requests 
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from matplotlib import pyplot as plt
from matplotlib.pyplot import figure
from telegramcli import *



def process_abnormality_time(abnormal_df, abn_date, day_window, top_abn, sensivity=1, \
                    window_df=None, val_name=None, val_f = None, url='dokhodchivo', name='None', check_period="0"):
    day_window = int(day_window)
    def split_df_by_date(df):
        '''
        Разбивает заданный датафрейм по полю 'date' и возвращает словарь с парами ключ-значение вида "дата : pdDataFrame"
        '''
        df_dict = dict()
        for i in df.groupby('date'):
            df_dict[str(i[0])[:10]] = i[1]
        return df_dict

    def hours_rus(n):
        if 10 <= n < 20:
            return f'{n} часов'
        if n % 10 == 1:
            return f'{n} час'
        if n % 10 in (2, 3, 4):
            return f'{n} часа'
        return f'{n} часов'


    def calculate_difference(df_tar):
        DIGITS_AFTER_POINT = 2
        df_tar_copy = df_tar.copy()
        df_tar_copy['date'] = df_tar_copy['date'].apply(lambda x: str(x)[:10])
        total_metric = df_tar_copy.groupby('date').sum()[:-1].values

        window_avg = total_metric.sum() / len(total_metric)

        last_day = total_metric[-1]

        diff = float(last_day / window_avg) - 1
        rounded_diff = round(diff * 100 * 10 ** DIGITS_AFTER_POINT) / 10 ** DIGITS_AFTER_POINT
        return ('+' if rounded_diff > 0 else '') + str(rounded_diff) + "%"    

    SENSIVITY_DECODE = {'0': 'Низкая', '1' : 'Средняя', '2' : 'Высокая'}

    ab_df = split_df_by_date(abnormal_df)[abn_date.strftime('%Y-%m-%d')].copy()

    date_str = abn_date.strftime('%Y-%m-%d')
    date_from = (abn_date - timedelta(day_window)).strftime('%Y-%m-%d')

    descr_list = []

    descr_list.append('<b>Сайт</b>: {site}'.format(site=url))
    descr_list.append('<b>Интервал</b>: с {date_from} по {date_to} в окне за {day_window_string}:'.format(
                                                                          date_from=date_from,
                                                                          date_to=date_str, 
                                                                          day_window_string=days_window_to_str(day_window)))
    descr_list.append('<b>Чувствительность</b>: {sens}'.format(sens=SENSIVITY_DECODE.get(sensivity, str(sensivity) + 'σ')))
    descr_list.append('<b>Проверка</b>: {check_name}'.format(check_name=name))
    descr_list.append('<b>Частота</b>: 1 раз в час')

    check_period_int = round(int(check_period))
    print(check_period_int, " -- PERIOD NUMBER")
    if check_period_int % 24 != 0:
        descr_list.append(f'<b>Период</b>: за последний {hours_rus(int(check_period))}.')
    #descr_list.append('<b>Изменение</b>: {diff}'.format(diff=calculate_difference()))

    diff = calculate_difference(window_df)
    if not (window_df is None): 
        change_name = 'Увеличение' if diff[0] == '+' else 'Снижение'
        descr_list.append('<b>{name}</b>: {diff}'.format(name=change_name, diff=calculate_difference(window_df)))   

    #message = "\n".join([msg_hello, msg_range, msg_sensivity, msg_check])
    message = "\n".join(descr_list)

    metric_column = ab_df.columns[-1]

    ab_df[metric_column] = ab_df[metric_column].map(int)


    if not(val_f is None) and val_name == 'Статус сейчас':
        url_pos = list(ab_df.columns).index("URL")
        ab_df["status"] = ab_df.apply(lambda x: val_f(list(x)[1:]), axis=1)
        ab_df_top = ab_df[ab_df["status"] == 404].sort_values(metric_column, ascending=False)[ab_df.columns[1:]].iloc[:top_abn].values
    else:
        ab_df_top = ab_df.sort_values(metric_column, ascending=False)[ab_df.columns[1:]].iloc[:top_abn].values
    str_table = ''
    url_template = '<a href="{src}">{text}</a>'

    translation_dict_const = {'URL' : 'Страница', 'pageviews' : 'Просмотры', 'referer' : 'Реферер', 'visits' : 'Визиты'}

    translation_dict = {**get_translation_dict(), **translation_dict_const}

    column_names = [translation_dict.get(name, name) for name in ab_df.columns[1:]]

    cv_pair = '<b>{column}</b>:\n{value}\n'

    #str_table += ['_______\n' + ''.join([cv_pair.format(column=str(column_names[val[0]]), value=str(val[1])) for val in enumerate(row)]) for row in ab_df_top]
    
    if int(top_abn) > 0:
        limit = -1 if 'status' in ab_df.columns else len(ab_df_top[0])
        for row in ab_df_top:
            row_message = []
            for val in enumerate(row[:limit]):
                if str(val[1]).find('https://') != -1:
                    value = url_template.format(src=val[1],text=short_url(val[1], url))
                else:
                    value = str(val[1])
                column = column_names[val[0]] 
                row_message.append(cv_pair.format(column=column, value=value))

            if val_name is not None and val_f is not None:
                row_message.append(cv_pair.format(column=val_name, value=val_f(row)))

            str_table += '_______\n' + ''.join(row_message)

        message_top = '\n<i>Топ {top_count} произошедших событий по частоте:</i>\n'
        message_ending = '_______\nПодробный отчет во вложении.'
        
        final_message = message + '\n' + message_top.format(top_count=top_abn) + str_table + message_ending
    else:
        final_message = message
    if val_name == 'Статус сейчас': 
         ab_df.drop(columns=['status'], axis=1, inplace=True)
    return final_message

def get_translation_dict(path='./translation.csv'):
    d = dict()
    try:
        trans_df = pd.read_csv(path)

        return {i[0][i[0].rfind(':') + 1:] : i[1] for i in trans_df.values}
    except Exception as e:
        print('Failed to get translation')
        print(str(e))
        return dict()


def days_window_to_str(day_window):
    '''
    Склоняет заданное число (5 -> '5 дней', 22 -> '22 дня' и т.д.)
    '''
    if day_window in range(10, 20):
        day_window_str = '{i} дней'.format(i=day_window)
    elif day_window % 10 == 1:
        day_window_str = '{i} день'.format(i=day_window)
    elif (day_window % 10) in [2, 3, 4]:
        day_window_str = '{i} дня'.format(i=day_window)
    else:
        day_window_str = '{i} дней'.format(i=day_window)
    return day_window_str



def retreive_args(check):
    return { "days_ago" : check['day_window'],
             "counter" : check['counter_id'],
             "metrics" : check['metrics'],
             "dimensions" : check['dimensions'],
             "filters" : check['filters'],
             "check_period" : check['check_period'],
             "accuracy" : 'full',
             "OAUTH_TOKEN" : 'AQAAAAALxNAjAAdYdvo6WUPa-U6-sj3x6ysdc7Q'}

def get_parametrized_metrics_csv_hourwise(days_ago, counter, metrics, dimensions, filters, accuracy, OAUTH_TOKEN, check_period=24):
    from io import StringIO

    date1 = f'{days_ago}daysAgo'.format(daysAgo=days_ago)
    date2  = 'today'
    group = 'day'    # документация https://yandex.ru/dev/metrika/doc/api2/api_v1/intro.html
    
    def parse_str(string):
        res = []
        for elem in string.split(','):
            try:
                res_elem = elem.replace(' ', '')
                res_elem = res_elem[res_elem.rfind(':') + 1:] 
                res.append(res_elem)
            except Exception as e:
                print(f'Exception {str(e)} occured during parsing {string} of element {res_elem}')
        return res
    
    def split_by_hours(arg, check_period):
        min_date = arg.min()['date']
        i = 0
        intervals = []
        dfs = []
        while True:
            int_from = (datetime.now() - timedelta(hours=check_period) - timedelta(i)).strftime('%Y-%m-%d %H')
            int_to = (datetime.now() - timedelta(i)).strftime('%Y-%m-%d %H')
            int_from = pd.to_datetime(int_from)
            int_to = pd.to_datetime(int_to)
            interval = [int_from, int_to]
            intervals.append(interval)
            dfs.append(arg[(arg['date'] >= interval[0]) & (arg['date'] < interval[1])])
            if interval[1] < min_date:
                break
            i += 1
        return pd.concat(dfs, ignore_index=True)
    
    
    headers = {
                'GET': '/management/v1/counters HTTP/1.1',
                'Host': 'api-metrika.yandex.net',
                'Authorization': 'OAuth {token}'.format(token=OAUTH_TOKEN),
                'Content-Type': 'application/x-yametrika+json'
    }
    
    metrics_url = 'https://api-metrika.yandex.net/stat/v1/data.csv?ids='+counter+'&metrics='+metrics+'&date1='+date1+'&date2='+date2+'&dimensions='+dimensions+'&filters='+filters+'&group='+group+'&limit=100000&accuracy='+accuracy
    csv_response = requests.get(metrics_url, headers=headers).text
    df = pd.read_csv(StringIO(csv_response))[1:]
    if len(df)<1:
        with open("hourly_check.log", "a") as myfile:
            # datetime object containing current date and time
            now = datetime.now()
            # dd/mm/YY H:M:S
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            myfile.write("zero dataframe detected:"+dt_string+" : ")	
            myfile.write(metrics_url)
    try:
        #return df
        mapper = {col : renamed_col for col, renamed_col in zip(df.columns, parse_str(dimensions) + parse_str(metrics)) }
        df = df.rename(columns=mapper).rename(columns={'startOfHour' : 'date'})
    
        hour_interval = round(int(check_period)) % 24
        df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d')
        if hour_interval != 0:
            df = split_by_hours(df, hour_interval)
        
        #vals = [days_ago, counter, metrics, dimensions, filters, accuracy, OAUTH_TOKEN, check_period]
        #val_names = ['days_ago', 'counter', 'metrics', 'dimensions', 'filters', 'accuracy', 'OAUTH_TOKEN', 'check_period']
            # Выведем в сообщение об ошибке так же все пары аргументов, при которых эта ошибка произошла:
        #print(",".join([val_name + ":" + val for val, val_name in zip(vals,val_names)]))
        if len(df)<1:
            with open("hourly_check.log", "a") as myfile:
                # datetime object containing current date and time
                now = datetime.now()
                # dd/mm/YY H:M:S
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                myfile.write("zero dataframe detected on return:"+dt_string+" : ")	
                myfile.write(metrics_url)
        return df
    except Exception as e:
        err_name = datetime.now().strftime('response_exception_%Y_%m_%d_%H_%M_%S')
        print('Exception occured during handling response. Dumped to errors/{err_name}.txt, errors/{err_name}.csv'.format(err_name=err_name))
        with open('errors/{err_name}.txt'.format(err_name=err_name)) as file:
            msg = ['Exception occured during handling response.']
            msg.append(str(e))
            msg.append('request arguments:')
                  
            vals = [days_ago, counter, metrics, dimensions, filters, accuracy, OAUTH_TOKEN, check_period]
            val_names = ['days_ago', 'counter', 'metrics', 'dimensions', 'filters', 'accuracy', 'OAUTH_TOKEN', 'check_period']
            # Выведем в сообщение об ошибке так же все пары аргументов, при которых эта ошибка произошла:
            msg.append(",".join([val_name + ":" + val for val, val_name in zip(vals,val_names)]))
            file.write(str(e))
        df.to_csv('./errors/{name}.csv'.format(name=err_name), index=False)
        return df
    
            

def get_abnormal_dates(df, sensivity):
    '''
    Получает список дат, для которых "прошла" проверка на аномальность
    '''
    metric_column = df.columns[-1]
    lowerbound,upperbound =outlier_treatment_sigma(df.groupby(['date'])[metric_column].sum().values, sensivity)
    date_anomaly_full_list = df.groupby(['date'])[metric_column].sum().index.where(df.groupby(['date'])[metric_column].sum()>upperbound)
    date_anomaly = [date_anomaly_full_list for date_anomaly_full_list in date_anomaly_full_list if date_anomaly_full_list==date_anomaly_full_list]
    return date_anomaly

def outlier_treatment_sigma(datacolumn, sensivity):
    '''
    Новый способ вычисления аномальных значений на основе нормального распределения и сравнения значения с 3\sigma
    '''
    data_mean, data_std = np.mean(datacolumn), np.std(datacolumn)
    cut_off = data_std * float(sensivity)
    lower = round(data_mean - cut_off,2)
    upper = round(data_mean + cut_off,2)
    return lower,upper


def get_abnormal_dates_new(df, sensivity, check_period="24"):
    '''   
    Получает список дат, для которых "прошла" проверка на аномальность
    ''' 
    hour_period = round(int(check_period)) % 24
    metric_column = df.columns[-1]

    if hour_period > 0: 
        freq_value = f'{hour_period}H'
    else:
        freq_value = '24H'
    
    lowerbound,upperbound =outlier_treatment_sigma(df.groupby(pd.Grouper(key='date', freq=freq_value))[metric_column].sum().values, sensivity)
    date_anomaly_full_list = df.groupby(pd.Grouper(key='date', freq=freq_value))[metric_column].sum().index.where(df.groupby(pd.Grouper(key='date', freq=freq_value))[metric_column].sum()>upperbound)
    date_anomaly = [date_anomaly_full_list for date_anomaly_full_list in date_anomaly_full_list if date_anomaly_full_list==date_anomaly_full_list]
    return date_anomaly

def get_abnormal_dates_hourwise(df, sensivity):
    df_aggr = get_aggr_by_date(df)
    lowerbound,upperbound =outlier_treatment_sigma(df_aggr[df_aggr.columns[-1]].values, sensivity)
    return df_aggr[(df_aggr[df_aggr.columns[-1]] > upperbound) | (df_aggr[df_aggr.columns[-1]] < lowerbound)].dropna()

def get_aggr_by_date(df):
    res_df = df.copy()
    res_df['date'] = pd.to_datetime(res_df['date'].dt.strftime('%Y-%m-%d'))
    return res_df[['date', df.columns[-1]]].groupby('date').sum().reset_index()


#counter_id = 1175048
'''
test_check = \
{"name" : "sberbank.ru",
 "check_name" : "submit form SITE_Corporate_open-accounts",
 "url" : "sberbank.ru",
 "counter_id" : "1175048",
 "day_window" : "7",
 "frequency" : "1",
 "sensivity" : "3",
 "top_count" : "3",
 "activation_time" : "test",
 "validity_function" : "status_code",
 "metrics": "ym:s:visits",
 "dimensions": "ym:s:startOfHour,ym:s:paramsLevel1,ym:s:paramsLevel2,ym:s:paramsLevel3",
 "filters": "paramsLevel1=='SITE_Corporate_open-accounts' AND paramsLevel2=='submit_form' AND paramsLevel3 !@ 'callback_multi'",
 "check_period" : "1",
 "receiver_list" : ["345537144"]
}

#metric = 'Pageviews' 
df = get_parametrized_metrics_csv_hourwise(**retreive_args(test_check))
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
    API_TOKEN = '1979869998:AAFS2m8yCDPQpWRMZK3aQq9wzib6A4fFsAg'
    client = TelegramClient(API_TOKEN)

    for receiver in receiver_list:
        for i in zip(message_list, file_list):
            print(i)
            client.sendMessage(receiver, i[0])
            client.sendDocument(receiver, i[1])
            '''