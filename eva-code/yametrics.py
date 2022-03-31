import pandas as pd
from pandas import json_normalize
import json
import requests
import numpy as np
from datetime import datetime, timedelta
import requests 

def outlier_treatment(datacolumn):
 '''
 Старый способ получения аномальных значений из колонки с помощью проверки через перцентили.
 '''
 sorted(datacolumn)
 Q1,Q3 = np.percentile(datacolumn , [25,75])
 IQR = Q3 - Q1
 lower_range = Q1 - (1.5 * IQR)
 upper_range = Q3 + (1.5 * IQR)
 return lower_range,upper_range


# Получение метрики (целевая программа)

def outlier_handler(datacolumn):
    '''
    Старый способ получения аномальных значений из колонки с помощью проверки через перцентили.
    '''
    sorted(datacolumn)
    Q1,Q3 = np.percentile(datacolumn , [25,75])
    IQR = Q3 - Q1
    lower_range = Q1 - (1.5 * IQR)
    upper_range = Q3 + (1.5 * IQR)
    return lower_range,upper_range

def outlier_treatment_sigma(datacolumn, sensivity): 
    '''
    Новый способ вычисления аномальных значений на основе нормального распределения и сравнения значения с 3\sigma
    '''
    data_mean, data_std = np.mean(datacolumn), np.std(datacolumn)
    cut_off = data_std * float(sensivity)
    lower = round(data_mean - cut_off,2)
    upper = round(data_mean + cut_off,2)
    return lower,upper

def filter_values(df_arg):
    try:
        res = df_arg.copy()
    
        *dimensions, metric = list(res.columns.values)

        def get_value_translation(path='./value_translation.txt'):
            value_translation_dict = dict()
            with open(path, 'r') as file:
                for line in file:
                    try:
                        dimension, value, translation = map(lambda x:x.strip(), line.split(','))
                        if dimension not in value_translation_dict:
                            value_translation_dict[dimension] = dict()
                        value_translation_dict[dimension][value] = translation
                    except:
                        pass
            return value_translation_dict
    
        def is_url(x): 
            '''
            Функция фильтрации для отесеивания колонок, которые по смыслу не содержат ссылки
            '''
            for var in ['url', 'referer']:
                if var in x.lower():
                    return True
            return False
    
        #Сначала отфильтруем все значения, содержащие get аргументы:
        for column in filter(is_url, dimensions):
            res[column] = res[column].apply(lambda x: x[:x.rfind('?')] if x.rfind('?') != -1 else x)

        #Теперь применим словарь с переводами значений:
        value_translation = get_value_translation() 
    
        for column in filter(lambda x: x in value_translation, dimensions):
            res[column] = res[column].apply(lambda x: value_translation[column].get(x, x))
        
        return res.groupby(dimensions).sum().reset_index()
    except Exception as e:
        print("Exception occured during transformation")
        print(str(e))
        return df_arg


def get_parametrized_metrics(days_ago, counter, metrics, dimensions, filters, accuracy, OAUTH_TOKEN):
    '''
    Получает таблицу со значением заданных метрик 
    
    days_ago -- на сколько дней нужно получить метрику
    counter  -- id счетчика 
    metrics     --|| Параметры API запроса к яндекс метрики, настраиваемые в конфигурационном файле
    dimensions  --||
    filters     --||
    OAUTH_TOKEN -- Токен яндекс метрики 
    '''
    def get_mapper(metrics, dimensions):
        '''
        Определяет принцип, по которому будут называться столбцы итогового датафрейма (для этого анализируются входящие
        параметры: metrics, dimensions, т.к. из их названия можно легко получить названия будущей таблицы)
        '''
        split = lambda x : x[x.rfind(':') + 1:]
        mapper_list = list(map(split, dimensions.split(','))) + [split(metrics.split(',')[0])]
        digit_list = list(range(len(mapper_list)))
        string_digit_dict = {str(pair[1]) : pair[0] for pair in zip(mapper_list, digit_list)}
        int_digit_dict = {pair[1] : pair[0]  for pair in zip(mapper_list, digit_list)}
        #Дополним получившийся словарь так же численными значениями (значения, получаемые с метриками, могут оказываться
        # как и численными так и строковыми )

        result_dict = {**string_digit_dict, **int_digit_dict}

        return result_dict
    date1 = f'{days_ago}daysAgo'.format(daysAgo=days_ago)
    date2  = 'today'
    group = 'day'    # документация https://yandex.ru/dev/metrika/doc/api2/api_v1/intro.html

    headers = {
                'GET': '/management/v1/counters HTTP/1.1',
                'Host': 'api-metrika.yandex.net',
                'Authorization': 'OAuth {token}'.format(token=OAUTH_TOKEN),
                'Content-Type': 'application/x-yametrika+json'
    }

    metrics_url = 'https://api-metrika.yandex.net/stat/v1/data?ids='+counter+'&metrics='+metrics+'&date1='+date1+'&date2='+date2+'&dimensions='+dimensions+'&filters='+filters+'&group='+group+'&limit=100000&accuracy='+accuracy
    json_content_response = requests.get(metrics_url, headers=headers).json()
    #the following lines are for debug only need to be removed
    f = open("check_type_24.txt","a")
    m_time_now = datetime.now().strftime('%H:%M')
    f.write(m_time_now)
    f.write('--------------------')
    f.write(filters)
    f.write('------END-BLOCK-----')
    f.close()
    
    data = json_normalize(json_content_response['data'])
    df = pd.DataFrame()
    
    try:
        for criteria in data.dimensions.values:
            app=pd.Series([i['name'] for i in criteria])
            df=df.append(app, ignore_index = True)
        app=[]
        for criteria in data.metrics.values:
            app.append(criteria[0])
        app=pd.Series(app)
        df[len(data.dimensions[0])]=app
        mapper= get_mapper(metrics, dimensions)
        df = df.rename(columns = mapper)
        df.date=df.date.apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
        col_values_to_drop_nulls = [i for i in df.columns if i not in ["pageviews", "visits", "date"]]
        return filter_values(df).dropna(subset=col_values_to_drop_nulls, how='all')
    except Exception as e:
        #raise e
        return pd.DataFrame([[None, None]], columns=['date', 'metrics'])


def get_abnormal_dates(df, sensivity):
    '''
    Получает список дат, для которых "прошла" проверка на аномальность
    '''
    metric_column = df.columns[-1]
    lowerbound,upperbound =outlier_treatment_sigma(df.groupby(['date'])[metric_column].sum().values, sensivity)
    date_anomaly_full_list = df.groupby(['date'])[metric_column].sum().index.where(df.groupby(['date'])[metric_column].sum()>upperbound)
    date_anomaly = [date_anomaly_full_list for date_anomaly_full_list in date_anomaly_full_list if date_anomaly_full_list==date_anomaly_full_list]
    return date_anomaly
