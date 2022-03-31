from yametrics import *
from telegramcli import *


def short_url(url, internal_name=None):
    '''
    Производит удаление из заданной строки url домен сайта (сокращает запись)
    '''
    if url is None:
        return None
    subs_index = url.find('https://')
    if subs_index != -1:
        sh_url = url[8:]
        if internal_name is None or url.find(internal_name) != -1:
            start = sh_url.find('/')
            if start != -1:
                return sh_url[start:]
            else:
                return url
        else:
            return sh_url
    else:
        return url

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


def split_df_by_date(df):
    '''
    Разбивает заданный датафрейм по полю 'date' и возвращает словарь с парами ключ-значение вида "дата : pdDataFrame"
    '''
    df_dict = dict()
    for i in df.groupby('date'):
        df_dict[str(i[0])[:10]] = i[1]
    return df_dict


def validate_404_error_row(row):
    '''
    Функция, которая принимает на вход строку таблицы, полученной из яндекс метрики и проводит для нее валидацию
    в смысле проверки статуса загрузки этой страницы
    '''
    try:
        from requests import get
        return get(row[0], verify=False).status_code
    except Exception as e:
        print('For value {val} exception occured:'.format(val=row))
        print(str(e))
        return 'None'
    
def validate_404_error(val):
    '''
    Функция, которая принимает значение (url) и на его основе проводит валидацию (в смысле проверки статуса загрузки страницы)
    '''
    try:
        from requests import get
        return get(val, verify=False).status_code
    except Exception as e:
        print('For value {val} exception occured:'.format(val=val))
        print(str(e))
        return 'None'
    

def process_abnormality(abnormal_df, abn_date, day_window, top_abn, sensivity=1, window_df=None, val_name=None, val_f = None, url='dokhodchivo', name='None'):
    '''
    Генерирует сообщение для "подписчика" телеграм бота, обрабатывая заданную анормальную дату (содержащую ) и датафрейм.
    Конкретнее:
    
    abnormal_df -- кусочек таблицы, содержащий значения таблицы для аномальной даты
    abn_date    -- аномальная дата, для которой нужно произвести обработку и сгенерировать сообщение
    day_window  -- размер окна, в течение которого берется метрика для выявления аномальностей 
    top_abn     -- количество сообщений, которые будут выведены в телеграм (топ по метрике)
    sensivity   -- чувствительность детектора аномалий
    window_df   -- таблица, содержащая метрику за весь период наблюдений (day_window). Если не передана, изменение не фиксируется
    val_name    -- название проверки, которая будет производится с указанной метрикой 
    val_f       -- название функции проверки, которая будет вызываться для заданной метрики 
    
    
    '''

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
    
    ab_df = split_df_by_date(abnormal_df)[abn_date].copy()
    date_str = abn_date
    date_from = (datetime.strptime(abn_date, '%Y-%m-%d') - timedelta(day_window)).strftime('%Y-%m-%d')
    
    descr_list = []

    descr_list.append('<b>Сайт</b>: {site}'.format(site=url))
    descr_list.append('<b>Период</b>: с {date_from} по {date_to} в окне за {day_window_string}:'.format(
                                                                          date_from=date_from,
                                                                          date_to=date_str, 
                                                                          day_window_string=days_window_to_str(day_window)))
    descr_list.append('<b>Чувствительность</b>: {sens}'.format(sens=SENSIVITY_DECODE.get(sensivity, str(sensivity) + 'σ')))
    descr_list.append('<b>Проверка</b>: {check_name}'.format(check_name=name))
    descr_list.append('<b>Частота</b>: 1 раз в день')
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
    print(ab_df.columns)
    print("before")
    if val_name == 'Статус сейчас': 
         ab_df.drop(columns=['status'], axis=1, inplace=True)
    print(ab_df.columns)
    print("after")
    return final_message



def check_and_send_message(name, check_name, url, counter, day_window, top_count, sensivity, accuracy, metrics, dimensions, 
                           filters, validity_function, SAVED_CHAT_ID, OAUTH_TOKEN, API_TOKEN, receiver_list, warn_receivers, mute_na):
    '''
    Производит проверку заданной метрики по следующим параметрам
    
    name -- название сайта
    check name -- название проверки
    url -- url, для которого производится проверка
    counter -- counter сайта, для которого происходит проверка метрики
    day_window -- размер окна (в днях) для которого производится проверка
    top_count -- какое количество 
    SAVED_CHAT_ID -- словарь идентификаторов чата, которым будет передано сообщение 
    OAUTH_TOKEN   -- Токен яндекс метрики
    API_TOKEN     -- Токен Telegram API бота, через которого будет передан отчет 
    '''

    top_count = int(top_count)
    day_window = int(day_window)
    
    is_anomalies_detected = False
    
        # 1. Получение метрики за N дней начиная с сегодняшнего для 

    VALIDITY_FUNCTIONS = {"status_code" : {
        "value_name" : 'Статус сейчас',
        "row_function" : validate_404_error_row,
        "function" : validate_404_error
    }}

    # 2.
    print(counter)
    print(metrics)
    print(dimensions)
    print(filters)
    df_target = get_parametrized_metrics(day_window, counter, metrics, dimensions, filters, accuracy, OAUTH_TOKEN=OAUTH_TOKEN)

    abnormal_dates = get_abnormal_dates(df_target, sensivity)
    abnormal_df = df_target[df_target['date'].isin(abnormal_dates)]

    abnormal_dates_str = [i.strftime('%Y-%m-%d') for i in abnormal_dates]

    all_found_dates_str = {i[0] for i in df_target.values}
    
    try:
        date_does_exist = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d') in [i.strftime('%Y-%m-%d') for i in all_found_dates_str]
        
        if not date_does_exist: 
            try: 
                print('APPENDING WITH NULL ROW')
                def append_with_nulls(df):
                    date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
                    return df.append(pd.DataFrame([[date] + [None] * (len(df.iloc[0]) - 2) + [0]], columns=df.columns))
                df_target = append_with_nulls(df_target)
                abnormal_df = append_with_nulls(abnormal_df)
                date_does_exist = True
            except Exception as e:
                print("##########################")
                print("Exception occured during row append with null row")
                date_does_exist = False
    except Exception as e:
        date_does_exist = False
        print(df_target)
        #raise e
    if not date_does_exist:
        message_list = ['<b>Для проверки "{check_name}" сайта {url} данных за {date} не было обнаружено</b>'.format(
            check_name=check_name,
            url=url,
            date=(datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        )]
        
        logfilename = './log/{name}_{date}.xls'.format(
            date=datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
            name=name
        )
        try:
            df_target['date'] = df_target['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df_target.to_excel(logfilename, index=False)

            file_list = [logfilename]
        except Exception as e:
            #raise e
            message_list = ['<b>Для проверки "{check_name}" сайта {url} данных не было обнаружено.</b>'.format(check_name=check_name,url=url)]
            file_list = ['']
        client = TelegramClient(API_TOKEN)
        for receiver in receiver_list:
            for i in zip(message_list, file_list):
                print('############################################')
                print(client.sendMessage(receiver, i[0]))
                client.sendDocument(receiver, i[1])
                print('############################################')
        return 
    if (datetime.now() - timedelta(1)).strftime('%Y-%m-%d') in abnormal_dates_str:
        message_list = ['Обнаружена аномалия 🙈. Полный отчет во вложении'.format(name=name)]
        file_list = ['']
        is_anomalies_detected = True
    else:
        message_list = ['Для сайта {name} за вчерашний день аномалий не обнаружено! Полный отчет во вложении'.format(name=name)]
        file_list = ['']

    # 3. Развилка: если существуют аномалии, о них стоит сообщить (а так же срез топ 10 реферов, переводящих на 404)

    #abnormalities = len(abnormal_dates)
    if (datetime.now() - timedelta(1)).strftime('%Y-%m-%d') in abnormal_dates_str or mute_na == "0":
        abnormal_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
        abnormal_df = df_target[df_target['date'].isin([pd.Timestamp((datetime.now() - timedelta(1)).strftime('%Y-%m-%d'))])]

        date_df_dict = split_df_by_date(abnormal_df)
    
        validate_f = None
        validate_name = None
        validate_f_row = None
        if not VALIDITY_FUNCTIONS.get(validity_function, None) is None:
            validate_name = VALIDITY_FUNCTIONS.get(validity_function, None).get('value_name')
            validate_f = VALIDITY_FUNCTIONS.get(validity_function, None).get('function')
            validate_f_row = VALIDITY_FUNCTIONS.get(validity_function, None).get('row_function')
    
        message = process_abnormality(abnormal_df, abnormal_date, day_window, top_count, sensivity, df_target, validate_name, validate_f_row, url, check_name)
        message_list.append(message)
        saved_filename = './{name}_{date}.xls'.format(name=name, date=abnormal_date)
        date_df_dict[abnormal_date]['date'] = date_df_dict[abnormal_date]['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    
        if not(validate_f is None):
            date_df_dict[abnormal_date]['status'] = date_df_dict[abnormal_date]['URL'].apply(lambda x: validate_f(x))
    
        metric = [i for i in ["pageviews", "visits"] if i in date_df_dict[abnormal_date].columns]    
        date_df_dict[abnormal_date].sort_values(metric, ascending=False).to_excel(saved_filename, index=False)
        file_list.append(saved_filename)

    # Постоянное сохранение лога можно отключить в конфигурационном файле.

        logfilename = './log/{name}_{date}.xls'.format(
            date=datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
            name=name
        )
        df_target['date'] = df_target['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        metric = [i for i in ["pageviews", "visits"] if i in df_target.columns]
        df_target.sort_values(metric, ascending=False).to_excel(logfilename, index=False)

    # 4. Отправляем результат работы всем администраторам

        client = TelegramClient(API_TOKEN)

        for receiver in receiver_list:
            for i in zip(message_list, file_list):
                print(i)
                client.sendMessage(receiver, i[0])
                client.sendDocument(receiver, i[1])
                
        if is_anomalies_detected:
            for receiver in warn_receivers:
                for i in zip(message_list, file_list):
                    print(i)
                    client.sendMessage(receiver, i[0])
                    client.sendDocument(receiver, i[1])
            
