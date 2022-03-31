import requests

class TelegramClient:
    def __init__(self, api_token):
        self.api_token = api_token
        self.recipients = set()        
        
    def _DisconnectMessage(self):
        return "Сессия была сброшена"
    
    def sendMessage(self, chat_id, text):
        if chat_id not in self.recipients:
            self.recipients.add(chat_id)
        return self._invokeTelegramMethod('sendMessage', 
                                          'POST', 
                                          chat_id=chat_id, 
                                          text=text,
                                          disable_web_page_preview=True,
                                          parse_mode='HTML'
                                          )
        
    def sendPhoto(self, chat_id, filename):
        with open(filename, 'rb') as fileio:
            file_bytes = fileio.read()
        return self._invokeTelegramMethodFiles('sendPhoto', 
                                               'POST', 
                                               chat_id=chat_id, 
                                               f_photo=file_bytes)
    
    def sendDocument(self, chat_id, filename, caption=''):
        try:
            with open(filename, 'rb') as fileio:
                file_bytes = fileio.read()
            return self._invokeTelegramMethodFiles('sendDocument', 
                                                   'POST', 
                                                   chat_id=chat_id, 
                                                   caption=caption,
                                                   h_filename=filename,
                                                   disable_content_type_detection=True,
                                                   f_document=file_bytes)
        except:
            return ''
    
    def _invokeTelegramMethod(self, method, request_type='GET', **kargs):
        from requests import get 

        arg_string = "&".join([i + '=' + str(kargs[i]) for i in kargs])

        REQ_GET_TEMPLATE = 'https://api.telegram.org/bot{API_TOKEN}/{method_name}{arg_string}'
        REQ_POST_TEMPLATE = 'https://api.telegram.org/bot{API_TOKEN}/{method_name}'

        if arg_string != '':
            arg_string = "?" + arg_string

        if request_type == 'GET':
            request = REQ_GET_TEMPLATE.format(API_TOKEN=self.api_token, method_name=method, arg_string=arg_string)    
            response = get(request)
        elif request_type == 'POST':
            request = REQ_POST_TEMPLATE.format(API_TOKEN=self.api_token, method_name=method)
            response = requests.post(request, data = kargs)      
        else:
            return None
        return response.content
    
    def _invokeTelegramMethodFiles(self, method, request_type='GET', **kargs):
        '''
        Аргументы, перечисленные с названием вида f_* передаются в запрос без префикса. 
        '''
        from requests import get 

        arg_string = "&".join([i + '=' + str(kargs[i]) for i in kargs])

        REQ_GET_TEMPLATE = 'https://api.telegram.org/bot{API_TOKEN}/{method_name}{arg_string}'
        REQ_POST_TEMPLATE = 'https://api.telegram.org/bot{API_TOKEN}/{method_name}'

        if arg_string != '':
            arg_string = "?" + arg_string

        if request_type == 'GET':
            request = REQ_GET_TEMPLATE.format(API_TOKEN=self.api_token, method_name=method, arg_string=arg_string)    
            response = get(request)
        elif request_type == 'POST':
            request = REQ_POST_TEMPLATE.format(API_TOKEN=self.api_token, method_name=method)
            file_args = dict()
            non_file_args = dict()
            header_args = dict()
            for arg in kargs:
                if arg[:2] == 'f_':
                    file_args[arg[2:]] = kargs[arg]
                elif arg[:2] == 'h_':
                    header_args[arg[2:]] = kargs[arg]
                else:
                    non_file_args[arg] = kargs[arg]
            for i in file_args:
                file_args[i] = (header_args['filename'], file_args[i])
            
            response = requests.post(request, data = non_file_args, files = file_args, headers = header_args)      
        else:
            return None
        return response.content