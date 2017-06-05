import pymysql
from datetime import date, timedelta
import configparser

cf = configparser.ConfigParser()
cf.read('config/config.cfg')

DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')
VALID_USER = cf.get('telegram', 'VALID_USER')

ORA_TOKEN = cf.get('telegram', 'ORA_TOKEN')


class DBManager:
    def __init__(self):
        self.conn = pymysql.connect(host=DB_IP, user=DB_USER, password=DB_PWD, db=DB_SCH, charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

    def __del__(self):
        self.conn.close()

    def get_target_forecast(self, code):
        #         code = self.get_code(param)
        #         if code is None:
        #             return None
        cursor = self.conn.cursor()
        cursor.execute("select max(evaluate) as maxEv, max(analyzeAt) as maxAt from data.forecast")
        result = cursor.fetchone()
        max_evaluate = result.get('maxEv')
        target_at = result.get('maxAt') - timedelta(days=max_evaluate)
        cursor.execute(
            "SELECT ds.name, f.type, f.code, f.analyzeAt, f.potential, f.volume, f.evaluate FROM data.forecast f, data.daily_stock ds WHERE ds.code = f.code AND f.analyzeAt > %s AND f.code = %s GROUP BY f.id ORDER BY f.analyzeAt, f.code ASC",
            (target_at, code))
        results = cursor.fetchall()
        return results

    def get_code(self, param):
        param = param.strip()
        cursor = self.conn.cursor()
        cursor.execute("SELECT distinct(code) FROM data.daily_stock WHERE name = %s", (param))
        result = cursor.fetchone()
        if result is not None:
            return result.get('code')
        cursor.execute("SELECT distinct(code) FROM data.daily_stock WHERE code = %s", (param))
        result = cursor.fetchone()
        if result is not None:
            return result.get('code')
        return None


import telegram
from telegram.ext import Updater


def send_message(bot, chat_id, message):
    bot.sendMessage(chat_id=chat_id, text=message)


import simulator

def conversation(bot, update):
    input_text = update.message.text
    chat_id = update.message.chat_id
    print(chat_id, input_text)
    if input_text == 'exit':
        send_message(bot, chat_id, 'bye')
        return

    dbm = DBManager()
    code = dbm.get_code(input_text)
    if code is None:
        send_message(bot, chat_id, 'not found.')
        return
    poten_datas = dbm.get_target_forecast(code)
    forecast_msg = get_forecast_explain(poten_datas)
    simul_msg = simulator.simulator(code=code)
    print(forecast_msg)
    print(simul_msg)
    send_message(bot, chat_id, forecast_msg)
    send_message(bot, chat_id, simul_msg)


TYPE_MAP = {3: '마감'}
def get_forecast_explain(poten_datas):
    msg = ''
    for data in poten_datas:
        today = date.today()
        forecast_at = (data.get('analyzeAt') + timedelta(days=data.get('evaluate'))).date()
        compare = (today - forecast_at).days + 1  # 계산날짜 포함
        forecast_type = TYPE_MAP[data.get('type')]
        if compare >= 0:
            msg += '[' + data.get('name') + ']는 [' + forecast_type + '] 이 [' + str(compare)
            msg += '] 일 뒤 [' + str(data.get('potential')) + '] 입니다.\n '
    return msg


def guide(bot, update):
    bot.sendMessage(chat_id=update.message.chat_id, text='stock code or name. (exact)')


import datetime
import sys

RETRY_LIMIT_CNT = 5
RETRY_LIMIT = {datetime.date.today(): RETRY_LIMIT_CNT}


def is_break():
    retry_cnt = get_date_retry_limit(datetime.date.today())
    if retry_cnt < 0:
        return True
    return False


def get_date_retry_limit(date):
    date_str = str(date)
    if date_str in RETRY_LIMIT:
        print('reduce today limit ', date_str, RETRY_LIMIT[date_str])
        RETRY_LIMIT[date_str] -= 1
    else:
        print('make today limit ', date_str)
        RETRY_LIMIT.update({date_str: RETRY_LIMIT_CNT})
    return RETRY_LIMIT[date_str]


tb = telegram.Bot(token=ORA_TOKEN)
try:
    updater = Updater(ORA_TOKEN)
    updater.dispatcher.addTelegramCommandHandler('help', guide)
    updater.dispatcher.addTelegramMessageHandler(conversation)
    send_message(tb, VALID_USER, 'hello telegram bot conversation')
    updater.start_polling()
except:
    send_message(tb, VALID_USER, 'unexpected error telegram bot conversation')
    print('unexpected error', sys.exc_info()[0])
    print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
