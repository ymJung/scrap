import pymysql
from datetime import date, timedelta
import configparser

LIMIT = 0.70
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
            "SELECT ds.name, f.type, f.code, f.analyzeAt, f.potential, f.volume FROM data.forecast f, data.daily_stock ds WHERE ds.code = f.code AND f.analyzeAt > %s AND f.code = %s GROUP BY f.id ORDER BY f.analyzeAt, f.code ASC",
            (target_at, code))
        results = cursor.fetchall()
        return self.get_result_msg(results)

    def get_result_msg(self, results):
        msg = ''
        for data in results:
            msg += (data.get('analyzeAt').strftime("%Y-%m-%d")
                    + ' [' + data.get('name')
                    + '] [' + data.get('code')
                    + '] [' + str(data.get('type'))
                    + '] ['+ str(data.get('potential'))
                    + '] [' + str(data.get('volume')) + ']\n')
        return msg

    def get_code(self, param):
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
    poten_data = dbm.get_target_forecast(code)
    send_message(bot, chat_id, poten_data)


def guide(bot, update):
    bot.sendMessage(chat_id=update.message.chat_id, text='stock code or name')


tb = telegram.Bot(token=ORA_TOKEN)
updater = Updater(ORA_TOKEN)

updater.dispatcher.addTelegramCommandHandler('help', guide)
updater.dispatcher.addTelegramMessageHandler(conversation)
send_message(tb, VALID_USER, 'hello')
updater.start_polling()
