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


import telegram
from telegram.ext import Updater


def send_message(bot, chat_id, message):
    bot.sendMessage(chat_id=chat_id, text=message)



def command_execute(bot, update):
    input_text = update.message.text
    chat_id = update.message.chat_id
    print(chat_id, input_text)
    if input_text == 'exit':
        send_message(bot, chat_id, 'bye')
        return


def guide(bot, update):
    bot.sendMessage(chat_id=update.message.chat_id, text='')


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
    updater.dispatcher.addTelegramMessageHandler(command_execute)
    send_message(tb, VALID_USER, 'hello telegram bot conversation')
    updater.start_polling()
except:
    send_message(tb, VALID_USER, 'unexpected error telegram bot conversation')
    print('unexpected error', sys.exc_info()[0])
    print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
