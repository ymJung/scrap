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
import os

def send_message(bot, chat_id, message):
    bot.sendMessage(chat_id=chat_id, text=message)



def command_execute(bot, update):
    input_text = update.message.text
    chat_id = update.message.chat_id
    print(chat_id, input_text)
    if input_text == 'exit':
        send_message(bot, chat_id, 'bye')
        return
    if COMMAND_MAP not in input_text:
        return
    else:
        os.system(input_text)
        print('ok', input_text)

def guide(bot, update):
    bot.sendMessage(chat_id=update.message.chat_id, text='')

def split(map, command):
    map[command.split(',')[0]] = command.split(',')[1]

import sys
COMMAND_MAP = {}
split(COMMAND_MAP, cf.get('commander', 'COMMAND1'))
split(COMMAND_MAP, cf.get('commander', 'COMMAND2'))


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
