from telegram.ext import Updater
import simulator
import runner
import dbmanager
import sys
import subprocess
import telegram
from datetime import date, timedelta


valid_user = 0
TOKEN = ''


class Reporter:
    def checkUser(self, userId):
        if userId != valid_user:
            raise Exception('invalid user')

    def __init__(self):
        self.updater = Updater(TOKEN)
        self.dispatcher = self.updater.dispatcher

        self.dispatcher.addTelegramCommandHandler('help', self.guide)
        self.dispatcher.addTelegramCommandHandler('0', self.initStocks)
        self.dispatcher.addTelegramCommandHandler('1', self.filteredTarget)
        self.dispatcher.addTelegramCommandHandler('3', self.dailyRun)
        self.dispatcher.addTelegramCommandHandler('5', self.targetAnalyze)
        self.dispatcher.addTelegramCommandHandler('g', self.garbageRecycle)
        self.dispatcher.addTelegramCommandHandler('c', self.calculate)

        self.dispatcher.addTelegramCommandHandler('ㅎ', self.garbageRecycle)
        self.dispatcher.addTelegramRegexHandler('.*', self.usefulGarbage)
        self.garbageIds = list()
        self.run = self.newRunnerInstance()

    def commit(self):
        self.run.dbm.commit()
    def guide(self, bot, update):
        self.checkUser(update.message.chat_id)
        reply_markup = telegram.ReplyKeyboardMarkup([['/0 init stocks','/1 filteredTarget (day)', '/3 dailyRun', '/5 targetAnalyze','/g', '/c [start] [end] [price]']])
        bot.sendMessage(chat_id=update.message.chat_id, text= 'guide', reply_markup=reply_markup)
    def filteredTarget(self, bot, update, args): # /1
        self.checkUser(update.message.chat_id)
        try:
            bot.sendMessage(chat_id=update.message.chat_id, text='targeting..')
            run = self.newRunnerInstance()
            if len(args) > 0 and args[0].isdigit() :
                targetDay = int(args[0])
                print('target day ', targetDay)
                filteredTarget = run.filteredTarget(date.today()+timedelta(days=targetDay)) #하루에 한번씩

            else :
                filteredTarget = run.filteredTarget(date.today())
            bot.sendMessage(chat_id=update.message.chat_id, text=str(filteredTarget))
        except:
            print("unexcept error.", sys.exc_info())
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')

    def dailyRun(self, bot, update):
        self.checkUser(update.message.chat_id)
        try:
            bot.sendMessage(chat_id=update.message.chat_id, text='running..')
            subprocess.call('python process\\runnerProcess.py')
            # run = self.runnerNewInstance()
            # targetPeriod = period
            # if bot, update.message.text.split()[1] != '':
            #     targetPeriod = int(bot, update.message.text.split()[1])
            # run.dailyRun(targetPeriod)
            bot.sendMessage(chat_id=update.message.chat_id, text='done')
        except dbmanager.DBManagerError:
            bot.sendMessage(chat_id=update.message.chat_id, text='today daily run is done')
        except:
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')
            print("unexcept error.", sys.exc_info())


    def targetAnalyze(self, bot, update, args):
        self.checkUser(update.message.chat_id)
        try:
            bot.sendMessage(chat_id=update.message.chat_id, text='target analyze')
            run = self.newRunnerInstance()
            result = run.targetAnalyze(stockCode= args[0], period=args[1])
            bot.sendMessage(chat_id=update.message.chat_id, text=str(result))
        except:
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')
            print("unexcept error.", sys.exc_info())

    def initStocks(self, bot, update):
        self.checkUser(update.message.chat_id)
        try:
            bot.sendMessage(chat_id=update.message.chat_id, text='init stocks')
            self.run.initStocks()
        except:
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')
            print("unexcept error.", sys.exc_info())


    def setFilteredGarbage(self, garbageId):
        if len(self.garbageIds) > 0 :
            self.garbageIds = list()
        self.garbageIds.append(garbageId)

    def garbageRecycle(self, bot, update, args):
        self.checkUser(update.message.chat_id)
        try:
            if len(args) > 1 :
                print(args)
                garbageId = args[0]
                process = args[1]
                if process is 'Y' :
                    bot.sendMessage(chat_id=update.message.chat_id, text='choose useful word. (I`m wait)')
                    self.setFilteredGarbage(garbageId)
                    return
                if process is 'P' :
                    self.run.updateGarbageAndInsertWord(garbageId, None)
                if process is 'D' :
                    self.run.updateGarbageStatus(garbageId, process)

            self.sendGarbageKeyBoard(bot, update.message.chat_id)
        except:
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')
            print("unexcept error.", sys.exc_info())
        finally:
            self.commit()
    def sendGarbageKeyBoard(self, bot, userId):
        garbage = self.run.getUndefinedGarbageWord()
        reply_markup = telegram.ReplyKeyboardMarkup([[
                            self.getGarbageMenuStr(garbage.get('id'), 'Y') + telegram.Emoji.THUMBS_UP_SIGN,
                            self.getGarbageMenuStr(garbage.get('id'),'P') + telegram.Emoji.BUS,
                            self.getGarbageMenuStr(garbage.get('id'),'D') + telegram.Emoji.THUMBS_DOWN_SIGN,
                            self.getGarbageMenuStr(garbage.get('id'),'N') + telegram.Emoji.BABY]])
        self.setFilteredGarbage(garbage.get('id'))
        bot.sendMessage(chat_id=userId, text= str(garbage.get('id')) + ' ' + garbage.get('word'), reply_markup=reply_markup)
    def getGarbageMenuStr(self, id, process):
        return '/g ' + str(id) + ' ' + process.replace(' ', '') + ' '

    def start(self):
        print('polling.....')
        self.updater.start_polling()

    def usefulGarbage(self, bot, update):
        try:
            if '/' in update.message.text :
                return
            if len(self.garbageIds) != 1 :
                bot.sendMessage(chat_id=update.message.chat_id, text='I need only one garbage. I will clear.')
                self.garbageIds = list()
                return
            garbageId = self.garbageIds[0]
            usefulWord = update.message.text.strip()
            result = self.run.updateGarbageAndInsertWord(garbageId, usefulWord)
            if result is True :
                bot.sendMessage(chat_id=update.message.chat_id, text='insert word. : ' + garbageId + ' ' + usefulWord)
            else :
                bot.sendMessage(chat_id=update.message.chat_id, text='pass. : ' + usefulWord)
            self.sendGarbageKeyBoard(bot, update.message.chat_id)
        except:
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')
            print("unexcept error.", sys.exc_info())
        finally:
            self.commit()

    def calculate(self, bot, update):
        self.checkUser(update.message.chat_id)
        try:
           params = update.message.text.split(' ')
           simul = simulator.Simulator()
           result = simul.calculate(float(params[1]), float(params[2]), float(params[3]))
           bot.sendMessage(chat_id=update.message.chat_id, text=str(result))

        except:
            bot.sendMessage(chat_id=update.message.chat_id, text='something to wrong')
            print("unexcept error.", sys.exc_info())

    def newRunnerInstance(self):
        return runner.Runner()
    def send(self, msg):
        self.updater.bot.sendMessage(chat_id=valid_user, text=msg)



