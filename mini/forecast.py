from datetime import date, timedelta
import pymysql
from telegram.ext import Updater
import configparser

cf = configparser.ConfigParser()
cf.read('config.cfg')
DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')
VALID_USER = 60403284

TOKEN = cf.get('telegram', 'TOKEN')
LINK3 = '/day_candles.json?limit='
LINK4 = '&to='
LINK1 = cf.get('KAKAO_STOCK', 'link1')


class Forecast:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.LIMIT_RATE = 0.70

    def get_max_target_at(self):
        cursor = self.connection.cursor()
        cursor.execute("select max(evaluate) as evaluateMax from data.forecast")
        evaluateMax = cursor.fetchone().get('evaluateMax')
        cursor.execute("select analyzeAt from data.forecast group by analyzeAt order by analyzeAt desc limit %s",
                       (evaluateMax))
        results = cursor.fetchall()
        if len(results) >= evaluateMax:
            return results[evaluateMax - 1].get('analyzeAt')
        return date.today()
    def getPotentialDatas(self, target_at, limitRate):
        cursor = self.connection.cursor()
        query = "SELECT ds.name, f.type, f.code, f.analyzeAt, f.potential, f.volume , f.percent, f.evaluate FROM data.forecast f, data.daily_stock ds " \
                "WHERE f.type = 3 AND ds.code = f.code AND analyzeAt > %s and potential > %s group by f.id ORDER BY f.analyzeAt, f.code ASC"
        cursor.execute(query, (target_at, str(limitRate)))
        return cursor.fetchall()

    def is_compare_chain_minus(self, code, analyze_at, day_cnt):
        cursor = self.connection.cursor()
        cursor.execute("select date from data.daily_stock ds "
        "where ds.code = %s and ds.date < %s order by ds.id desc limit %s", (code, analyze_at, day_cnt))
        dates = cursor.fetchall()

        result = True
        for date in dates:
            cursor.execute("select (ds.close-ds.open) as compare from data.daily_stock ds where ds.code = %s and ds.date = %s",
                (code, date.get('date')))
            compare = cursor.fetchone().get('compare')
            if compare > 0:
                result = False
        return result


    def getPotential(self, target_at, chan_minus):
        datas = self.getPotentialDatas(target_at, self.LIMIT_RATE)
        msg = ''
        for data in datas:
            compare = self.is_compare_chain_minus(data.get('code'), data.get('analyzeAt'), chan_minus)
            if compare:
                msg += (data.get('analyzeAt').strftime("%Y-%m-%d")
                        + ' [' + str(data.get('evaluate'))
                        + '] [' + data.get('code')
                        + '] [' + data.get('name')
                        + '] [' + str(data.get('type'))
                        + '] [' + str(data.get('potential'))
                        + '] [' + str(data.get('volume'))
                        + '] [' + str(data.get('percent'))
                        + ']\n')
        return msg


forecast = Forecast(DB_IP, DB_USER, DB_PWD, DB_SCH)
updater = Updater(TOKEN)

updater.bot.sendMessage(chat_id=VALID_USER,
                            text=forecast.getPotential(target_at=forecast.get_max_target_at() - timedelta(days=1), chan_minus=1))
