# percent 비율로 자금을 투입해서 period 뒤에 파는 방식으로 1년치 계산했을때 자금 결과를 시뮬레이션
import datetime
import pymysql
from datetime import timedelta
import dbmanager


class Simulator :
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        self.TRUST_LIMIT = 0.90
        self.LEAVE_LIMIT = 0.60

    def getPercent(self, start, final) :
        return round((final / start) - 1, 5)
    def isTargetPercent(self, percent):
        return percent > self.TRUST_LIMIT
    def isLeavePercent(self, percent):
        return percent < self.LEAVE_LIMIT
    def simulate(self, stockName, dayLimit, period):
        today = datetime.date.today()
        seed = 100
        investFlag = False
        plusForecasts = list()
        for day in range (dayLimit) :
            minusDayCnt = dayLimit - day
            pastDate = today - timedelta(days=minusDayCnt)
            analyzedData = self.dbm.analyzedSqlTargetAt(stockName, pastDate, period)
            if analyzedData is None :
                continue
            forecastPercent = self.getDivideNumPercent(analyzedData.get('plus'), (analyzedData.get('plus') + analyzedData.get('minus')))
            if forecastPercent == 0 :
                continue
            movedPercent = self.getPercent(analyzedData.get('start'), analyzedData.get('final'))

            if investFlag :
                if self.isLeavePercent(forecastPercent) is False :
                    seed = self.keepSeed(movedPercent, forecastPercent, seed)
                    continue
                else :
                    investFlag = False
            if self.isTargetPercent(forecastPercent) :
                investFlag = True
                seed = self.keepSeed(movedPercent, forecastPercent, seed)
        return {'name': stockName, 'seed': seed, 'percents': plusForecasts}
    def getDivideNumPercent(self, num1, num2):
        if num2 == 0:
            return 0
        return int((num1 / num2) * 100)
    def keepSeed(self, movedPercent, forecastPercent, seed):
        seed = self.calculate(0, movedPercent, seed)
        print('target percent', forecastPercent, movedPercent, seed)
        return seed

    def calculate(self, startPercent, endPercent, price):
        percent = endPercent - startPercent
        # return price + (price * (percent * 0.01))
        return price + (price * percent)


