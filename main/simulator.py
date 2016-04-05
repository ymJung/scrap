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
        self.TRUST_LIMIT = 0.60

    def getPercent(self, stockName, targetDate) :
        finance = self.dbm.getFinanceDataByStockNameAndData(stockName, targetDate)
        start = finance.get('start')
        final = finance.get('final')
        return final / start
    def isTargetPercent(self, percent):
        return percent > self.TRUST_LIMIT
    def simulate(self, stockName, dayLimit):
        today = datetime.date.today()
        price = 100
        for day in dayLimit :
            minusDayCnt = dayLimit - day
            pastDate = today - timedelta(days=minusDayCnt)
            # if self.isTargetPercent(forecastPercent) :
            #     percent = self.getPercent(stockName, pastDate)
            #     self.calculate(0, percent, price)



        pass
    def calculate(self, startPercent, endPercent, price):
        pass