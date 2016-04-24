__author__ = 'YoungMin'

import win32com.client
import datetime
import dbmanager


# member = win32com.client.Dispatch("dscbo1.StockMember1")
class DSStockError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class DSStock:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        self.DATE_FORMAT = '%Y%m%d'
        self.cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        self.ins = win32com.client.Dispatch("CpUtil.CpStockCode")
        self.stock = win32com.client.Dispatch("dscbo1.StockMst")
        self.graph = win32com.client.Dispatch("dscbo1.CbGraph1")
        self.chart = win32com.client.Dispatch("CpSysDib.StockChart")
        if self.cybos.IsConnect is not 1:
            raise DSStockError("disconnect")
        self.DATE = 'date'
        self.START = 'start'
        self.HIGH = 'high'
        self.LOW = 'low'
        self.FINAL = 'final'
        self.MARKET_OFF_HOUR = 15


    def __del__(self):
        self.dbm.commit()
        self.dbm.close()

    def getStock(self, stockCode):
        stock = self.dbm.selectStockByCode(stockCode)
        if stock is None:
            totalCount = self.ins.GetCount()
            for i in range(0, totalCount):
                dsCode = self.ins.GetData(0, i)
                dsName = self.ins.getData(1, i)

                if dsCode == str(stockCode) or dsCode.replace('A','') == str(stockCode):
                    self.dbm.insertStock(dsCode, dsName)
                    print("insert [", dsCode , "][", dsName , "]")
                    return self.dbm.selectStockByCode(stockCode)
            print("Not found name : " + str(stockCode))

            raise DSStockError('not found stock')
        else:
            hit = int(stock.get('hit')) + 1
            self.dbm.updateStockHit(hit, stock.get('id'))
            return self.dbm.selectStockByCode(stockCode)

    def getUpDown(self, code):
        UP_DOWN_CODE = 12
        self.stock.setInputValue(0, code)
        self.stock.BlockRequest()
        print(self.stock.GetHeaderValue(UP_DOWN_CODE))

    def getChartDataList(self, code, count):
        self.chart.SetInputValue(0, code)  # 대신증권 종목 코드
        self.chart.SetInputValue(1, ord('2'))  # 요청 구분 (개수로 요청)
        self.chart.SetInputValue(4, count)  # 요청개수
        self.chart.SetInputValue(5, [0, 2, 3, 4, 5])  # 날짜, 시가, 고가, 저가, 종가
        self.chart.SetInputValue(6, ord('D'))  # 차트 구분 (일)

        ## 데이터 호출
        self.chart.BlockRequest()
        num = self.chart.GetHeaderValue(3)
        data = []
        for i in range(num):
            temp = {}
            temp[self.DATE] = (self.chart.GetDataValue(0, i))
            temp[self.START] = float(format(self.chart.GetDataValue(1, i), '.2f'))  # 선물값이 오류수정
            temp[self.HIGH] = float(format(self.chart.GetDataValue(2, i), '.2f'))
            temp[self.LOW] = float(format(self.chart.GetDataValue(3, i), '.2f'))
            temp[self.FINAL] = float(format(self.chart.GetDataValue(4, i), '.2f'))
            data.append(temp)
        return data
    def insertFinanceData(self, datas, stockId):

        for data in datas:
            date = datetime.datetime.strptime(str(data.get(self.DATE)), self.DATE_FORMAT)
            start = data.get(self.START)
            high = data.get(self.HIGH)
            low = data.get(self.LOW)
            final = data.get(self.FINAL)
            if (int(date.today().strftime(self.DATE_FORMAT)) == data.get(self.DATE)) and date.now().hour < self.MARKET_OFF_HOUR :
                continue

            finance = self.dbm.selectFinanceByStockIdAndDate(stockId, date)
            if finance is None :
                self.dbm.insertFinance(stockId, date, high, low, start, final)
                print('insert finance' + str(date))
            dayOfFinanceData = self.dbm.getFinanceDataByDay(stockId, date, datetime.datetime.strptime(str(data.get(self.DATE)) + str(self.MARKET_OFF_HOUR), self.DATE_FORMAT + '%H'))

            if dayOfFinanceData is not None :
                self.dbm.updateFinance(high, low, start, final, dayOfFinanceData.get('id'))
                print('update finance' + str(date))
        self.dbm.commit()


    def insertNewStock(self, stockCode):
        insert = self.getStock(stockCode)
        datas = self.getChartDataList(insert.get('code'), 365 * 2)
        self.insertFinanceData(datas, str(insert.get('id')))
        self.dbm.commit()

