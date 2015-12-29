__author__ = 'YoungMin'
STOCK_NAME = ""

import win32com.client
import pymysql.cursors
import datetime

DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"


# member = win32com.client.Dispatch("dscbo1.StockMember1")
class DSStockError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class DSStock:
    def __init__(self):
        self.DATE_FORMAT = '%Y%m%d'
        self.cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        self.ins = win32com.client.Dispatch("CpUtil.CpStockCode")
        self.stock = win32com.client.Dispatch("dscbo1.StockMst")
        self.graph = win32com.client.Dispatch("dscbo1.CbGraph1")
        self.chart = win32com.client.Dispatch("CpSysDib.StockChart")
        if self.cybos.IsConnect is not 1:
            raise DSStockError("disconnect")
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.DATE = 'date'
        self.START = 'start'
        self.HIGH = 'high'
        self.LOW = 'low'
        self.FINAL = 'final'

    def finalize(self):
        self.connection.commit()
        self.connection.close()
        print('finish')

    def selectStock(self, name):
        cursor = self.connection.cursor()
        stockCursor = cursor.execute("SELECT `id`,`code`,`hit` FROM `stock` WHERE `name`=%s", (name))
        if stockCursor != 0:
            return cursor.fetchone()
        return None

    def getStock(self, name):
        stock = self.selectStock(name)
        cursor = self.connection.cursor()
        if stock is None:
            totalCount = self.ins.GetCount()
            for i in range(0, totalCount):
                if self.ins.GetData(1, i).startswith(name):
                    cursor.execute("INSERT INTO `data`.`stock` (`code`,`name`) VALUES (%s, %s);", (self.ins.getData(0, i), self.ins.getData(1, i)))
                    print("insert [", self.ins.getData(0, i) + "][", self.ins.getData(1, i) + "]")
                    return self.selectStock(name)
            print("Not found name : " + str(name))

            raise DSStockError('not found stock')
        else:
            hit = int(stock.get('hit')) + 1
            cursor.execute("UPDATE `stock` SET `hit`=%s WHERE `id`=%s", (hit, stock.get('id')))
            return self.selectStock(name)

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
        cursor = self.connection.cursor()
        for data in datas:
            date = datetime.datetime.strptime(str(data.get(self.DATE)), self.DATE_FORMAT)
            if self.isFinanceTarget(date, stockId) is False:
                return
            start = data.get(self.START)
            high = data.get(self.HIGH)
            low = data.get(self.LOW)
            final = data.get(self.FINAL)
            cursor.execute("INSERT INTO `data`.`finance` (`stockId`,`date`,`high`,`low`,`start`,`final`) "
                           "VALUES (%s, %s, %s, %s, %s, %s);", (stockId, date, high, low, start, final))
            print('insert finance' + str(date))

    def isFinanceTarget(self, date, stockId):
        cursor = self.connection.cursor()
        stockCursor = cursor.execute("SELECT `id`,`stockId`,`date` FROM `finance` WHERE `stockId`=%s AND date = %s", (stockId, date))
        if stockCursor == 0 :
            return True
        return False


ds = DSStock()
stock = ds.getStock(STOCK_NAME)
datas = ds.getChartDataList(stock.get('code'), 365 * 2)
ds.insertFinanceData(datas, str(stock.get('id')))
ds.finalize()

