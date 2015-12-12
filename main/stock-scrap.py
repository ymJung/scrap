__author__ = 'YoungMin'

import win32com.client
import pymysql.cursors

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

    def finalize(self):
        self.connection.commit()
        self.connection.close()

    def selectStockCode(self, name):
        cursor = self.connection.cursor()
        selectStockSql = "SELECT `id`,`code`,`refreshCount` FROM `stock` WHERE `name`=%s"
        stockCursor = cursor.execute(selectStockSql, (name))
        if stockCursor == 0:
            totalCount = self.ins.GetCount()
            for i in range(0, totalCount):
                if self.ins.GetData(1, i).startswith(name):
                    insertStockSql = "INSERT INTO `data`.`stock` (`code`,`name`) VALUES (%s, %s);"
                    cursor.execute(insertStockSql, (self.ins.getData(0, i), self.ins.getData(1, i)))
                    print("insert [", self.ins.getData(0, i) + "][", self.ins.getData(1, i) + "]")
                    return self.ins.getData(0, i)
            print("Not found name : " + str(name))
            return ""
        else:
            fetch = cursor.fetchone()
            code = fetch.get('code')
            id = fetch.get('id')
            refreshCount = fetch.get('refreshCount')
            refreshCount = int(refreshCount) + 1
            refreshCountUpdateSql = "UPDATE `stock` SET `refreshCount`=%s WHERE `id`=%s"
            cursor.execute(refreshCountUpdateSql, (refreshCount, id))
            return code

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
            temp['Date'] = (self.chart.GetDataValue(0, i))
            temp['Open'] = float(format(self.chart.GetDataValue(1, i), '.2f'))  # 선물값이 오류수정
            temp['High'] = float(format(self.chart.GetDataValue(2, i), '.2f'))
            temp['Low'] = float(format(self.chart.GetDataValue(3, i), '.2f'))
            temp['Close'] = float(format(self.chart.GetDataValue(4, i), '.2f'))
            data.append(temp)
        return data


ds = DSStock()
code = ds.selectStockCode("삼성정밀화학")
ds.finalize()
print(code)
#datas = ds.getChartDataList(code, 365 * 2)
