__author__ = 'YoungMin'

import win32com.client




#member = win32com.client.Dispatch("dscbo1.StockMember1")
class DSStockError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class DSStock :
    def __init__(self):
        self.cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        self.ins = win32com.client.Dispatch("CpUtil.CpStockCode")
        self.stock = win32com.client.Dispatch("dscbo1.StockMst")
        self.graph = win32com.client.Dispatch("dscbo1.CbGraph1")
        self.chart = win32com.client.Dispatch("CpSysDib.StockChart")
        if self.cybos.IsConnect is not 1:
            raise DSStockError("disconnect")

    def Get(self, name):
        totalCount = self.ins.GetCount()
        for i in range(0, totalCount):
            if self.ins.GetData(1, i).startswith(name):
                print("[", self.ins.getData(0, i) + "][", self.ins.getData(1, i)+"]")
                return self.ins.getData(0, i)
        print("Not found name : " + str(name))
        return ""
    def getUpDown(self, code):
        UP_DOWN_CODE = 12
        self.stock.setInputValue(0, code)
        self.stock.BlockRequest()
        print(self.stock.GetHeaderValue(UP_DOWN_CODE))

    def getChartDataList(self, code, count):
        self.chart.SetInputValue(0, code)    # 대신증권 종목 코드
        self.chart.SetInputValue(1, ord('2'))    # 요청 구분 (개수로 요청)
        self.chart.SetInputValue(4, count)     # 요청개수
        self.chart.SetInputValue(5, [0,2,3,4,5]) # 날짜, 시가, 고가, 저가, 종가
        self.chart.SetInputValue(6, ord('D'))    # 차트 구분 (일)
    
        ## 데이터 호출
        self.chart.BlockRequest()
        num = self.chart.GetHeaderValue(3)
        data=[]
        for i in range(num):
            temp ={}
            temp['Date']=(self.chart.GetDataValue(0,i))
            temp['Open']=float(format(self.chart.GetDataValue(1,i), '.2f')) # 선물값이 오류수정
            temp['High']=float(format(self.chart.GetDataValue(2,i), '.2f'))
            temp['Low']=float(format(self.chart.GetDataValue(3,i), '.2f'))
            temp['Close']=float(format(self.chart.GetDataValue(4,i), '.2f'))
            data.append(temp)
        return data

ds = DSStock()
code = ds.Get("삼성정밀화학")
print(code)
datas = ds.getChartDataList(code, 365*2)

for data in datas :
    print(data)
















