__author__ = 'YoungMin'

import win32com.client

cybos = win32com.client.Dispatch("CpUtil.CpCybos")
ins = win32com.client.Dispatch("CpUtil.CpStockCode")
stock = win32com.client.Dispatch("dscbo1.StockMst")
#chart = win32com.client.Dispatch("CpSysDib.StockChart")
#member = win32com.client.Dispatch("dscbo1.StockMember1")

totalCount = ins.GetCount()

def Get(name):
    for i in range(0, totalCount):
        if ins.GetData(1, i).startswith(name):
            print("[", ins.getData(0, i) + "][", ins.getData(1, i)+"]")
            return ins.getData(0, i)
    print("Not found name : " + name)
    return ""




code = Get("삼성정밀화학")
UP_DOWN = 12
flag = cybos.IsConnect
if flag != 1 :
    print ("disconnect")
else :
    stock.setInputValue(0, code)
    stock.BlockRequest()
    print(stock.GetHeaderValue(UP_DOWN))








