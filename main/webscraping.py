__author__ = 'YoungMin'

import win32com.client

ins = win32com.client.Dispatch("CpUtil.CpStockCode")
totalCount = ins.GetCount()

def Get(name):
    find = False
    for i in range(0, totalCount):
        if ins.GetData(1, i).startswith(name):
            find = True
            print("Code : ", ins.getData(0, i))
            print("Name : ", ins.getData(1, i))
            print("Idx : ", i)
            return ins.getData(0, i)
    if find is False:
        print("Not found name : " + name)
    return "";



code = Get("삼성정밀화학")






