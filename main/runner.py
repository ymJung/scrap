DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

PPOMPPU_ID = ""
PPOMPPU_PWD = ""


import stockscrap
import dbmanager
import analyzer
import miner
import webscrap
from datetime import date, timedelta

def insertFinance(stocks) :
    for stock in stocks :
        stockCode = stock.get('code')
        stockId = stock.get('id')
        ds = stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH)
        datas = ds.getChartDataList(stockCode, 365 * 2)
        ds.insertFinanceData(datas, str(stockId))
        ds.finalize()
def insertPpomppuResult(stocks):
    for stock in stocks :
        stockName = stock.get('name')
        ppomppuResult = webscrap.Ppomppu().getTrend(PPOMPPU_ID, PPOMPPU_PWD, stockName)  # id , password , search
        saveDbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        saveDbm.saveData(ppomppuResult, stockName)
        saveDbm.finalize()
def insertPaxnetResult(stocks) :
    for stock in stocks :
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnetResult = webscrap.Paxnet().getTrendByCode(stockCode)
        paxnetSaveDbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        paxnetSaveDbm.saveData(paxnetResult, stockName)
        paxnetSaveDbm.finalize()
def insertNaverResult(stocks) :
    for stock in stocks :
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode)
        naverSaveDbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        naverSaveDbm.saveData(naverResult, stockName)
        naverSaveDbm.finalize()
def insertAnalyzedResult(stocks) :
    for stock in stocks :
        stockDbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        stockName = stock.get('name')
        anal = analyzer.Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH)
        anal.analyze()
        mine = miner.Miner(DB_IP, DB_USER, DB_PWD, DB_SCH)

        period = 2
        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt = mine.getAnalyzedCnt(period, stockName)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, date.today() - timedelta(days=period))
        print(str(result))


dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)

stocks = dbm.getUsefulStockList()

insertFinance(stocks)
insertPpomppuResult(stocks)
insertPaxnetResult(stocks)
insertNaverResult(stocks)
insertAnalyzedResult(stocks)
dbm.finalize()



