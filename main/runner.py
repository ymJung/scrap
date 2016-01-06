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



dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)

stocks = dbm.getUsefulStockList()
results = []
for stock in stocks :
    stockCode = stock.get('code')
    stockId = stock.get('id')
    stockName = stock.get('name')
    ds = stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH)
    datas = ds.getChartDataList(stockCode, 365 * 2)
    ds.insertFinanceData(datas, str(stockId))
    ds.finalize()
    ppomppuResult = webscrap.Ppomppu().getTrend(PPOMPPU_ID, PPOMPPU_PWD, stockName)  # id , password , search
    saveDbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
    saveDbm.saveData(ppomppuResult, stockName)
    saveDbm.finalize()
    paxnetResult = webscrap.Paxnet().getTrendByCode(stockCode)
    paxnetSaveDbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
    paxnetSaveDbm.saveData(paxnetResult, stockName)
    paxnetSaveDbm.finalize()
    anal = analyzer.Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH)
    anal.analyze()
    mine = miner.Miner(DB_IP, DB_USER, DB_PWD, DB_SCH)

    period = 2
    minusCnt, plusCnt = mine.getAnalyzedCnt(period, stockName)
    result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
    results.append(result)
dbm.finalize()

print(results)





