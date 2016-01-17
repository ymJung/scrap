import stockscrap
import dbmanager
import analyzer
import miner
import webscrap
from datetime import date, timedelta

class runner :
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC) :
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH

        self.PPOMPPU_ID = PPOMPPU_ACC.get('id')
        self.PPOMPPU_PWD = PPOMPPU_ACC.get('pwd')

    def insertFinance(self, stock) :
        stockCode = stock.get('code')
        stockId = stock.get('id')
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        datas = ds.getChartDataList(stockCode, 365 * 2)
        ds.insertFinanceData(datas, str(stockId))
        ds.finalize()
    def insertPpomppuResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockName = stock.get('name')
        ppomppuResult = webscrap.Ppomppu().getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName, lastUseDateAt)  # id , password , search
        saveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        saveDbm.saveData(ppomppuResult, stockName)
        saveDbm.finalize()
    def insertPaxnetResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnetResult = webscrap.Paxnet().getTrendByCode(stockCode, lastUseDateAt)
        paxnetSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        paxnetSaveDbm.saveData(paxnetResult, stockName)
        paxnetSaveDbm.finalize()
    def insertNaverResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastUseDateAt)
        naverSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        naverSaveDbm.saveData(naverResult, stockName)
        naverSaveDbm.finalize()
    def insertAnalyzedResult(self, stock) :
        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        stockName = stock.get('name')
        anal = analyzer.Analyzer(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        anal.analyze()
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)

        period = 2
        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt = mine.getAnalyzedCnt(period, stockName)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, date.today() + timedelta(days=period))
        print(str(result))
        stockDbm.finalize()

    def run(self, stock):
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        self.insertFinance(stock)
        self.insertPpomppuResult(stock)
        self.insertPaxnetResult(stock)
        self.insertNaverResult(stock)
        self.insertAnalyzedResult(stock)
        runDbm.updateLastUseDate(stock)
        # dbm.updateAnalyzedResultItem(stock) #TODO --
        runDbm.finalize()

    def getNewItem(self, stockName):
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        insert = ds.getStock(stockName)
        datas = ds.getChartDataList(insert.get('code'), 365 * 2)
        ds.insertFinanceData(datas, str(insert.get('id')))
        ds.finalize()



dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
stocks = dbm.getUsefulStockList()
for stock in stocks :
     work = runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC)
     work.run(stock)

#stockName = '대우조선해양'
#work.getNewItem(stockName)


#dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
#dbm.updateAnalyzedResultItem('서연')


