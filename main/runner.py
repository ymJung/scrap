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
        ppomppu = webscrap.Ppomppu()
        ppomppuResult = ppomppu.getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName, lastUseDateAt)  # id , password , search
        saveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        saveDbm.saveData(ppomppu.SITE, ppomppuResult, stockName)
        saveDbm.finalize()
    def insertPaxnetResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnet = webscrap.Paxnet()
        paxnetResult = paxnet.getTrendByCode(stockCode, lastUseDateAt)
        paxnetSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        paxnetSaveDbm.saveData(paxnet.SITE,  paxnetResult, stockName)
        paxnetSaveDbm.finalize()
    def insertNaverResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastUseDateAt)
        naverSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        naverSaveDbm.saveData(ns.SITE, naverResult, stockName)
        naverSaveDbm.finalize()

    def insertAnalyzedResult(self, stock, targetAt, period) :
        stockName = stock.get('name')
        anal = analyzer.Analyzer(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        anal.analyze()
        anal.finalize()
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)

        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg = mine.getAnalyzedCnt(targetAt, period, stockName)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}

        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt + timedelta(days=period), targetPlusAvg, targetMinusAvg)
        stockDbm.finalize()


    def todayRun(self, stock, busy):
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        if busy is False :
            self.insertFinance(stock)
            self.insertPpomppuResult(stock)
            self.insertPaxnetResult(stock)
            self.insertNaverResult(stock)
            runDbm.updateLastUseDate(stock)
        period = 2 # 2일 뒤 예측.
        targetAt = date.today()
        self.insertAnalyzedResult(stock, targetAt, period)
        runDbm.updateAnalyzedResultItem(stock)
        runDbm.finalize()

    def migration(self, stock, period, dayLimit):
        for i in dayLimit :
             pass
             #TODO  -self.insertAnalyzedResult(stock, targetAt, period)
    pass

    def getNewItem(self, stockName):
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        insert = ds.getStock(stockName)
        datas = ds.getChartDataList(insert.get('code'), 365 * 2)
        ds.insertFinanceData(datas, str(insert.get('id')))
        ds.finalize()

    def printForecastData(self, forecastResult, analyzedResult):
        #i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final
        #i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId
        for each in forecastResult :
            print('plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] result  : ' +  str(each.get('final') - each.get('start')))
        for each in analyzedResult:
            print(each.get('name') + ' plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] target  : ' +  str(each.get('targetAt')))




DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"
PPOMPPU_ACC = { 'id': "", 'pwd' : ""}
newItem = ''
busy = False
runner = runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC)
# if len(newItem) > 0 :
#     runner.getNewItem(newItem)

dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
stocks = dbm.getUsefulStockList()
for stock in stocks :
    # analyzed, forecast = dbm.analyzedSql(stock.get('name'))
    # runner.printForecastData(analyzed, forecast)
    runner.todayRun(stock, busy)

    # 1빠른 검색을 만즐자.

