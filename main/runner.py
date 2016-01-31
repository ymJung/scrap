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

        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)

        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg = mine.getAnalyzedCnt(targetAt, period, stockName)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
        print(str(result))

        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt + timedelta(days=period), targetPlusAvg, targetMinusAvg)
        stockDbm.finalize()


    def run(self, stock, targetAt, period, busy):
        if busy is False :
            self.insertFinance(stock)
            self.insertPpomppuResult(stock)
            self.insertPaxnetResult(stock)
            self.insertNaverResult(stock)
            anal = analyzer.Analyzer(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
            anal.analyze()
            anal.finalize()
            upd = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
            upd.updateLastUseDate(stock)
            upd.finalize()

        self.insertAnalyzedResult(stock, targetAt, period)
        self.update(stock)

    def update(self, stock) :
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        runDbm.updateAnalyzedResultItem(stock)
        runDbm.finalize()

    def migration(self, stock, period, dayLimit):
        print('migration')
        for minusDay in range(dayLimit):
            targetAt = date.today() - timedelta(days=minusDay)
            print('migration target at ' + str(targetAt) + ' period ' + str(minusDay) + '/' + str(dayLimit))
            self.run(stock, targetAt, period, True)



    def getNewItem(self, stockCode):
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        insert = ds.getStock(stockCode)
        datas = ds.getChartDataList(insert.get('code'), 365 * 2)
        ds.insertFinanceData(datas, str(insert.get('id')))
        ds.finalize()

    def printForecastData(self, forecastResult, analyzedResult):
        #i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final
        #i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId
        plusPoint = []
        minusPoint = []

        for each in forecastResult:
            resultPrice = each.get('final') - each.get('start')
            plus = each.get('plus')
            plusAvg = each.get('plusAvg')
            minus = each.get('minus')
            total = plus + minus
            stockName = each.get('name')
            minusAvg = each.get('minusAvg')
            if total > 0 :
                if resultPrice > 0:  # plus
                    plusPoint.append({'name': stockName, 'point': plus / total, 'avg': plusAvg})
                else:
                    minusPoint.append({'name': stockName, 'point': minus / total, 'avg': minusAvg})

        for each in forecastResult :
            print('plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] result  : ' +  str(each.get('final') - each.get('start')))
        for each in analyzedResult:
            print(each.get('name') + ' plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] target  : ' +  str(each.get('targetAt')))




DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"
PPOMPPU_ACC = { 'id': "", 'pwd' : ""}
newItemCode = ''
busy = False
runner = runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC)
if len(newItemCode) > 0 :
    runner.getNewItem(newItemCode)

dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
stocks = dbm.getUsefulStockList()
period = 2 # 2일 뒤 예측.
targetAt = date.today()
duration = 30
for stock in stocks :
    # analyzed, forecast = dbm.analyzedSql(stock.get('name'))
    # runner.printForecastData(analyzed, forecast)
    runner.run(stock, targetAt, period, busy)
    # runner.migration(stock, period, duration)



