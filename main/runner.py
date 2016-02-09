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
        forecastAt = targetAt + timedelta(days=period)
        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        if stockDbm.existForecatDate(forecastAt) :
            return
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg = mine.getAnalyzedCnt(targetAt, period, stockName)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
        print(str(result))

        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, forecastAt, targetPlusAvg, targetMinusAvg, period)
        stockDbm.finalize()
        self.update(stock)


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


    def update(self, stock) :
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        runDbm.updateAnalyzedResultItem(stock)
        runDbm.finalize()

    def migration(self, stock, period, dayLimit):
        print('migration')
        for minusDay in range(dayLimit - 1):
            targetAt = date.today() - timedelta(days=minusDay + 1)
            print('migration target at ' + str(targetAt) + ' period ' + str(minusDay + 1) + '/' + str(dayLimit))
            self.run(stock, targetAt, period, True)



    def getNewItem(self, stockCode):
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        insert = ds.getStock(stockCode)
        datas = ds.getChartDataList(insert.get('code'), 365 * 2)
        ds.insertFinanceData(datas, str(insert.get('id')))
        ds.finalize()
    def getDivideNum(self, num1, num2) :
        if num2 == 0 :
            return 0
        return num1 / num2



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
            targetAt = each.get('targetAt')
            if total > 0 :
                if resultPrice > 0:  # plus
                    plusPoint.append({'name': stockName, 'result': resultPrice, 'point': plus / total, 'avg': plusAvg, 'targetAt': str(targetAt)})
                else:
                    minusPoint.append({'name': stockName, 'result': resultPrice, 'point': minus / total,'avg': minusAvg, 'targetAt': str(targetAt)})
        # for each in plusPoint :
        #     print('plus' + str(each))
        # for each in minusPoint :
        #     print('minus' + str(each))
        # for each in analyzedResult :
        #     print('name:',each.get('name'),'plus point :',each.get('plus'),'minus point :',each.get('minus'), 'compare :', self.getDivideNum(each.get('plus'), each.get('minus')))

        for each in plusPoint :
            print (each.get('name'), 'plus',each.get('point'))
        for each in minusPoint :
            print (each.get('name'), 'minus',each.get('point'))
        for each in analyzedResult :
            print(each.get('name'), 'plus',each.get('plus'),'minus',each.get('minus'), 'point', each.get('plus')/each.get('plus')+each.get('minus'))





        # for each in forecastResult :
        #     print('plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] result  [' +  str(each.get('final') - each.get('start')) + '] target  : ' +  str(each.get('targetAt')))
        # for each in analyzedResult:
        #     print(each.get('name') + ' plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] target  : ' +  str(each.get('targetAt')))




DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"
PPOMPPU_ACC = { 'id': "", 'pwd' : ""}
busy = False
runner = runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC)

dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
stocks = dbm.getUsefulStockList()
period = 2
targetAt = date.today()
# code here
import time

t1 = time.time() # start time
for stock in stocks :
#    runner.printForecastData(analyzed, forecast)
   runner.run(stock, targetAt, period, busy)

t2 = time.time() # end time
print(str(t2-t1)) # analyzed, forecast = dbm.analyzedSql(stock.get('name'))


    # runner.getNewItem(each)
migrationDuration = 365
#for stock in stocks :
 #   runner.migration(stock, period, migrationDuration)



