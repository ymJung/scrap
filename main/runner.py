import stockscrap
import dbmanager
import analyzer
import miner
import webscrap
from datetime import date, timedelta

class runner :
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH) :
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.PPOMPPU_ID = ''
        self.PPOMPPU_PWD = ''

    def insertFinance(self, stock) :
        stockCode = stock.get('code')
        stockId = stock.get('id')
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        datas = ds.getChartDataList(stockCode, 365 * 2)
        ds.insertFinanceData(datas, str(stockId))
        ds.commit()
    def insertPpomppuResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockName = stock.get('name')
        ppomppu = webscrap.Ppomppu()
        ppomppuResult = ppomppu.getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName, lastUseDateAt)  # id , password , search
        saveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        saveDbm.saveData(ppomppu.SITE, ppomppuResult, stockName)
        saveDbm.commit()
    def insertPaxnetResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnet = webscrap.Paxnet()
        paxnetResult = paxnet.getTrendByCode(stockCode, lastUseDateAt)
        paxnetSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        paxnetSaveDbm.saveData(paxnet.SITE,  paxnetResult, stockName)
        paxnetSaveDbm.commit()
    def insertNaverResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastUseDateAt)
        naverSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        naverSaveDbm.saveData(ns.SITE, naverResult, stockName)
        naverSaveDbm.commit()
    def insertDaumResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ds = webscrap.DaumStock()
        daumResult = ds.getTrendByCode(stockCode, lastUseDateAt)
        daumSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        daumSaveDbm.saveData(ds.SITE, daumResult, stockName)
        daumSaveDbm.commit()
    def insertAnalyzedResult(self, stock, targetAt, period) :
        stockName = stock.get('name')
        forecastAt = targetAt + timedelta(days=period)
        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        if stockDbm.existForecastDate(forecastAt, stock.get('id')) :
            print('exist forecast date', forecastAt)
            return
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        contents = mine.getStockNameContent(stock.get('name'), today, today - timedelta(days=365))
        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg = mine.getAnalyzedCnt(targetAt, period, stockName, contents)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
        mine.close()
        print(str(result))
        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, forecastAt, targetPlusAvg, targetMinusAvg, period)
        stockDbm.commit()
        stockDbm.close()
        self.update(stock)


    def run(self, stock, targetAt, period, busy):
        if stock is None :
            return
        if busy is False :
            self.insertFinance(stock)
            # self.insertPpomppuResult(stock)
            self.insertPaxnetResult(stock)
            self.insertNaverResult(stock)
            self.insertDaumResult(stock)
            anal = analyzer.Analyzer(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
            anal.analyze()
            anal.commit()
            upd = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
            upd.updateLastUseDate(stock)
            upd.commit()
            upd.close()
        self.insertAnalyzedResult(stock, targetAt, period)


    def update(self, stock) :
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        runDbm.updateAnalyzedResultItem(stock)
        runDbm.commit()
        runDbm.close()

    def migration(self, stock, period, dayLimit):
        print('migration', stock.get('name'))
        for minusDay in range(dayLimit - 1):
            targetAt = date.today() - timedelta(days=minusDay + 1)
            print('migration target at ', targetAt, 'period ',minusDay + 1, '/')
            self.run(stock, targetAt, period, True)

    def getDivideNumPercent(self, num1, num2) :
        if num2 == 0 :
            return 0
        return int((num1 / num2) * 100)



    def printForecastData(self, input):
        plusPoint = []
        minusPoint = []
        sumPoint = []
        for each in input.get('analyzed'):
            resultPrice = each.get('final') - each.get('start')
            plus = each.get('plus')
            plusAvg = each.get('plusAvg')
            minus = each.get('minus')
            total = plus + minus
            stockName = each.get('name')
            minusAvg = each.get('minusAvg')
            targetAt = each.get('targetAt')

            plusPercent = self.getDivideNumPercent(plus, total)
            minusPercent = self.getDivideNumPercent(minus, total)

            if (total > 0 and resultPrice != 0) and not (plusPercent == 0 or minusPercent == 0):
                sumPoint.append({'name': stockName, 'result': resultPrice, 'plus_point': plusPercent, 'minus_point': minusPercent, 'plus_avg': plusAvg, 'minus_avg':minusAvg, 'plus':plus, 'minus':minus, 'targetAt': str(targetAt)})
                if resultPrice > 0:  # plus
                    plusPoint.append({'name': stockName, 'result': resultPrice, 'point': plusPercent, 'avg': plusAvg, 'targetAt': str(targetAt)})
                else:
                    minusPoint.append({'name': stockName, 'result': resultPrice, 'point': minusPercent, 'avg': minusAvg, 'targetAt': str(targetAt)})

        # for each in plusPoint : print (each.get('name'), 'plus',each.get('point'), 'targetAt', each.get('targetAt'), 'result', each.get('result'))
        print(input.get('name'), self.getDivideNumPercent(len(plusPoint), len(sumPoint)))
        # for each in minusPoint :
        #     print (each.get('name'), 'minus',each.get('point'), 'targetAt', each.get('targetAt'), 'result', each.get('result'))
        # for each in sumPoint : print (each.get('name'), 'plus',each.get('plus_point'), 'minus', each.get('minus_point'), 'targetAt', each.get('targetAt'), 'result', each.get('result'), 'plus', each.get('plus'), 'minus', each.get('minus'))
        for each in input.get('forecast') : print('forecast', each.get('name'), 'targetAt',each.get('targetAt'), 'plus', each.get('plus'),'minus', each.get('minus'), 'point', self.getDivideNumPercent(each.get('plus'), each.get('plus') + each.get('minus')))





        # for each in forecastResult :
        #     print('plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] result  [' +  str(each.get('final') - each.get('start')) + '] target  : ' +  str(each.get('targetAt')))
        # for each in analyzedResult:
        #     print(each.get('name') + ' plus[' + str(each.get('plus')) + '] avg [' + str(each.get('plusAvg')) + '] minus[' + str(each.get('minus')) + '] avg [' + str(each.get('minusAvg')) + '] target  : ' +  str(each.get('targetAt')))




DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

runner = runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
period = 2
today = date.today()
# dbm.initStock()

#while True : runner.migration(dbm.getUsefulStock(True), period, 365)
while True : runner.run(dbm.getUsefulStock(True), today, period, False)
# for stock in dbm.getStockList() : runner.printForecastData(dbm.analyzedSql(stock.get('name')))
# analyzer.Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH).analyze()
# stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH).insertNewStock('012280')
dbm.close()




