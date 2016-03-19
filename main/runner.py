import stockscrap
import dbmanager
import analyzer
import miner
import webscrap
import numpy
from datetime import date, timedelta


class runner:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.FILTER_LIMIT = 60
        self.CHANCE_PERCENT = 0.10
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.PPOMPPU_ID = ''
        self.PPOMPPU_PWD = ''

    def insertFinance(self, stock):
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
        ppomppuResult = ppomppu.getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName,
                                         lastUseDateAt)  # id , password , search
        saveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        saveDbm.saveData(ppomppu.SITE, ppomppuResult, stockName)
        saveDbm.commit()

    def insertPaxnetResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnet = webscrap.Paxnet()
        paxnetResult = paxnet.getTrendByCode(stockCode, lastUseDateAt)
        paxnetSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        paxnetSaveDbm.saveData(paxnet.SITE, paxnetResult, stockName)
        paxnetSaveDbm.commit()

    def insertNaverResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastUseDateAt)
        naverSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        naverSaveDbm.saveData(ns.SITE, naverResult, stockName)
        naverSaveDbm.commit()

    def insertDaumResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ds = webscrap.DaumStock()
        daumResult = ds.getTrendByCode(stockCode, lastUseDateAt)
        daumSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        daumSaveDbm.saveData(ds.SITE, daumResult, stockName)
        daumSaveDbm.commit()

    def insertAnalyzedResult(self, stock, targetAt, period):
        stockName = stock.get('name')
        forecastAt = targetAt + timedelta(days=period)
        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        if stockDbm.forecastTarget(forecastAt, stock, targetAt):
            return
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        # plusCnt, minusCnt, totalPlusCnt, totalMinusCnt =  mine.getAnalyzedCnt(targetAt, period, stockName)
        targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList = mine.getAnalyzedCnt(targetAt, period, stockName)
        mine.close()
        savedItemId = stockDbm.saveAnalyzedData(stockName, targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt,forecastAt, period)
        stockDbm.saveAnalyzedItemFinanceList(savedItemId, targetFinanceIdList)
        stockDbm.commit()
        stockDbm.close()
        self.update(stock)

    def run(self, stock, targetAt, period, busy):
        if stock is None:
            return
        if busy is False:
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

    def update(self, stock):
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        runDbm.updateAnalyzedResultItem(stock)
        runDbm.commit()
        runDbm.close()

    def migration(self, stock, period, dayLimit):
        print('migration', stock.get('name'))
        for minusDay in range(dayLimit - 1):
            targetAt = date.today() - timedelta(days=minusDay + 1)
            print('migration target at ', targetAt, 'period ', minusDay + 1, '/')
            self.run(stock, targetAt, period, True)

    def getDivideNumPercent(self, num1, num2):
        if num2 == 0:
            return 0
        return int((num1 / num2) * 100)

    def printForecastData(self, period):
        filteredTarget = []

        for stock in dbm.getStockList():
            analyzedResult = dbm.analyzedSql(stock.get('name'))
            plusPoint = []
            minusPoint = []
            sumPoint = []
            for each in analyzedResult:
                resultPrice = each.get('final') - each.get('start')
                plus = each.get('plus')
                minus = each.get('minus')
                total = plus + minus
                stockName = each.get('name')
                targetAt = each.get('targetAt')
                itemId = each.get('id')
                financeList = dbm.getFinanceListFromItemId(itemId)
                financeMap = self.getFinanceDataMap(financeList) # avg chance danger

                plusPercent = self.getDivideNumPercent(plus, total)
                minusPercent = self.getDivideNumPercent(minus, total)

                if (total > 0 and resultPrice != 0) and not (plusPercent == 0 or minusPercent == 0):
                    sumPoint.append({'name': stockName, 'result': resultPrice, 'plus_point': plusPercent, 'minus_point': minusPercent, 'plus': plus, 'minus': minus, 'targetAt': str(targetAt)})
                    if resultPrice > 0:  # plus
                        plusPoint.append({'name': stockName, 'result': resultPrice, 'point': plusPercent, 'targetAt': str(targetAt), 'financeMap': financeMap})
                    else:
                        minusPoint.append({'name': stockName, 'result': resultPrice, 'point': minusPercent, 'targetAt': str(targetAt)})
            plusChanceIds = []
            for point in plusPoint :
                chanceIds = point.get('financeMap').get('chance')
                plusChanceIds += chanceIds
                plusChanceIds = list(set(plusChanceIds))

            pointDict = {stock.get('name'): {'percent': (self.getDivideNumPercent(len(plusPoint), len(sumPoint))), 'total':len(analyzedResult)}}
            print(pointDict)

            forecastResult = dbm.getForecastResult(stock.get('name'), date.today() - timedelta(days=period + 1))
            for each in forecastResult :
                plus = each.get('plus')
                minus = each.get('minus')
                point = self.getDivideNumPercent(plus, plus + minus)
                stockName = each.get('name')
                targetAt = each.get('targetAt')


                if point > self.FILTER_LIMIT :
                    itemId = each.get('id')
                    financeList = dbm.getFinanceListFromItemId(itemId)
                    chanceIds = []
                    for chanceId in plusChanceIds :
                        if chanceId in financeList :
                            chanceIds.append(chanceId)
                    filteredTarget.append({stockName: point, 'point': pointDict.get(stockName), 'targetAt': targetAt, 'chance' : chanceIds})


                print('forecast', stockName, 'targetAt', targetAt, 'plus', plus, 'minus', minus, 'percent', point)

        print('FILTERED TARGET')
        for goodDic in filteredTarget:
            print(goodDic)

    def getFinanceDataMap(self, financeIdList):
        chanceIds = []
        dangerIds = []
        prices = []
        financeDataList = dbm.getFinanceDataIn(financeIdList)
        for finance in financeDataList:
            price = finance.get('start') - finance.get('final')
            compare = price - finance.get('final')
            percent = (price / compare)
            if percent > self.CHANCE_PERCENT:
                chanceIds.append(finance.get('id'))
            if percent < -self.CHANCE_PERCENT:
                dangerIds.append(finance.get('id'))
            prices.append(price)
        return {'avg': numpy.mean(prices), 'chance': chanceIds, 'danger': dangerIds}



DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

runner = runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
period = 2

# dbm.initStock()


# while True : runner.migration(dbm.getUsefulStock(True), period, 365)
# while True : runner.run(dbm.getUsefulStock(True), date.today(), period, False)
runner.printForecastData(period)

# analyzer.Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH).analyze()
# stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH).insertNewStock('011780')
dbm.close()
