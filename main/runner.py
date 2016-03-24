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
        filteredTargets = []
        for stock in dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
            filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period)
            filteredTargets = filteredTargetList + filteredTargets
        self.printFilteredTarget(filteredTargets)


    def getFinanceDataMap(self, financeIdList):
        chanceIds = []
        dangerIds = []
        prices = []
        financeDataList = dbm.getFinanceDataIn(financeIdList)
        for finance in financeDataList:
            price = finance.get('start') - finance.get('final')
            compare = finance.get('final') - price
            percent = (price / compare)
            if percent > self.CHANCE_PERCENT:
                chanceIds.append(finance.get('id'))
            if percent < -self.CHANCE_PERCENT:
                dangerIds.append(finance.get('id'))
            prices.append(price)
        avg = 0
        if len(prices) > 0 :
            avg = numpy.mean(prices)
        return {'avg': avg, 'chance': chanceIds, 'danger': dangerIds}

    def insertNewStockAndMigrate(self, stockCode, period):
        stocks.insertNewStock(stockCode)
        stock = stocks.getStock(stockCode)
        self.scrapWebAndMigration(stock, period)
    def scrapWebAndMigration(self, stock, period):
        runner.run(stock, date.today(), period, False)
        runner.migration(stock, period, 365)

    def filteredTarget(self, period):
        filteredTargets = []
        for stock in dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
            filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period)
            filteredTargets = filteredTargets + filteredTargetList
        return filteredTargets

    def getAnalyzeExistData(self, stock):
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
            financeMap = self.getFinanceDataMap(financeList)  # avg chance danger

            plusPercent = self.getDivideNumPercent(plus, total)
            minusPercent = self.getDivideNumPercent(minus, total)

            if (total > 0 and resultPrice != 0) and not (plusPercent == 0 or minusPercent == 0):
                sumPoint.append(
                    {'name': stockName, 'result': resultPrice, 'plus_point': plusPercent, 'minus_point': minusPercent,
                     'plus': plus, 'minus': minus, 'targetAt': str(targetAt)})
                if resultPrice > 0:  # plus
                    plusPoint.append(
                        {'name': stockName, 'result': resultPrice, 'point': plusPercent, 'targetAt': str(targetAt),
                         'financeMap': financeMap})
                else:
                    minusPoint.append(
                        {'name': stockName, 'result': resultPrice, 'point': minusPercent, 'targetAt': str(targetAt)})
        plusChanceIds = []
        for point in plusPoint:
            chanceIds = point.get('financeMap').get('chance')
            plusChanceIds += chanceIds
            plusChanceIds = list(set(plusChanceIds))

        trustPercent = self.getDivideNumPercent(len(plusPoint), len(sumPoint))
        pointDict = {stock.get('name'): {'percent': trustPercent, 'total': len(analyzedResult)}}
        return plusChanceIds, pointDict


    def getFilteredTarget(self, plusChanceIds, pointDict, stock, period):
        filteredTargets = []
        forecastResult = dbm.getForecastResult(stock.get('name'), date.today() - timedelta(days=period + 1))
        for each in forecastResult:
            plus = each.get('plus')
            minus = each.get('minus')
            point = self.getDivideNumPercent(plus, plus + minus)
            stockName = each.get('name')
            targetAt = each.get('targetAt')

            if point > self.FILTER_LIMIT:
                itemId = each.get('id')
                financeList = dbm.getFinanceListFromItemId(itemId)
                chanceIds = []
                for chanceId in plusChanceIds:
                    if chanceId in financeList:
                        chanceIds.append(chanceId)
                filteredTargets.append({stockName: point, 'point': pointDict.get(stockName), 'targetAt': targetAt, 'chance': chanceIds})
        return filteredTargets

    def trustedTarget(self):
        trustedTargets = []
        for stock in dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
            target = pointDict.get(stock.get('name'))
            percent = target.get('percent')
            if self.FILTER_LIMIT < percent :
                trustedTargets.append(target)
        return trustedTargets

    def dailyRun(self, period):
        while True :
            stock = dbm.getUsefulStock(True)
            busy = False
            targetAt = date.today()
            runner.run(stock, targetAt, period, busy)

    def targetAnalyze(self, stockCode):
        stock = stocks.getStock(stockCode)
        print('targetAnalyze', stock)
        plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
        filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period)
        return filteredTargetList



    def printFilteredTarget(self, filteredTargets):
        print('FILTERED TARGET')
        for goodDic in filteredTargets:
            print(goodDic)


DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

runner = runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
stocks = stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH)
period = 2

# 1. filteredTarget
# 2. trustedTarget
# 3. dailyRun
# 4. insertNewAndMigrate
# 5. targetAnalyze
# filteredTargets = runner.filteredTarget(period)
# trustedTargets = runner.trustedTarget()
# runner.dailyRun(period)
# runner.insertNewStockAndMigrate('011780', period)
# runner.targetAnalyze('011780')


# dbm.initStock()
# while True : runner.migration(dbm.getUsefulStock(True), period, 365)
# while True : runner.run(dbm.getUsefulStock(True), date.today(), period, False)
# while True : runner.scrapWebAndMigration(dbm.getUsefulStock(True), period)
# runner.printForecastData(period)

# analyzer.Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH).analyze()
# runner.insertNewStock('011780')

# dbm.close()
