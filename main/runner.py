import dbmanager
import analyzer
import miner
import webscrap
import numpy
import stockscrap
from datetime import date, timedelta
import pythoncom
import sys
import dictionary

class Runner:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.FILTER_LIMIT = 60
        self.CHANCE_PERCENT = 0.10
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.PPOMPPU_ID = ''
        self.PPOMPPU_PWD = ''
        pythoncom.CoInitialize()
        self.stocks = None
        self.dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        self.analyze = analyzer.Analyzer(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        self.dic = None
    def setStocks(self):
        if self.stocks is None :
            self.stocks = stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH)
    def __del__(self):
        self.dbm.commit()
        self.analyze.commit()
        if self.stocks is not None:
            self.stocks.commit()

    def insertFinance(self, stock):
        stockCode = stock.get('code')
        stockId = stock.get('id')
        self.setStocks()
        datas = self.stocks.getChartDataList(stockCode, 365 * 2)
        self.stocks.insertFinanceData(datas, str(stockId))


    def insertPpomppuResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockName = stock.get('name')
        ppomppu = webscrap.Ppomppu()
        ppomppuResult = ppomppu.getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName,
                                         lastUseDateAt)  # id , password , search
        self.dbm.saveData(ppomppu.SITE, ppomppuResult, stockName)
        self.dbm.commit()

    def insertPaxnetResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnet = webscrap.Paxnet()
        paxnetResult = paxnet.getTrendByCode(stockCode, lastUseDateAt)
        self.dbm.saveData(paxnet.SITE, paxnetResult, stockName)
        self.dbm.commit()

    def insertNaverResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastUseDateAt)
        self.dbm.saveData(ns.SITE, naverResult, stockName)
        self.dbm.commit()

    def insertDaumResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ds = webscrap.DaumStock()
        daumResult = ds.getTrendByCode(stockCode, lastUseDateAt)
        self.dbm.saveData(ds.SITE, daumResult, stockName)
        self.dbm.commit()

    def insertAnalyzedResult(self, stock, targetAt, period):
        stockName = stock.get('name')
        forecastAt = targetAt + timedelta(days=period)
        if self.dbm.forecastTarget(forecastAt, stock, targetAt):
            return
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        # plusCnt, minusCnt, totalPlusCnt, totalMinusCnt =  mine.getAnalyzedCnt(targetAt, period, stockName)
        targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList = mine.getAnalyzedCnt(targetAt, period, stockName)

        savedItemId = self.dbm.saveAnalyzedData(stockName, targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt,forecastAt, period)
        self.dbm.saveAnalyzedItemFinanceList(savedItemId, targetFinanceIdList)
        self.dbm.commit()
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

            self.analyze.analyze()
            self.analyze.commit()
            self.dbm.updateLastUseDate(stock)
            self.dbm.commit()
        self.insertAnalyzedResult(stock, targetAt, period)

    def update(self, stock):
        self.dbm.updateAnalyzedResultItem(stock)
        self.dbm.commit()

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
        for stock in self.dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
            filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period)
            filteredTargets = filteredTargetList + filteredTargets
        self.printFilteredTarget(filteredTargets)


    def getFinanceDataMap(self, financeIdList):
        chanceIds = []
        dangerIds = []
        prices = []
        financeDataList = self.dbm.getFinanceDataIn(financeIdList)
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
        self.setStocks()
        self.stocks.insertNewStock(stockCode)
        stock = self.stocks.getStock(stockCode)
        self.scrapWebAndMigration(stock, period)
    def scrapWebAndMigration(self, stock, period):
        self.run(stock, date.today(), period, False)
        self.migration(stock, period, 365)

    def filteredTarget(self, period):
        filteredTargets = ''
        for stock in self.dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
            filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period)
            if len(filteredTargetList) > 0 :
                filteredTargets = filteredTargets + str(filteredTargetList) + '\n'
        return filteredTargets

    def getAnalyzeExistData(self, stock):
        analyzedResult = self.dbm.analyzedSql(stock.get('name'))
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
            financeList = self.dbm.getFinanceListFromItemId(itemId)
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
        forecastResult = self.dbm.getForecastResult(stock.get('name'), date.today() - timedelta(days=period + 1))
        for each in forecastResult:
            itemId, point, stockName, targetAt = self.getFilteredForecastResult(each)

            if point > self.FILTER_LIMIT:
                financeList = self.dbm.getFinanceListFromItemId(itemId)
                chanceIds = []
                for chanceId in plusChanceIds:
                    if chanceId in financeList:
                        chanceIds.append(chanceId)
                filteredTargets.append({stockName: point, 'point': pointDict.get(stockName), 'targetAt': targetAt, 'chance': chanceIds})
        return filteredTargets

    def getFilteredForecastResult(self, each):
        plus = each.get('plus')
        minus = each.get('minus')
        point = self.getDivideNumPercent(plus, plus + minus)
        stockName = each.get('name')
        targetAt = each.get('targetAt')
        return each.get('id'), point, stockName, targetAt

    def trustedTarget(self):
        trustedTargetPoints = ''
        for stock in self.dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
            target = pointDict.get(stock.get('name'))
            percent = target.get('percent')
            if self.FILTER_LIMIT < percent :
                print('trust target', target)
                forecastResult = self.dbm.getForecastResult(stock.get('name'), date.today())
                filteredResults = []
                for each in forecastResult :
                    itemId, point, stockName, targetAt = self.getFilteredForecastResult(each)
                    filteredResults.append({'point': point,'targetAt': targetAt})

                trustedTargetPoints = trustedTargetPoints + str({'pointDict': pointDict, 'forecast': filteredResults}) + '\n'
        return trustedTargetPoints

    def dailyRun(self, period):
        while True :
            try :
                stock = self.dbm.getUsefulStock(True)
                print(stock.get('name'), 'is start')

                busy = False
                targetAt = date.today()
                self.run(stock, targetAt, period, busy)
                print(stock.get('name'), 'is done')
            except dbmanager.DBManagerError :
                print('work is done.')
                break
            except :
                print("unexpect error.", sys.exc_info())
                break


    def targetAnalyze(self, stockCode, period):
        self.setStocks()
        stock = self.stocks.getStock(stockCode)
        if stock is None :
            return None
        print('targetAnalyze', stock)
        plusChanceIds, pointDict = self.getAnalyzeExistData(stock)
        filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period)
        return filteredTargetList



    def printFilteredTarget(self, filteredTargets):
        print('FILTERED TARGET')
        for goodDic in filteredTargets:
            print(goodDic)

    def initStocks(self):
        self.dbm.initStock()

    def migrateStocks(self, period):
        while True :
            try :
                stock = self.dbm.getUsefulStock(False)
                print(stock.get('name'), 'is start')
                self.migration(stock, period, 365 * 2)
                print(stock.get('name'), 'is done')
            except dbmanager.DBManagerError :
                print('work is done.')
                break
            except :
                print("unexpect error.", sys.exc_info())
                break

    def getGarbageWord(self):
        garbage = self.dbm.getUnfilterdGarbageWord()
        return str(garbage.get('id')) + ' ' + garbage.get('word')

    def updateGarbageStatus(self, garbageId, process):
        self.dbm.updateGarbageStatus(garbageId, process)

    def getUndefinedGarbageWord(self):
        return self.dbm.getUnfilterdGarbageWord()

    def newDictionaryInstance(self):
        if self.dic is None :
            self.dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        return self.dic
    def garbageRecycle(self, maxId):
        data = self.dbm.getUnfilterdGarbageWord()
        if data is None or data.get('id') == maxId :
            raise dbmanager.DBManagerError('garbage is none' + str(maxId))
        word = data.get('word')
        dic = self.newDictionaryInstance()
        wordId = dic.getWordByStr(word)
        if wordId != '' :
            print('update garbage recycle', data.get('word'), data.get('id'), maxId)
            self.dbm.updateGarbageStatus(data.get('id'), 'Y')
            self.dbm.commit()
        pass
    def getMaxGarbageId(self):
        return self.dbm.getMaxGarbageWord()
    def updateGarbageAndInsertWord(self, garbageId, usefulWord):
        garbageWord = self.dbm.getGarbageWord(garbageId)
        garbage = garbageWord.get('word')
        if usefulWord is None :
            usefulWord = garbage
        if usefulWord in garbage:
            self.dbm.updateGarbageStatus(garbageId, 'Y')
            return self.dbm.insertWord(usefulWord)
        return False


DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

period = 2
run = Runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
# run.initStocks()
max = run.getMaxGarbageId()
while True :
    try :
        stock = run.dbm.getUsefulStock(True)
        print(stock.get('name'), 'is start')
        run.run(stock, date.today(), period, False)
        print(stock.get('name'), 'is done')
        # run.garbageRecycle(max.get('id'))
    except dbmanager.DBManagerError :
        print('work is done.')
        break

