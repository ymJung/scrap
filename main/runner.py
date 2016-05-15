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
import simulator


class Runner:
    def __init__(self):
        self.GUARANTEE_COUNT = 150
        self.FILTER_LIMIT = 50
        self.FILTER_TARGET_LIMIT = 70
        self.CHANCE_PERCENT = 0.10
        self.PPOMPPU_ID = ''
        self.PPOMPPU_PWD = ''
        pythoncom.CoInitialize()
        self.stocks = None
        self.dbm = dbmanager.DBManager()
        self.analyze = analyzer.Analyzer()
        self.simul = simulator.Simulator()
        self.dic = None

    def insertFinance(self, stock):
        stockCode = stock.get('code')
        stockId = stock.get('id')
        todayDate = date.today()
        financeDate = self.dbm.selectLastestFinance(stock.get('id')).date()
        if (financeDate == todayDate) : #or datetime.datetime.now().hour > self.dbm.LIMIT_HOUR : #  fix.
            print('not in target')
        else :
            stocks = self.getStocks()
            datas = stocks.getChartDataList(stockCode, 365 * 2)
            self.stocks.insertFinanceData(datas, str(stockId))
    def getStocks(self):
        if self.stocks is None :
            self.stocks = stockscrap.DSStock()
            self.stocks.updateStockInfo()
            self.updateAllStockFinance()

        return self.stocks

    def __del__(self):
        self.dbm.commit()
        self.analyze.commit()
        if self.stocks is not None:
            self.stocks.dbm.commit()


    def insertPpomppuResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockName = stock.get('name')
        ppomppu = webscrap.Ppomppu()
        ppomppuResult = ppomppu.getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName, lastScrapAt)  # id , password , search
        self.dbm.saveData(ppomppu.SITE, ppomppuResult, stockName, stock.get('id'))
        self.dbm.commit()

    def insertPaxnetResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnet = webscrap.Paxnet()
        paxnetResult = paxnet.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(paxnet.SITE, paxnetResult, stockName, stock.get('id'))
        self.dbm.commit()

    def insertNaverResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ns.SITE, naverResult, stockName, stock.get('id'))
        self.dbm.commit()

    def insertDaumResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ds = webscrap.DaumStock()
        daumResult = ds.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ds.SITE, daumResult, stockName, stock.get('id'))
        self.dbm.commit()

    def insertAnalyzedResult(self, stock, targetAt, period):
        stockName = stock.get('name')
        forecastAt = targetAt + timedelta(days=period)
        if self.dbm.forecastTarget(forecastAt, stock, targetAt, period):
            return
        mine = miner.Miner()
        targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList = mine.getAnalyzedCnt(targetAt, period, stockName, stock.get('id'))
        savedItemId = self.dbm.saveAnalyzedData(stockName, targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt,forecastAt, period)
        self.dbm.saveAnalyzedItemFinanceList(savedItemId, targetFinanceIdList)
        self.dbm.updateAnalyzedResultItem(stock)
        self.dbm.commit()

    def run(self, stock, targetAt, period):
        if stock is None :
            print("target unexist.")
            return
        self.insertFinance(stock)
        self.insertAnalyzedResult(stock, targetAt, period)

    def migrationWork(self, period):
        for stock in self.dbm.getStockList() :
            targetDate = self.getTargetDate(stock.get('id'), period)
            limitDate = self.getFirstContentDate(stock.get('id'))
            print('migration', stock.get('name'), targetDate, limitDate)
            if limitDate is None :
                print('content is none.')
                continue
            idx = 0
            while True :
                idx += 1
                targetAt = targetDate - timedelta(days=idx)
                if limitDate > targetDate :
                    print('done', stock.get('name'), limitDate)
                    break
                print('migration target at ', targetAt, 'period ', idx, '/')
                self.run(stock, targetAt, period)

    def migration(self, stock, period):
        print('migration', stock.get('name'))
        targetDate = self.getTargetDate(stock.get('id'), period)
        limitDate = self.getFirstContentDate(stock.get('id'))
        idx = 0
        while True :
            idx += 1
            targetAt = targetDate - timedelta(days=idx)
            if limitDate > targetAt :
                print('done', stock.get('name'), limitDate)
                break
            if self.dbm.checkHolyDay(targetAt) :
                continue
            print('migration target at ', targetAt, 'period ', idx, '/')
            self.run(stock, targetAt, period)
        # for minusDay in range(dayLimit - 1):

    def getDivideNumPercent(self, num1, num2):
        if num2 == 0:
            return 0
        return int((num1 / num2) * 100)

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

    def insertNewStockScrap(self, stockCode):
        stock = self.dbm.selectStockByCode(stockCode)
        if stock is None :
            stocks = self.getStocks()
            stock = stocks.insertNewStock(stockCode)
        else :
            self.dbm.updateStockMuch(stock.get('id'), 0)
        # self.insertPpomppuResult(stock)
        self.insertPaxnetResult(stock)
        self.insertNaverResult(stock)
        self.insertDaumResult(stock)
        self.insertKakaoResult(stock)
        # self.analyze.analyze()
        self.dbm.commit()
        # self.run(stock, date.today(), period)
        # self.migration(stock, period)

    def filteredTarget(self, period, limitAt):
        targetList = list()
        filterdList = list()
        for stock in self.dbm.getStockList():
            plusChanceIds, pointDict = self.getAnalyzeExistData(stock.get('name'), period)
            filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period, limitAt)
            if len(filteredTargetList) > 0 :
                print(filteredTargetList)
                for filter in filteredTargetList :
                    if filter.get(stock.get('name')) > self.FILTER_LIMIT and filter.get('potential') > self.FILTER_LIMIT:
                        filterdList.append(filter)
                targetList.append(filteredTargetList)
        print(targetList)
        print('print', filterdList)
        return targetList

    def getAnalyzeExistData(self, stockName, period):
        analyzedResult = self.dbm.analyzedSql(stockName, period)
        plusPoint = []
        minusPoint = []
        sumPoint = []
        for each in analyzedResult:
            resultPrice = each.get('final') - each.get('start')
            plus = each.get('plus')
            minus = each.get('minus')
            total = plus + minus
            plusPercent = self.getDivideNumPercent(plus, total)
            minusPercent = self.getDivideNumPercent(minus, total)
            targetAt = each.get('targetAt')
            itemId = each.get('id')
            financeList = self.dbm.getFinanceListFromItemId(itemId)
            financeMap = self.getFinanceDataMap(financeList)
            if (total > 0 and resultPrice != 0) and not (plusPercent == 0 or minusPercent == 0):
                sumPoint.append({'name': stockName, 'result': resultPrice, 'plus_point': plusPercent, 'minus_point': minusPercent,'plus': plus, 'minus': minus, 'targetAt': str(targetAt)})
                if resultPrice >= 0:  # plus or 0
                    plusPoint.append({'name': stockName, 'result': resultPrice, 'point': plusPercent, 'targetAt': str(targetAt),'financeMap': financeMap})
                else:
                    minusPoint.append({'name': stockName, 'result': resultPrice, 'point': minusPercent, 'targetAt': str(targetAt)})
        plusChanceIds = []
        for point in plusPoint:
            chanceIds = point.get('financeMap').get('chance')
            plusChanceIds += chanceIds
            plusChanceIds = list(set(plusChanceIds))

        trustPercent = self.getDivideNumPercent(len(plusPoint), len(sumPoint))
        pointDict = {stockName: {'name': stockName, 'potential': trustPercent, 'total': len(analyzedResult)}}
        return plusChanceIds, pointDict


    def getFilteredTarget(self, plusChanceIds, pointDict, stock, period, startAt):
        filteredTargets = []
        forecastResult = self.dbm.getForecastResult(stock.get('name'), startAt, period)
        for each in forecastResult:
            itemId, point, stockName, targetAt = self.getFilteredForecastResult(each)
            # if point > self.FILTER_TARGET_LIMIT:
            analyzedTargetData = self.getAnalyzedTarget(itemId, plusChanceIds, point, pointDict, stockName, targetAt)
            filteredTargets.append(analyzedTargetData)
        return filteredTargets
    def getForecastTarget(self, plusChanceIds, pointDict, stock, period):
        results = list()
        forecastResults = self.dbm.getForecastResult(stock.get('name'), date.today(), period)
        for forecastResult in forecastResults:
            itemId, point, stockName, targetAt = self.getFilteredForecastResult(forecastResult)
            analyzedTargetData = self.getAnalyzedTarget(itemId, plusChanceIds, point, pointDict, stockName, targetAt)
            results.append(analyzedTargetData)
        return results


    def getAnalyzedTarget(self, itemId, plusChanceIds, point, pointDict, stockName, targetAt):
        financeList = self.dbm.getFinanceListFromItemId(itemId)
        chanceIds = []
        for chanceId in plusChanceIds:
            if chanceId in financeList:
                chanceIds.append(chanceId)
        return {stockName: point, 'potential': pointDict.get(stockName).get('potential'),'total': pointDict.get(stockName).get('total'), 'targetAt': targetAt.day,'chance': chanceIds}

    def getFilteredForecastResult(self, each):
        plus = each.get('plus')
        minus = each.get('minus')
        point = self.getDivideNumPercent(plus, plus + minus)
        stockName = each.get('name')
        targetAt = each.get('targetAt')
        return each.get('id'), point, stockName, targetAt

    def filterPotentialStock(self, period):
        for stock in self.dbm.getAllStockList():
            self.updatePotentialStock(stock)
            poten = self.dbm.selectPotentialStock(stock.get('id'), period)
            if poten.get('count') > self.GUARANTEE_COUNT and poten.get('potential') < self.FILTER_LIMIT and stock.get('much') == 0 :
                self.dbm.updateStockMuch(stock.get('id'), 1)
                print(stock, ' set much 1. ', poten.get('potential'))

    def dailyRun(self, period, after):
        targetAt = date.today() + timedelta(days=after)
        while True :
            try :
                stock = self.dbm.getUsefulStock(targetAt)
                print(stock.get('name'), 'is start', targetAt)
                self.run(stock, targetAt, period)
                print(stock.get('name'), 'is done', targetAt)
            except dbmanager.DBManagerError :
                print('work is done.')
                break
            except :
                print("unexpect error.", sys.exc_info())
                break


    def targetAnalyze(self, stockCode, period):
        stock = self.dbm.selectStockByCode(stockCode)
        print('targetAnalyze', stock)
        plusChanceIds, pointDict = self.getAnalyzeExistData(stock.get('name'), period)
        filteredTargetList = self.getForecastTarget(plusChanceIds, pointDict, stock, period, 0)
        print(filteredTargetList)
        return filteredTargetList

    def printFilteredTarget(self, filteredTargets):
        print('FILTERED TARGET')
        for goodDic in filteredTargets:
            print(goodDic)

    def initStocks(self):
        self.dbm.initStock()

    def getGarbageWord(self):
        garbage = self.dbm.getUnfilterdGarbageWord()
        return str(garbage.get('id')) + ' ' + garbage.get('word')

    def updateGarbageStatus(self, garbageId, process):
        self.dbm.updateGarbageStatus(garbageId, process)

    def getUndefinedGarbageWord(self):
        return self.dbm.getUnfilterdGarbageWord()

    def newDictionaryInstance(self):
        if self.dic is None :
            self.dic = dictionary.Dictionary()
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
    def simulate(self, stockName, limit, period):
        self.simul.simulate(stockName, limit, period)

    def getTargetDate(self, stockId, period):
        targetAt = self.dbm.selectLastestItem(stockId, period)
        if targetAt is None :
            return date.today()
        return targetAt

    def getFirstContentDate(self, stockId):
        return self.dbm.selectFirstContentDate(stockId)

    def updateAllStockFinance(self):
        stocks = self.getStocks()
        for stock in self.dbm.getStockList():
            stocks.insertNewStock(stock.get('code'))
            items = self.dbm.selectItemByFinanceIsNull(stock.get('id'))
            for item in items :
                finance = self.dbm.selectFinanceByStockIdAndDate(stock.get('id'), item.get('targetAt'))
                if finance is not None :
                    print('update item finance id ', stock.get('name'), item.get('targetAt'))
                    self.dbm.updateItemFinanceId(finance.get('id'), item.get('id'))
    def updatePotentialStock(self, stock):
        r1, r2 = self.getAnalyzeExistData(stock.get('name'), period)
        pd = r2.get(stock.get('name'))
        potential = pd.get('potential')
        count = pd.get('total')
        self.dbm.insertStockPotential(stock.get('id'), period, potential, count)
        print(stock, potential, 'is done')

    def insertKakaoResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ks = webscrap.KakaoStock()
        kakaoResult = ks.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ks.SITE, kakaoResult, stockName, stock.get('id'))
        self.dbm.commit()

period = 2
run = Runner()

run.initStocks() #조건부
# run.filterPotentialStock(period) #하루에 한번씩.
# run.updateAllStockFinance() #하루에 한번씩 15시 이후
run.dailyRun(period, 1) #하루에 한번씩
# run.filteredTarget(period, date.today()+timedelta(days=1)) #하루에 한번씩

# run.insertNewStockScrap('007070') #필요할때 한번씩
# run.migration(run.dbm.selectStockByCode('007070'), period) #필요할때 한번씩
# run.targetAnalyze('', period) #필요할때 한번씩

# run.migrationWork(period) #항상 수행








