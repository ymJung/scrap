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
        self.GUARANTEE_COUNT = 100
        self.FILTER_LIMIT = 50
        self.FILTER_TARGET_LIMIT = 70
        self.LIMIT_RATE = 0.70
        self.CHANCE_PERCENT = 0.10
        self.PPOMPPU_ID = ''
        self.PPOMPPU_PWD = ''
        pythoncom.CoInitialize()
        self.stocks = None
        self.dbm = dbmanager.DBManager()
        self.analyze = analyzer.Analyzer()
        self.simul = simulator.Simulator()
        self.dic = None
        self.KOSPI_CODE = 'D0011001'
        self.KOSPI_NAME = 'KOSPI'

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

    def insertAnalyzedResult(self, stockId, targetAt, period):
        stock = self.dbm.selectStockById(stockId)
        stockName = stock.get('name')
        print(stockName, 'is analyze', targetAt)
        if self.dbm.isNotForecastTarget(stock, targetAt.date(), period):
            return
        mine = miner.Miner()
        targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList, duration = mine.getAnalyzedCnt(targetAt, period, stockName, stock.get('id'))
        savedItemId = self.dbm.saveAnalyzedData(stockName, targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, duration)
        self.dbm.saveAnalyzedItemFinanceList(savedItemId, targetFinanceIdList)
        self.dbm.updateAnalyzedResultItem(stock)
        self.dbm.commit()

    def run(self, stock, targetAt, period):
        if stock is None :
            print("target unexist.")
            return
        self.insertFinance(stock)
        self.insertAnalyzedResult(stock.get('id'), targetAt, period)

    def migrationWork(self, periods):
        for period in periods:
            while True:
                item = self.dbm.selectItemByPeriodAndYet(period, self.dbm.WORK_YET)
                if item is not None :
                    try :
                        self.dbm.updateItemYet(item.get('id'), self.dbm.WORK_DONE)
                        self.insertAnalyzedResult(item.get('stockId'), item.get('targetAt'), period)
                    except Exception :
                        print('work is done.',  sys.exc_info())
                        break
                    except :
                        print("unexpect error.", sys.exc_info())
                        break
                else :
                    print('all clean')
                break

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
    def getDivideNumPercentFloat(self, num1, num2):
        if num2 == 0:
            return 0
        return round(float(((num1 / num2) * 100) - 100), 2)

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
            stocks.insertNewStock(stockCode)
            stock = self.dbm.selectStockByCode(stockCode)
        else :
            self.dbm.updateStockMuch(stock.get('id'), 0)
        #insert
        self.insertPaxnetResult(stock)
        self.insertNaverResult(stock)
        self.insertDaumResult(stock)
        self.insertKakaoResult(stock)

        self.dbm.commit()

    def filteredTarget(self, limitAt):
        targetList = list()
        results = list()
        results.append(str(self.dbm.selectSitePerCount(date.today(), limitAt)))
        for period in self.dbm.getPeriodAll():
            filterdList = list()
            for stock in self.dbm.getStockList():
                plusChanceIds, pointDict = self.getAnalyzeExistData(stock.get('name'), period)
                filteredTargetList = self.getFilteredTarget(plusChanceIds, pointDict, stock, period, date.today())
                for filter in filteredTargetList :
                    percentCheck = filter.get(stock.get('name')) > self.FILTER_LIMIT
                    potentialCheck = filter.get('potential') > self.FILTER_LIMIT
                    chanceCheck = len(filter.get('chance')) > 1
                    countCheck = filter.get('total') > self.GUARANTEE_COUNT
                    if (countCheck and potentialCheck) and (percentCheck or chanceCheck):# and filter.get('yesterday') < 0:
                        filterdList.append(filter)
                targetList.append(filteredTargetList)
            for filter in filterdList :
                  result = "[" + str(filter.get('targetAt')) \
                           + "] [" + str(filter.get('period')) \
                           + "] [" + str(filter.get('code') + "," + filter.get('name')) \
                           + '] [' + str(filter.get('potential')) \
                           + '] [' + str(filter.get(filter.get('name'))) \
                           + '] [' + str(len(filter.get('chance'))) + "]"
                  results.append(result)
                  if (filter.get('targetAt') == limitAt.day):
                    targetList.append(filter)
                    print('today', filter)
        print('print', targetList)
        print('result', results)
        return self.printList(results)

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
            self.insertEventWord(itemId, financeMap.get('chance'))
            self.insertEventWord(itemId, financeMap.get('danger'))
            if (total > 0 and resultPrice != 0) and not (plusPercent == 0 or minusPercent == 0):
                if resultPrice < 0:
                    if self.checkDefence(stockName, targetAt):
                        continue
                    else :
                        minusPoint.append({'name': stockName, 'result': resultPrice, 'point': minusPercent, 'targetAt': str(targetAt)})
                sumPoint.append({'name': stockName, 'result': resultPrice, 'plus_point': plusPercent, 'minus_point': minusPercent,'plus': plus, 'minus': minus, 'targetAt': str(targetAt)})
                if (resultPrice >= 0) :  # plus or 0
                    plusPoint.append({'name': stockName, 'result': resultPrice, 'point': plusPercent, 'targetAt': str(targetAt),'financeMap': financeMap})


        plusChanceIds = []
        for point in plusPoint:
            chanceIds = point.get('financeMap').get('chance')
            plusChanceIds += chanceIds
            plusChanceIds = list(set(plusChanceIds))

        trustPercent = self.getDivideNumPercent(len(plusPoint), len(sumPoint))
        pointDict = {stockName: {'name': stockName, 'period': period, 'potential': trustPercent, 'total': len(analyzedResult)}}
        return plusChanceIds, pointDict


    def getFilteredTarget(self, plusChanceIds, pointDict, stock, period, startAt):
        filteredTargets = []
        forecastResults = self.dbm.getForecastResult(stock.get('name'), startAt, period)
        for each in forecastResults:
            itemId, point, stockName, targetAt = self.getFilteredForecastResult(each)
            analyzedTargetData = self.getAnalyzedTarget(itemId, plusChanceIds, point, pointDict, stockName, targetAt, stock.get('code'))
            filteredTargets.append(analyzedTargetData)
        return filteredTargets

    def getForecastTarget(self, plusChanceIds, pointDict, stock, period):
        results = list()
        forecastResults = self.dbm.getForecastResult(stock.get('name'), date.today(), period)
        for forecastResult in forecastResults:
            itemId, point, stockName, targetAt = self.getFilteredForecastResult(forecastResult)
            analyzedTargetData = self.getAnalyzedTarget(itemId, plusChanceIds, point, pointDict, stockName, targetAt, stock.get('code'))
            results.append(analyzedTargetData)
        return results


    def getAnalyzedTarget(self, itemId, plusChanceIds, point, pointDict, stockName, targetAt, stockCode):
        financeList = self.dbm.getFinanceListFromItemId(itemId)
        chanceIds = []
        for chanceId in plusChanceIds:
            if chanceId in financeList:
                chanceIds.append(chanceId)
        financeResult = self.getBeforeFinanceResult(itemId)
        return {'name':stockName, stockName: point, 'period' : pointDict.get(stockName).get('period'), 'potential': pointDict.get(stockName).get('potential'),
                'total': pointDict.get(stockName).get('total'), 'targetAt': targetAt.day,'chance': chanceIds, 'yesterday': financeResult, 'code':stockCode}

    def getFilteredForecastResult(self, each):
        plus = each.get('plus')
        minus = each.get('minus')
        point = self.getDivideNumPercent(plus, plus + minus)
        stockName = each.get('name')
        targetAt = each.get('targetAt')
        return each.get('id'), point, stockName, targetAt

    def filterPotentialStock(self, periods):
        for period in periods :
            for stock in self.dbm.getAllStockList():
                self.updatePotentialStock(stock, period)
                poten = self.dbm.selectPotentialStock(stock.get('id'), period)
                if poten.get('count') > self.GUARANTEE_COUNT and poten.get('potential') < self.FILTER_LIMIT and stock.get('much') == 0 :
                    self.dbm.updateStockMuch(stock.get('id'), 1)
                    print(stock, ' set much 1. ', poten.get('potential'))
        self.dbm.commit()
    def insertDefaultItemList(self, forecastAt, period):
        for stock in self.dbm.getUsefulStockList(forecastAt, period) :
            if self.dbm.isNotForecastTarget(stock, forecastAt, period) :
                continue
            else :
                self.dbm.insertItemDefault(stock.get('id'), forecastAt, period)
        self.dbm.commit()
    def dailyRun(self, forecastAt, period):
        self.insertDefaultItemList(forecastAt, period)
        items = self.dbm.getWorkYetItems(forecastAt, period)
        for item in items:
            try :
                item = self.dbm.selectItem(item.get('id'))
                if item.get('yet') == self.dbm.WORK_DONE:
                    continue
                self.dbm.updateItemYet(item.get('id'), self.dbm.WORK_DONE)
                self.insertAnalyzedResult(item.get('stockId'), item.get('targetAt'), item.get('period'))
            except Exception :
                print('work is done.',  sys.exc_info(), forecastAt)
                break
            except :
                print("unexpect error.", sys.exc_info())
                break
        self.updateDefaultItemList()

    def targetAnalyze(self, stockCode, period):
        stock = self.dbm.selectStockByCode(stockCode)
        print('targetAnalyze', stock)
        lastDate = self.dbm.selectLastestItem(stock.get('id'), period)
        today = date.today()
        if lastDate < today:
            self.run(stock, today, period)
        plusChanceIds, pointDict = self.getAnalyzeExistData(stock.get('name'), period)
        filteredTargetList = self.getForecastTarget(plusChanceIds, pointDict, stock, period)
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
        self.updateKospiPrice()

    def updateKospiPrice(self):
        stock = self.dbm.selectStockByCode(self.KOSPI_CODE)
        lastestDate = self.dbm.selectLastestFinance(stock.get('id'))
        priceInfos = webscrap.KakaoStock().getPriceInfos(stock.get('code'), lastestDate)
        for each in priceInfos:
            exist = self.dbm.selectFinanceByStockIdAndDate(stock.get('id'), each.get('date'))
            if exist is None :
                self.dbm.insertFinance(stock.get('id'), each.get('date'), high=0, low=0, start=each.get('start'), final=each.get('final'))
    def updatePotentialStock(self, stock, period):
        r1, r2 = self.getAnalyzeExistData(stock.get('name'), period)
        pd = r2.get(stock.get('name'))
        potential = pd.get('potential')
        count = pd.get('total')
        self.dbm.insertOrUpdateStockPotential(stock.get('id'), period, potential, count)
        if stock.get('much') == 0 :
            print(stock, potential, 'is done')

    def insertKakaoResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ks = webscrap.KakaoStock()
        kakaoResult = ks.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ks.SITE, kakaoResult, stockName, stock.get('id'))
        self.dbm.commit()

    def getWorkYetItemAndCheck(self, forecastAt, period):
        item = self.dbm.selectItemByTargetAtAndPeriodAndYet(forecastAt, period, self.dbm.WORK_YET)
        self.dbm.updateItemYet(item.get('id'), self.dbm.WORK_DONE)
        return item

    def updateDefaultItemList(self):
        items = self.dbm.selectItemListByCnt(0, 0, 0, 0)
        for item in items :
            self.dbm.updateItemYet(item.get('id'), self.dbm.WORK_YET)

    def dailyAll(self):
        for period in self.dbm.getPeriodAll() :
            forecastAt = date.today() + timedelta(days=period)
            self.dailyRun(forecastAt, period)
    def deleteDefaultItem(self, periods):
        for period in periods :
            for stock in self.dbm.getStockList() :
                for idx in range(365) :
                    targetAt = date.today() - timedelta(days=idx)
                    if self.dbm.isNotForecastTarget(stock, targetAt, period) is True:
                        self.dbm.deleteItemDefault(stock.get('id'), targetAt, period)
        self.dbm.commit()
    def insertDefaultItem(self, periods):
        for stock in self.dbm.getStockList() :
            for period in periods :
                for idx in range(365) :
                    targetAt = date.today() - timedelta(days=idx)
                    if self.dbm.isNotForecastTarget(stock, targetAt, period) is False:
                        self.dbm.insertItemDefault(stock.get('id'), targetAt, period)
        self.updateAllStockFinance()
        self.dbm.commit()

    def getBeforeFinanceResult(self, itemId):
        financeId = self.dbm.getBeforeFinanceId(itemId)
        return self.dbm.getFinancePrice(financeId)
    def printList(self, results):
        results.sort()
        msg = 'targetAt period potential per chance\n'
        for result in results :
            msg += str(result) + "\n"
        return msg

    def getFinancePercent(self, stockName, targetAt):
        result = self.dbm.getFinanceDataByStockNameAndData(stockName=stockName, sliceDate=targetAt)
        return self.getDivideNumPercentFloat(result.get('final'), result.get('start'))

    def checkDefence(self, stockName, targetAt):
        try :
            return self.getFinancePercent(stockName, targetAt) > self.getFinancePercent(self.KOSPI_NAME, targetAt)
        except:
            return False
    def updateItemYet(self, stockCode):
        if len(stockCode) == 0:
            return
        stock = self.dbm.selectStockByCode(stockCode=stockCode)
        itemList = self.dbm.selectItemList(stockId=stock.get("id"))
        for item in itemList:
            self.dbm.updateItemYet(item.get('id'), self.dbm.WORK_YET)
        self.dbm.commit()

    def insertEventWord(self, itemId, wordIds):
        for wordId in wordIds:
            if self.dbm.hasEventWord(itemId, wordId):
                self.dbm.insertEvent(itemId, wordId)
    def getPotential(self):
        datas = self.dbm.getPotentialDatas(self.LIMIT_RATE)
        msg = ''
        for data in datas:
            msg += (data.get('analyzeAt').strftime("%Y-%m-%d") + ' [' + data.get(' name') + '] [' + data.get('code') + '] [' + str(data.get('potential')) + '] [' + str(data.get('volume')) + ']\n')
        return msg

run = Runner()

# run.updateAllStockFinance() #하루에 한번씩 15시 이후
#run.filterPotentialStock(run.dbm.getPeriodAll()) #하루에 한번씩.
#run.dailyAll() #하루에 한번씩.
# print(run.filteredTarget(date.today()+timedelta(days=max(run.dbm.getPeriodAll())))) #하루에 한번씩
print(run.getPotential())


# print(results)

# run.insertNewStockScrap(stockCode='029780') #필요할때 한번씩
# run.insertDefaultItem(run.dbm.getPeriodAll())
#run.updateItemYet(stockCode='')  #필요할때 한번씩


# run.targetAnalyze('', 3) #필요할때 한번씩
# run.migrationWork(run.dbm.getPeriodAll()) #항상 수행

