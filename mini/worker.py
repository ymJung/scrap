from datetime import date, timedelta
import pymysql.cursors
import datetime
import threading
import queue
import numpy
import sys
import win32com.client
from telegram.ext import Updater
from bs4 import BeautifulSoup
from urllib.request import urlopen
import urllib
import time
import json


class KakaoStock:
    def __init__(self):
        self.DATE_FORMAT = '%Y-%m-%dT%H:%M:%S' #2016-05-06T00:37:53.000+00:00
        self.LIMIT_COUNT = 500
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)

    def convertDate(self, param):
        try:
            return datetime.datetime.strptime(param[0:18], self.DATE_FORMAT)  # 2016.02.13 20:30
        except:
            return self.DEFAULT_DATE

    def getPriceInfos(self, code, lastUseDateAt):
        if lastUseDateAt is None :
            lastUseDateAt = datetime.date.today() - datetime.timedelta(days=365 * 3)
        else :
            lastUseDateAt = lastUseDateAt.date()
        todate = (datetime.datetime.today() + datetime.timedelta(days=1)).date()
        limit = (todate - lastUseDateAt).days
        breakFlag = False
        data = list()
        dates = set()
        while True :
            if limit > self.LIMIT_COUNT :
                nowLimit = (limit % self.LIMIT_COUNT)
                if nowLimit == 0 :
                    nowLimit = self.LIMIT_COUNT
                limit = limit - nowLimit
            else :
                breakFlag = True
                nowLimit = limit
            todateStr = todate.strftime('%Y-%m-%d')
            try :
                print('code(' , code ,') todateStr : ' , todateStr ,' nowLimit : ' ,nowLimit)
                url = LINK1 +code+ LINK3 + str(nowLimit) + LINK4 + todateStr
                soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')
                jls = json.loads(soup.text)

                dayCandles = jls['dayCandles']
                for jl in dayCandles:
                    date = self.convertDate(jl['date'])
                    tradePrice = jl['tradePrice']
                    changePrice = jl['changePrice']
                    dataMap = {
                        'date': date,
                        'start': tradePrice - changePrice,
                        'final': tradePrice
                    }
                    if date in dates:
                        continue
                    else:
                        dates.add(date)
                    data.append(dataMap)
                if breakFlag is True:
                    break
            except urllib.error.URLError as e :
                print('url error')
                time.sleep(0.3)
                print(e)
            except :
                print('some thing are wrong. will return', sys.exc_info())
        return data

class DSStockError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class DSStock:
    def __init__(self):
        self.dbm = Runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
        self.DATE_FORMAT = '%Y%m%d'
        self.cybos = win32com.client.Dispatch("CpUtil.CpCybos")
        self.ins = win32com.client.Dispatch("CpUtil.CpStockCode")
        self.stock = win32com.client.Dispatch("dscbo1.StockMst")
        self.graph = win32com.client.Dispatch("dscbo1.CbGraph1")
        self.chart = win32com.client.Dispatch("CpSysDib.StockChart")
        if self.cybos.IsConnect is not 1:
            raise DSStockError("disconnect")
        self.DATE = 'date'
        self.START = 'start'
        self.HIGH = 'high'
        self.LOW = 'low'
        self.FINAL = 'final'
        self.MARKET_OFF_HOUR = 15


    def __del__(self):
        self.dbm.commit()

    def updateStockInfo(self):
        totalCount = self.ins.GetCount()
        for i in range(0, totalCount):
            dsCode = self.ins.GetData(0, i)
            dsName = self.ins.getData(1, i)
            stock = self.dbm.selectStockByCode(dsCode)
            if stock is not None :
                if stock.get('name') != dsName :
                    print('update stock name', stock.get('name'), dsName)
                    self.dbm.updateStockName(stock.get('id'), dsName, dsCode)
                    contentIdList = self.dbm.selectContentIdList(stock.get('id'))
                    for contentId in contentIdList :
                        self.dbm.updateContentQuery(contentId.get('id'), stock.get('id'))
                    self.dbm.commit()
    def getStock(self, stockCode):
        stock = self.dbm.selectStockByCode(stockCode)
        if stock is None:
            totalCount = self.ins.GetCount()
            for i in range(0, totalCount):
                dsCode = self.ins.GetData(0, i)
                dsName = self.ins.getData(1, i)

                if dsCode == str(stockCode) or dsCode.replace('A','') == str(stockCode):
                    self.dbm.insertStock(dsCode, dsName)
                    print("insert [", dsCode , "][", dsName , "]")
                    return self.dbm.selectStockByCode(stockCode)
            print("Not found name : " + str(stockCode))

            raise DSStockError('not found stock')
        else:
            hit = int(stock.get('hit')) + 1
            self.dbm.updateStockHit(hit, stock.get('id'))
            return self.dbm.selectStockByCode(stockCode)
    def getChartDataList(self, code, count):
        self.chart.SetInputValue(0, code)
        self.chart.SetInputValue(1, ord('2'))
        self.chart.SetInputValue(4, count)
        self.chart.SetInputValue(5, [0, 2, 3, 4, 5])
        self.chart.SetInputValue(6, ord('D'))

        self.chart.BlockRequest()
        num = self.chart.GetHeaderValue(3)
        data = []
        for i in range(num):
            temp = {}
            temp[self.DATE] = (self.chart.GetDataValue(0, i))
            temp[self.START] = float(format(self.chart.GetDataValue(1, i), '.2f'))
            temp[self.HIGH] = float(format(self.chart.GetDataValue(2, i), '.2f'))
            temp[self.LOW] = float(format(self.chart.GetDataValue(3, i), '.2f'))
            temp[self.FINAL] = float(format(self.chart.GetDataValue(4, i), '.2f'))
            data.append(temp)
        return data
    def insertFinanceData(self, datas, stockId):
        for data in datas:
            date = datetime.datetime.strptime(str(data.get(self.DATE)), self.DATE_FORMAT)
            start = data.get(self.START)
            high = data.get(self.HIGH)
            low = data.get(self.LOW)
            final = data.get(self.FINAL)
            if (int(date.today().strftime(self.DATE_FORMAT)) == data.get(self.DATE)) and date.now().hour < self.MARKET_OFF_HOUR :
                continue

            finances = self.dbm.selectFinanceByStockIdAndDate(stockId, date)
            if len(finances) == 0:
                self.dbm.insertFinance(stockId, date, high, low, start, final)
                print('insert finance' + str(date))
            dayOfFinanceData = self.dbm.getFinanceDataByDay(stockId, date, datetime.datetime.strptime(str(data.get(self.DATE)) + str(self.MARKET_OFF_HOUR), self.DATE_FORMAT + '%H'))

            if dayOfFinanceData is not None :
                self.dbm.updateFinance(high, low, start, final, dayOfFinanceData.get('id'))
                print('update finance' + str(date))
        self.dbm.commit()


    def insertNewStock(self, stockCode):
        insert = self.getStock(stockCode)
        datas = self.getChartDataList(insert.get('code'), 365 * 2)
        self.insertFinanceData(datas, str(insert.get('id')))
        self.dbm.commit()
        return insert




class MinerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)
class Runner:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):


        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.LIMIT_COUNT = 5
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.LIMIT_YEAR_SEPERATOR = 5
        self.INTERVAL_YEAR_SEPERATOR = 73
        self.SPLIT_COUNT = 1000
        self.THREAD_LIMIT_COUNT = 4
        self.CONTENT_DATA_NAME = 'contentData'
        self.MIN_WORD_LEN = 2
        self.MAX_WORD_LEN = 50
        self.WORD_NAME = 'word'
        self.PLUS_NAME = 'plus'
        self.MINUS_NAME = 'minus'
        self.FINANCE_NAME = 'finance'
        self.DATE_NAME = 'date'
        self.WORK_DONE = 0
        self.WORK_YET = 1
        self.GUARANTEE_COUNT = 100
        self.FILTER_LIMIT = 50
        self.FILTER_TARGET_LIMIT = 70
        self.CHANCE_PERCENT = 0.10
        self.stocks = None
        self.KOSPI_CODE = 'D0011001'
        self.KOSPI_NAME = 'KOSPI'
        self.LIMIT_RATE = 0.70


    def updateFinance(self, high, low, start, final, financeId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `data`.`finance` SET `high`=%s,`low`=%s,`start`=%s,`final`=%s WHERE `id`=%s;", (high, low, start, final, financeId))

    def getFinanceDataByDay(self, stockId, date, dateStr):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`stockId`,`date` FROM `finance` WHERE `stockId`=%s AND date = %s AND createdAt < %s", (stockId, date, dateStr))
        return cursor.fetchone()

    def insertFinance(self, stockId, date, high, low, start, final):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`finance` (`stockId`,`date`,`high`,`low`,`start`,`final`) VALUES (%s, %s, %s, %s, %s, %s);", (stockId, date, high, low, start, final))

    def selectFinanceByStockIdAndDate(self, stockId, date):
        cursor = self.connection.cursor()
        selectSql = "SELECT `id`,`stockId`,`date` FROM `finance` WHERE `stockId`=%s AND date = %s"
        cursor.execute(selectSql, (stockId, date))
        return cursor.fetchall()
    def updateStockHit(self, hit, stockId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `stock` SET `hit`=%s WHERE `id`=%s", (hit, stockId))

    def insertStock(self, code, name):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`stock` (`code`,`name`,`use`, `scrap`) VALUES (%s, %s, 1, 1);", (code, name))

    def updateContentQuery(self, contentId, stockId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `content` SET `stockId`=%s WHERE `id`=%s", (stockId, contentId))

    def migrationWork(self, periods):
        for period in periods :
            while True :
                item = self.selectItemByPeriodAndYet(period, self.WORK_YET)
                if item is not None :
                    self.updateItemYet(item.get('id'), self.WORK_DONE)
                    self.insertAnalyzedResult(item.get('stockId'), item.get('targetAt'), period)
                else :
                    print('all clean')
                    break
    def migration(self, period, stockCode):
        stock = self.getStock(stockCode)
        if stock is not None :
            targetDate = self.getLastItemDate(stock.get('id'), period)
            limitDate = self.getFirstContentDate(stock.get('id'))
            print('migration', stock.get('name'), targetDate, limitDate)
            if limitDate is None :
                print('content is none.')
                pass
            idx = 0
            while True :
                idx += 1
                targetAt = targetDate - timedelta(days=idx)
                if limitDate > targetAt :
                    print('done', stock.get('name'), limitDate)
                    break
                if self.checkHolyDay(targetAt):
                    continue
                print('migration target at ', targetAt, 'period ', idx, '/')
                self.run(stock, targetAt, period)
    def selectLastestItem(self, stockId, period):
        cursor = self.connection.cursor()
        cursor.execute("select targetAt from item where period = %s and stockId = %s order by targetAt asc limit 1", (period, stockId))
        result = cursor.fetchone()
        if result is not None :
            return result.get('targetAt').date()
        return None

    def getLastItemDate(self, stockId, period):
        targetAt = self.selectLastestItem(stockId, period)
        if targetAt is None :
            return date.today()
        return targetAt

    def getFirstContentDate(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("select date from content where stockId = %s order by date asc limit 1;", (stockId))
        result = cursor.fetchone()
        if result is not None :
            return result.get('date').date()
        return None
    def run(self, stock, targetAt, period):
        if stock is None :
            print("target unexist.")
            return
        # self.insertFinance(stock)
        self.insertAnalyzedResult(stock, targetAt, period)

    def commit(self):
        self.connection.commit()
        print('dbm commit')
    def insertAnalyzedResult(self, stockId, targetAt, period):
        stock = self.selectStockById(stockId)
        stockName = stock.get('name')
        print(stockName, 'is analyze', targetAt)
        if self.isNotForecastTarget(stock, targetAt.date(), period):
            return
        targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList, duration = self.getAnalyzedCnt(targetAt, period, stockName, stock.get('id'))
        savedItemId = self.saveAnalyzedData(stockName, targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, duration)
        self.saveAnalyzedItemFinanceList(savedItemId, targetFinanceIdList)
        self.updateAnalyzedResultItem(stock)
        self.commit()
    def updateAnalyzedResultItem(self, stock):
        cursor = self.connection.cursor()
        stockName = stock.get('name')
        stockId = stock.get('id')

        selectTargetItemsSql = 'SELECT id, targetAt FROM item WHERE financeId IS NULL AND stockId = %s'
        cursor.execute(selectTargetItemsSql, (stockId))
        itemList = cursor.fetchall()
        for item in itemList:
            itemId = item.get('id')
            itemTargetAt = item.get('targetAt')
            cursor.execute(
                'SELECT f.id, s.name, f.date, f.start, f.final FROM finance f, stock s WHERE f.stockId = s.id AND s.name = %s AND f.date = %s ORDER BY f.createdAt DESC LIMIT 1',
                (stockName, itemTargetAt))
            targetFinance = cursor.fetchone()
            if targetFinance is not None:
                print('update finance date.', stock.get('name'), itemTargetAt)
                self.updateItemFinanceId(targetFinance.get('id'), itemId)

    def updateItemFinanceId(self, financeId, itemId):
        cursor = self.connection.cursor()
        updateItemPriceSql = "UPDATE item SET financeId = %s WHERE id= %s"
        cursor.execute(updateItemPriceSql, (financeId, itemId))
    def saveAnalyzedItemFinanceList(self, itemId, financeIdList):
        cursor = self.connection.cursor()
        for financeId in financeIdList :
            cursor.execute("INSERT INTO chart_map (itemId, financeId) VALUES (%s, %s)", (itemId, financeId))
    def saveAnalyzedData(self, stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, duration):
        cursor = self.connection.cursor()
        cursor.execute('SELECT id FROM stock WHERE name = %s', stockName)
        stock = cursor.fetchone()
        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s ', (stock.get('id'), targetAt, period))
        items = cursor.fetchall()
        if len(items) == 0:
            cursor.execute("INSERT INTO `data`.`item` (`stockId`, `plus`, `minus`, `totalPlus`, `totalMinus`, `targetAt`, `period`, `duration`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, duration))
        else :
            for item in items :
                cursor.execute("UPDATE item SET `plus`=%s, `minus`=%s, `totalPlus`=%s, `totalMinus`=%s, `yet`=%s, `duration`=%s WHERE `id`= %s",
                               (plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, self.WORK_DONE, duration, item.get('id')))
        self.commit()
        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s', (stock.get('id'), targetAt, period))
        return cursor.fetchone().get('id')


    def isNotForecastTarget(self, stock, targetAt, period):
        stockId = stock.get('id')
        stockName = stock.get('name')
        lastScrapAt = stock.get('lastScrapAt')
        if self.checkHolyDay(targetAt):
            print('exist holy day', stockName, targetAt)
            return True
        if lastScrapAt is None or lastScrapAt.date() < targetAt - datetime.timedelta(days=period):
            print('not yet to scrap.', stockName, targetAt)
            return True
        cursor = self.connection.cursor()
        result = cursor.execute('SELECT id FROM item WHERE targetAt = %s and stockId = %s and period = %s and %s', (targetAt, stockId, period, self.WORK_DONE))
        if result != 0:
            print('exist item date ', targetAt, stockId)
            return True
        targetStartAt = self.getTargetStartAt(targetAt, period)
        targetEndAt = targetAt - datetime.timedelta(days=period)
        result = cursor.execute('SELECT `id` FROM `content` WHERE  `date` > %s and `date` <= %s AND `stockId` = %s', (targetStartAt, targetEndAt, stockId))
        if result == 0:
            print('empty content data.', targetStartAt, targetEndAt, stockName)
            return True
        return False

    def getAnalyzedCnt(self, targetDate, period, stockName, stockId):
        totalWordIdFinanceMap = {}
        start = datetime.datetime.now()
        firstAt = self.selectFirstContentDate(stockId)
        contents = self.getStockNameContent(stockName, firstAt, targetDate, stockId)
        wordIdFinanceMap = self.multiThreadWordChangePriceMap(contents, stockName, period)
        self.appendWordPriceMap(wordIdFinanceMap, totalWordIdFinanceMap)

        targetStartAt = self.getTargetStartAt(targetDate, period)
        targetWordIds = self.getTargetContentWordIds(stockName, targetStartAt, targetDate, stockId)

        resultWordFinanceMap = self.getWordFinanceMap(targetWordIds, totalWordIdFinanceMap)
        targetChartList = self.getAnalyzedChartList(resultWordFinanceMap)
        totalChartList = self.getAnalyzedChartList(totalWordIdFinanceMap)
        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetFinanceIdList = self.getFinanceIdList(targetChartList)
        end = datetime.datetime.now()
        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList, end - start
    def appendWordPriceMap(self, wordIdFinanceMap, totalWordPriceMap):
         for wordId in wordIdFinanceMap.keys() :
            if wordIdFinanceMap[wordId] == None :
                continue
            try :
                totalWordPriceMap[wordId] = totalWordPriceMap[wordId] + wordIdFinanceMap[wordId]
            except KeyError :
                totalWordPriceMap[wordId] = []
                totalWordPriceMap[wordId] = totalWordPriceMap[wordId] + wordIdFinanceMap[wordId]
            totalWordPriceMap[wordId] = list(set(totalWordPriceMap[wordId]))
    def selectFirstContentDate(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("select date from content where stockId = %s and date !='1970-12-31 23:59:59' order by date asc limit 1;", (stockId))
        result = cursor.fetchone()
        if result is not None :
            return result.get('date').date()
        return None
    def getStockNameContent(self, stockName, startAt, limitAt, stockId):
        contentsList = []
        count = 0
        cnt = self.countContents(stockId, startAt, limitAt)
        if cnt is not None:
            count = cnt.get('cnt')
            if count == None :
                print('count is None', stockName, startAt, limitAt)
                return []
        for i in range(int((count / self.LIMIT_COUNT)) + 1):
            try:
                startPos = (i * 10) + 1
                endPos = (i + 1) * self.LIMIT_COUNT
                contents = self.getContentBetween(stockId, startAt, limitAt, startPos, endPos)
                if contents is not None:
                    contentsList = contentsList + contents
                else:
                    raise MinerError('content is not valid.')
            except MinerError:
                print('data is empty.')
                continue
            except MemoryError :
                print('memory error', stockName, len(contentsList))
                return contentsList
        return contentsList

    def multiThreadWordChangePriceMap(self, contents, stockName, period):
        queueList = []
        threadList = []
        lock = threading.Lock()
        for idx in range(int(len(contents) / self.SPLIT_COUNT) + 1) :
            start = idx * self.SPLIT_COUNT
            end = idx * self.SPLIT_COUNT + self.SPLIT_COUNT
            if end > len(contents) :
                end = len(contents)
            splitedContentTarget = contents[start : end]

            resultQueue = queue.Queue()
            thread = threading.Thread(target=self.getWordChangePriceMap, args=(splitedContentTarget, stockName, period, resultQueue, lock), name=stockName+str(len(splitedContentTarget)))
            threadList.append(thread)
            queueList.append(resultQueue)

        totalWordPriceMap = self.getTotalPriceMap(queueList, threadList)

        return totalWordPriceMap
    def getTargetContentWordIds(self, stockName, periodDate, targetDate, stockId):
        wordIds = []
        contents = self.getStockNameContent(stockName, periodDate, targetDate, stockId)
        print('target content word find. content length . ', len(contents))
        for content in contents:
            contentWordIds = self.getContentWordIdAndWork(content.get('id'), content.get('yet'))
            wordIds = wordIds + contentWordIds
        return wordIds

    def getWordFinanceMap(self, wordIds, totalWordFinances):
        print('get word price ', len(wordIds))
        wordPriceDict = {}
        for wordId in wordIds:
            try:
                totalWordFinances[wordId]
            except KeyError:
                continue
            try:
                wordPriceDict[wordId] = wordPriceDict[wordId] + totalWordFinances[wordId]
            except KeyError:
                wordPriceDict[wordId] = totalWordFinances[wordId]
            except MemoryError:
                print('memory error', len(totalWordFinances[wordId]))
                totalWordFinances[wordId] = list(set(totalWordFinances[wordId]))
                wordPriceDict[wordId] = wordPriceDict[wordId] + totalWordFinances[wordId]

        return wordPriceDict
    def getAnalyzedChartList(self, wordFinanceMap):
        chartList = []
        print('get analyzed chart list word map size : ', len(wordFinanceMap))
        for wordId in wordFinanceMap.keys():
            plusList = []
            minusList = []
            financeIdList = []
            for financeId in wordFinanceMap[wordId]:
                price = self.getFinancePrice(financeId)
                financeIdList.append(financeId)
                try :
                    if price > 0:
                        plusList.append(price)
                    if price < 0:
                        minusList.append(price)
                except MemoryError :
                    print('Memory Error. getAnalyzedChartList plus ', wordId, len(plusList), len(minusList))
                    plusList = self.divideList(plusList)
                    minusList = self.divideList(minusList)
            chart = {self.WORD_NAME: wordId, self.PLUS_NAME: plusList, self.MINUS_NAME: minusList, self.FINANCE_NAME: financeIdList}
            chartList.append(chart)
        return chartList

    def getAnalyzedCountList(self, chartList):
        plusCnt = 0
        minusCnt = 0
        for chart in chartList:
            plusCnt += len(chart.get(self.PLUS_NAME))
            minusCnt += len(chart.get(self.MINUS_NAME))
        return plusCnt, minusCnt
    def getFinanceIdList(self, chartList):
        financeIdList = []
        for chart in chartList :
            financeIds = chart.get(self.FINANCE_NAME)
            financeIdList = financeIdList + financeIds
        return financeIdList
    def countContents(self, stockId, startAt, limitAt):
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(c.id) as cnt FROM content c WHERE c.stockId = %s and c.date > %s and c.date <= %s", (stockId, startAt, self.getPlusOneDay(limitAt)))
        return cursor.fetchone()

    def getContentBetween(self, stockId, startAt, limitAt, startPos, endPos):
        cursor = self.connection.cursor()
        cursor.execute("SELECT c.id, c.yet, c.date FROM content c WHERE c.stockId = %s and c.date > %s and c.date <= %s ORDER BY c.id DESC LIMIT %s , %s", (stockId, startAt, self.getPlusOneDay(limitAt), startPos, endPos))
        result = cursor.fetchall()
        if result is not None :
            return list(result)
        return result


    def getWordChangePriceMap(self, contentDataList, stockName, period,  queue, lock):
        wordIdFinanceMap = {}
        cacheFinanceChangePrices = {}

        for content in contentDataList:
            date = content.get(self.DATE_NAME)
            sliceDate = (date + timedelta(days=period)).strftime('%Y-%m-%d')
            lock.acquire()
            try :
                financeId = self.getFinanceChangeId(sliceDate, stockName, cacheFinanceChangePrices)
            finally:
                lock.release()
            if financeId is None:
                continue
            lock.acquire()
            wordIds = self.getContentWordIdAndWork(content.get('id'), content.get('yet'))
            lock.release()
            for wordId in wordIds :
                 try:
                     wordIdFinanceMap[wordId].append(financeId)
                 except KeyError:
                     wordIdFinanceMap[wordId] = [financeId]
        print('put word data map to queue ', len(wordIdFinanceMap))
        queue.put(wordIdFinanceMap)
    def getTotalPriceMap(self, queueList, threadList):
        for idx in range(int(len(threadList) / self.THREAD_LIMIT_COUNT) + 1) :
            start = idx * self.THREAD_LIMIT_COUNT
            end = idx * self.THREAD_LIMIT_COUNT + self.THREAD_LIMIT_COUNT
            if end > len(threadList) :
                end = len(threadList)
            splitedThreads = threadList[start : end]
            for thread in splitedThreads:
                print('thread start', thread.getName(), start, end, len(threadList))
                thread.start()
            for thread in splitedThreads:
                print('thread join', thread.getName(), start, end, len(threadList))
                thread.join()
        totalWordPriceMap = {}

        for que in queueList:
            wordIdFinanceMap = que.get()
            self.appendWordPriceMap(wordIdFinanceMap, totalWordPriceMap)
        return totalWordPriceMap

    def getFinanceChangeId(self, sliceDate, stockName, cacheFinanceChangePrices):
        try:
            return cacheFinanceChangePrices[str(sliceDate) + stockName]
        except KeyError:
            pass
        try :
            finance = self.getFinanceDataByStockNameAndData(stockName, sliceDate)
            if finance is not None :
                financeId = finance.get('id')  # one ? many?
                # stockPrice = int(finance.get(self.START_NAME)) - int(finance.get(self.FINAL_NAME))
                cacheFinanceChangePrices[str(sliceDate) + stockName] = financeId
                return financeId
            else:
                cacheFinanceChangePrices[str(sliceDate) + stockName] = None
                return None
        except :
            print('except', stockName, sliceDate, len(cacheFinanceChangePrices))
            print("miner unexpected error:", sys.exc_info())
            return None

    def getFinancePrice(self, financeId):
        cursor = self.connection.cursor()
        cursor.execute("select start, final from finance where id = %s", financeId)
        result = cursor.fetchone()
        price = result.get('start') - result.get('final')
        return price
    def divideList(self, list):
        newList = []
        try :
            for i in range(len(list)) :
                idx = i*2
                if (idx + 1) >= len(list) :
                    return newList
                split = list[idx:idx+1]
                newList.append(numpy.mean(split))
            return newList
        except MemoryError :
            print('divideList memory error', len(list))
            return self.divideList(newList)
    def splitStr(self, str):
        if str is None:
            return []
        str = str.replace('\n', ' ')
        return str.split()
    def existSplitWord(self, fullWord):
        if self.getExistWordIdx(fullWord) > 0:
            return True
        return False
    def getExistWordIdx(self, fullWord):
        for i in range(len(fullWord) - self.MIN_WORD_LEN):
            if self.existWord(fullWord[0:i + self.MIN_WORD_LEN]):
                return i + self.MIN_WORD_LEN
        return 0
    def existWord(self, data):
        result = self.selectWord(data)
        return result is not None
    def selectWord(self, word):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id` FROM `word` WHERE `word`=%s", (word))
        return cursor.fetchone()

    def getWordByStr(self, fullWord):
        idx = self.getExistWordIdx(fullWord)
        if idx > 0:
            return self.getWordId(fullWord[0:idx])
        else:
            return ''

    def getWordId(self, data):
        return self.selectWord(data).get('id')
    def getFinanceDataByStockNameAndData(self, stockName, sliceDate):
        cursor = self.connection.cursor()
        cursor.execute("SELECT f.id, f.start, f.final FROM finance f, stock s WHERE f.stockId = s.id and s.name = %s and f.date = %s", (stockName, sliceDate))
        return cursor.fetchone()

    def insertItemDefault(self, stockId, forecastAt, period):
        cursor = self.connection.cursor()
        cursor.execute("select id from item where stockId = %s and targetAt = %s and period = %s", (stockId, forecastAt, period))
        result = cursor.fetchall()
        if len(result) == 0 :
            cursor.execute("INSERT INTO item (`stockId`, `targetAt`, `period`, `yet`) VALUES (%s, %s, %s, %s)", (stockId, forecastAt, period, self.WORK_YET))

    def getStock(self, stockCode):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` FROM stock WHERE `much` = 0 AND code =%s ORDER BY id asc", (stockCode))
        return cursor.fetchone()

    def getStockList(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` FROM stock where `much` = 0 ORDER BY id asc")
        return cursor.fetchall()
    def checkHolyDay(self, targetAt):
        cursor = self.connection.cursor()
        cursor.execute("select id from holyday where date = %s", (targetAt))
        results = cursor.fetchall()
        return len(results) > 0
    def dailyRun(self, forecastAt, period):
        self.insertDefaultItemList(forecastAt, period)
        items = self.getWorkYetItems(forecastAt, period)
        for item in items :
            try :
                item = self.selectItem(item.get('id'))
                if item.get('yet') == self.WORK_DONE:
                    continue
                self.updateItemYet(item.get('id'), self.WORK_DONE)
                self.insertAnalyzedResult(item.get('stockId'), item.get('targetAt'), item.get('period'))
            except Exception :
                print('work is done.',  sys.exc_info(), forecastAt)
                break
            except :
                print("unexpect error.", sys.exc_info())
                break
        self.updateDefaultItemList()


    def insertDefaultItemList(self, forecastAt, period):
        for stock in self.getUsefulStockList(forecastAt, period) :
            if self.isNotForecastTarget(stock, forecastAt, period) :
                continue
            else :
                self.insertItemDefault(stock.get('id'), forecastAt, period)
        self.commit()

    def getWorkYetItemAndCheck(self, forecastAt, period):
        item = self.selectItemByTargetAtAndPeriodAndYet(forecastAt, period, self.WORK_YET)
        self.updateItemYet(item.get('id'), self.WORK_DONE)
        return item

    def selectStockById(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`code`,`name`,`lastUseDateAt`, `hit`, `lastScrapAt` FROM `stock` WHERE `id` = %s", (stockId))
        return cursor.fetchone()

    def updateItemYet(self, stockId, yet):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `data`.`item` SET `yet`=%s WHERE `id`=%s", (yet, stockId))
        self.commit()
    def selectItemByTargetAtAndPeriodAndYet(self, forecastAt, period, yet):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE targetAt = %s AND period = %s AND yet = %s LIMIT  1", (forecastAt, period, yet))
        return cursor.fetchone()
    def updateDefaultItemList(self):
        items = self.selectItemListByCnt(0, 0, 0, 0)
        for item in items :
            self.updateItemYet(item.get('id'), self.WORK_YET)
    def selectItemListByCnt(self, plus, minus, plusTot, minusTot):
        cursor = self.connection.cursor()
        cursor.execute("select `id` from `item` where `plus`= %s and `minus` = %s and `totalPlus` = %s and `totalMinus` = %s", (plus, minus, plusTot, minusTot))
        return cursor.fetchall()
    def getUsefulStockList(self, targetAt, period):
        cursor = self.connection.cursor()
        cursor.execute('select `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` from stock where much = 0 and id not in  '
                       '(select s.id from item i, stock s where s.id = i.stockId and i.targetAt = %s and i.period = %s) order by id asc', (targetAt, period))
        return cursor.fetchall()

    def selectItemByPeriodAndYet(self, period, yet):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE period = %s AND yet = %s LIMIT  1", (period, yet))
        return cursor.fetchone()
    def getPlusOneDay(self, limitAt):
         return limitAt + datetime.timedelta(days=1)
    def getTargetStartAt(self, targetAt, period):
        return targetAt - datetime.timedelta(days=period) - datetime.timedelta(days=period)

    def getWorkYetItems(self, forecastAt, period):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE period = %s AND yet = %s AND targetAt = %s", (period, self.WORK_YET, forecastAt))
        return cursor.fetchall()
    def selectItem(self, id):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE id=%s ORDER BY createdAt ASC", (id))
        return cursor.fetchone()
    def insertContentMap(self, contentId, wordId):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`content_map` (`contentId`, `wordId`) VALUES (%s, %s)", (contentId, wordId))

    def getContentWordIds(self, contentId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, contentId, wordId FROM content_map WHERE contentId=%s", (contentId))
        return cursor.fetchall()

    def updateContentYet(self, contentId, yet):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `data`.`content` SET `yet`=%s WHERE `id`=%s", (yet, contentId))
        self.commit()

    def putContentWordId(self, contentId, contentData):
        self.updateContentYet(contentId, self.WORK_DONE)
        splitWords = self.splitStr(contentData)
        for targetWord in splitWords:
            existTargetWord = self.existSplitWord(targetWord)
            if existTargetWord :
                wordId = self.getWordByStr(targetWord)
                self.insertContentMap(contentId, wordId)
    def getContentWordIdAndWork(self, contentId, yet):
        if yet == self.WORK_YET:
            content = self.getContentById(contentId)
            if content.get('yet') == self.WORK_YET :
                self.updateContentYet(contentId, self.WORK_DONE)
                splitWords = self.splitStr(content.get('contentData'))
                for targetWord in splitWords:
                    existTargetWord = self.existSplitWord(targetWord)
                    if existTargetWord :
                        wordId = self.getWordByStr(targetWord)
                        self.insertContentMap(contentId, wordId)
        wordIds = []
        for contentWordId in self.getContentWordIds(contentId):
            wordIds.append(contentWordId.get('wordId'))
        return wordIds

    def getContentById(self, contentId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, contentData, yet FROM content WHERE id=%s", (contentId))
        return cursor.fetchone()
    def dailyAll(self):
        for period in self.getPeriodAll():
            forecastAt = date.today() + timedelta(days=period)
            self.dailyRun(forecastAt, period)
    def getPeriodAll(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT distinct(period) FROM item")
        results = list()
        for each in cursor.fetchall():
            results.append(each.get('period'))
        return results

    def updateAllStockFinance(self):
        stocks = self.getStocks()
        for stock in self.getStockList():
            stocks.insertNewStock(stock.get('code'))
            items = self.selectItemByFinanceIsNull(stock.get('id'))
            for item in items :
                finances = self.selectFinanceByStockIdAndDate(stock.get('id'), item.get('targetAt'))
                for finance in finances :
                    print('update item finance id ', stock.get('name'), item.get('targetAt'))
                    self.updateItemFinanceId(finance.get('id'), item.get('id'))
        self.updateKospiPrice()
        self.commit()
    def selectLastestFinance(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT date FROM finance WHERE stockId =%s ORDER BY date DESC LIMIT 1", stockId)
        result = cursor.fetchone()
        if result is not None :
            return result.get('date')
        return None
    def updateKospiPrice(self):
        stock = self.selectStockByCode(self.KOSPI_CODE)
        lastestDate = self.selectLastestFinance(stock.get('id'))
        priceInfos = KakaoStock().getPriceInfos(stock.get('code'), lastestDate)
        for each in priceInfos:
            exist = self.selectFinanceByStockIdAndDate(stock.get('id'), each.get('date'))
            if exist is None :
                self.insertFinance(stock.get('id'), each.get('date'), high=0, low=0, start=each.get('start'), final=each.get('final'))

    def getStocks(self):
        if self.stocks is None :
            self.stocks = DSStock()
            self.stocks.updateStockInfo()
            self.updateAllStockFinance()
        return self.stocks

    def selectStockByCode(self, stockCode):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`code`,`name`,`lastUseDateAt`, `hit`, `lastScrapAt` FROM `stock` WHERE `code` like %s", ('%'+stockCode))
        return cursor.fetchone()
    def updateStockName(self, stockId, dsName, dsCode):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `stock` SET `name`=%s, `code`=%s WHERE `id`=%s", (dsName, dsCode, stockId))
    def selectContentIdList(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id FROM content WHERE stockId=%s", stockId)
        return cursor.fetchall()
    def getAllStockList(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt`, `much` FROM stock ORDER BY id ASC")
        return cursor.fetchall()
    def getDivideNumPercent(self, num1, num2):
        if num2 == 0:
            return 0
        return int((num1 / num2) * 100)
    def getDivideNumPercentFloat(self, num1, num2):
        if num2 == 0:
            return 0
        return round(float(((num1 / num2) * 100) - 100), 2)
    def getFinanceListFromItemId(self, itemId):
        cursor = self.connection.cursor()
        financeIds = cursor.execute('SELECT distinct(financeId) FROM chart_map WHERE itemId=%s', (itemId))
        results = []
        if financeIds != 0 :
            for each in cursor.fetchall() :
                results.append(each.get('financeId'))
                results = list(set(results))
        return results
    def analyzedSql(self, stockName, period):
        cursor = self.connection.cursor()
        selectAnalyzedSql = 'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final FROM item i, stock s, finance f WHERE i.stockId = s.id AND f.id = i.financeId AND s.name = %s AND i.period = %s GROUP BY i.targetAt ORDER BY i.targetAt DESC'
        cursor.execute(selectAnalyzedSql, (stockName, period))
        analyzedResult = cursor.fetchall()
        return analyzedResult
    def getFinanceDataIn(self, financeIdList):
        if len(financeIdList) == 0 :
            return []
        cursor = self.connection.cursor()
        selectFinanceQuery = 'SELECT f.id, f.start, f.final, f.date, f.createdAt FROM finance f WHERE f.id IN (%s)'
        inquery = ', '.join(list(map(lambda x: '%s', financeIdList)))
        selectFinanceQuery = selectFinanceQuery % inquery
        cursor.execute(selectFinanceQuery, financeIdList)
        return cursor.fetchall()
    def getFinanceDataMap(self, financeIdList):
        chanceIds = []
        dangerIds = []
        prices = []
        financeDataList = self.getFinanceDataIn(financeIdList)
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
    def getAnalyzeExistData(self, stockName, period):
        analyzedResult = self.analyzedSql(stockName, period)
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
            financeList = self.getFinanceListFromItemId(itemId)
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
    def getFinancePercent(self, stockName, targetAt):
        result = self.getFinanceDataByStockNameAndData(stockName=stockName, sliceDate=targetAt)
        return self.getDivideNumPercentFloat(result.get('final'), result.get('start'))
    def checkDefence(self, stockName, targetAt):
        try :
            return self.getFinancePercent(stockName, targetAt) > self.getFinancePercent(self.KOSPI_NAME, targetAt)
        except:
            return False
    def insertStockPotential(self, stockId, period, potential, count):
        cursor = self.connection.cursor()
        cursor.execute("select id from potential where stockId = %s and period = %s", (stockId, period))
        result = cursor.fetchone()
        if result is not None :
            cursor.execute("update potential set potential = %s, count = %s where id = %s ", (potential, count, result.get('id')))
        else :
            cursor.execute("INSERT INTO potential (`stockId`,`period`,`potential`, `count`) VALUES (%s, %s, %s, %s)", (stockId, period, potential, count))
    def updatePotentialStock(self, stock, period):
        r1, r2 = self.getAnalyzeExistData(stock.get('name'), period)
        pd = r2.get(stock.get('name'))
        potential = pd.get('potential')
        count = pd.get('total')
        self.insertStockPotential(stock.get('id'), period, potential, count)
        print(stock, potential, 'is done')
    def selectPotentialStock(self, stockId, period):
        cursor = self.connection.cursor()
        cursor.execute("select stockId, potential, count from potential where stockId = %s and period = %s", (stockId, period))
        return cursor.fetchone()
    def updateStockMuch(self, stockId, much):
        cursor = self.connection.cursor()
        cursor.execute("update stock set much = %s where id = %s ", (much, stockId))
    def filterPotentialStock(self, periods):
        for period in periods :
            for stock in self.getAllStockList():
                self.updatePotentialStock(stock, period)
                poten = self.selectPotentialStock(stock.get('id'), period)
                if poten.get('count') > self.GUARANTEE_COUNT and poten.get('potential') < self.FILTER_LIMIT and stock.get('much') == 0 :
                    self.updateStockMuch(stock.get('id'), 1)
                    print(stock, ' set much 1. ', poten.get('potential'))
        self.commit()
    def getFilteredForecastResult(self, each):
        plus = each.get('plus')
        minus = each.get('minus')
        point = self.getDivideNumPercent(plus, plus + minus)
        stockName = each.get('name')
        targetAt = each.get('targetAt')
        return each.get('id'), point, stockName, targetAt
    def getItem(self, itemId):
        cursor = self.connection.cursor()
        cursor.execute("select i.id, i.stockId, i.financeId, i.targetAt from item i where i.id = %s", (itemId))
        return cursor.fetchone()
    def getBeforeFinanceId(self, itemId):
        cursor = self.connection.cursor()
        item = self.getItem(itemId)
        cursor.execute("select * from finance where stockId = %s and date < %s order by date desc limit 1", (item.get('stockId'), item.get('targetAt')))
        result = cursor.fetchone()
        if result is not None :
            return result.get('id')
        else:
            return None
    def getBeforeFinanceResult(self, itemId):
        financeId = self.getBeforeFinanceId(itemId)
        return self.getFinancePrice(financeId)
    def getAnalyzedTarget(self, itemId, plusChanceIds, point, pointDict, stockName, targetAt, stockCode):
        financeList = self.getFinanceListFromItemId(itemId)
        chanceIds = []
        for chanceId in plusChanceIds:
            if chanceId in financeList:
                chanceIds.append(chanceId)
        financeResult = self.getBeforeFinanceResult(itemId)
        return {'name':stockName, stockName: point, 'period' : pointDict.get(stockName).get('period'), 'potential': pointDict.get(stockName).get('potential'),
                'total': pointDict.get(stockName).get('total'), 'targetAt': targetAt.day,'chance': chanceIds, 'yesterday': financeResult, 'code':stockCode}

    def getForecastResult(self, stockName, limitAt, period):
        cursor = self.connection.cursor()
        selectForecastSql =  'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt FROM item i, stock s ' \
                             'WHERE i.stockId = s.id AND s.name = %s AND i.targetAt >= %s AND i.period = %s AND i.financeId IS NULL ORDER BY i.id DESC' # AND i.financeId IS NULL
        cursor.execute(selectForecastSql, (stockName, limitAt, period))
        return cursor.fetchall()
    def getFilteredTarget(self, plusChanceIds, pointDict, stock, period, startAt):
        filteredTargets = []
        forecastResults = self.getForecastResult(stock.get('name'), startAt, period)
        for each in forecastResults:
            itemId, point, stockName, targetAt = self.getFilteredForecastResult(each)
            analyzedTargetData = self.getAnalyzedTarget(itemId, plusChanceIds, point, pointDict, stockName, targetAt, stock.get('code'))
            filteredTargets.append(analyzedTargetData)
        return filteredTargets
    def selectItemByFinanceIsNull(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, targetAt FROM item WHERE stockId = %s AND financeId is NULL", (stockId))
        return cursor.fetchall()
    def filteredTarget(self, limitAt):
        targetList = list()
        results = list()
        results.append(str(self.selectSitePerCount(date.today(), limitAt)))
        for period in self.getPeriodAll():
            filterdList = list()
            for stock in self.getStockList():
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
    def printList(self, results):
        results.sort()
        msg = 'targetAt period potential per chance\n'
        for result in results :
            msg += str(result) + "\n"
        return msg

    def insertEventWord(self, itemId, wordIds):
        for wordId in wordIds:
            if self.hasEventWord(itemId, wordId):
                self.insertEvent(itemId, wordId)

    def hasEventWord(self, itemId, wordId):
        cursor = self.connection.cursor()
        cursor.execute("select count(id) as c from data.event where itemId = %s and wordId = %s", (itemId, wordId))
        cnt = cursor.fetchone().get('c')
        return cnt == 0

    def insertEvent(self, itemId, wordId):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`event` (`itemId`, `wordId`) VALUES (%s, %s)", (itemId, wordId))

    def selectSitePerCount(self, startAt, endAt):
        cursor = self.connection.cursor()
        cursor.execute("select site, count(id) from content where date between %s and date %s group by site", (startAt, endAt))
        return cursor.fetchall()


    def getPotentialDatas(self, target_at, limitRate):
        cursor = self.connection.cursor()
        query = "SELECT ds.name, f.type, f.code, f.analyzeAt, f.potential, f.volume , f.percent, f.evaluate FROM data.forecast f, data.daily_stock ds " \
                "WHERE f.type = 3 AND ds.code = f.code AND analyzeAt > %s and potential > %s group by f.id ORDER BY f.analyzeAt, f.code ASC"
        cursor.execute(query, (target_at, str(limitRate)))
        return cursor.fetchall()

    def compare_yesterday(self, code, analyze_at):
        cursor = self.connection.cursor()
        cursor.execute("select ds.id as id, (ds.close-ds.open) as compare from data.daily_stock ds where ds.code = %s and ds.date < %s order by ds.id desc limit 1", (code, analyze_at))
        return cursor.fetchone()

    def is_compare_chain_minus(self, code, analyze_at, day_cnt):
        cursor = self.connection.cursor()
        cursor.execute("select date from data.daily_stock ds "
        "where ds.code = %s and ds.date < %s order by ds.id desc limit %s", (code, analyze_at, day_cnt))
        dates = cursor.fetchall()

        result = True
        for date in dates:
            cursor.execute("select (ds.close-ds.open) as compare from data.daily_stock ds where ds.code = %s and ds.date = %s",
                (code, date.get('date')))
            compare = cursor.fetchone().get('compare')
            if compare > 0:
                result = False
        return result
    def get_max_target_at(self):
        cursor = self.connection.cursor()
        cursor.execute("select max(evaluate) as evaluateMax from data.forecast")
        evaluateMax = cursor.fetchone().get('evaluateMax')
        cursor.execute("select analyzeAt from data.forecast group by analyzeAt order by analyzeAt desc limit %s",
                       (evaluateMax))
        results = cursor.fetchall()
        if len(results) >= evaluateMax:
            return results[evaluateMax-1].get('analyzeAt')
        return datetime.date.today()
    def getPotential(self, target_at, chan_minus):
        datas = self.getPotentialDatas(target_at, self.LIMIT_RATE)
        msg = ''
        for data in datas:
            compare = self.is_compare_chain_minus(data.get('code'), data.get('analyzeAt'), chan_minus)
            if compare:
                msg += (data.get('analyzeAt').strftime("%Y-%m-%d")
                        + ' [' + str(data.get('evaluate'))
                        + '] [' + data.get('code')
                        + '] [' + data.get('name')
                        + '] [' + str(data.get('type'))
                        + '] [' + str(data.get('potential'))
                        + '] [' + str(data.get('volume'))
                        + '] [' + str(data.get('percent'))
                        + ']\n')
        return msg

    def now_progress(self):
        daily_cnt = self.get_daily_stock_count(date.today())
        forecast_cnt = self.get_calculated_forecast_count(date.today())
        return {'daily_cnt': daily_cnt, 'forecast_cnt': forecast_cnt}

    def get_daily_stock_count(self, date):
        cursor = self.connection.cursor()
        cursor.execute('select count(*) as cnt from daily_stock where date = %s', date)
        return cursor.fetchone().get('cnt')

    def get_calculated_forecast_count(self, date):
        cursor = self.connection.cursor()
        cursor.execute('select count(*) as cnt from forecast where analyzeAt = %s', date)
        return cursor.fetchone().get('cnt')


import configparser

cf = configparser.ConfigParser()
cf.read('config.cfg')

DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')
VALID_USER = 60403284

TOKEN = cf.get('telegram', 'TOKEN')
LINK3 = '/day_candles.json?limit='
LINK4 = '&to='
LINK1 = cf.get('KAKAO_STOCK', 'link1')
run = Runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
updater = Updater(TOKEN)

command = 'forecast'
if len(sys.argv) > 1:
    command = sys.argv[1]


if command == 'daily':
    run.filterPotentialStock(periods=run.getPeriodAll())
    run.dailyAll()
elif command == 'stock':
    try :
        run.getStocks()
    except DSStockError :
#        updater.bot.sendMessage(chat_id=VALID_USER, text=str('disconnect stock.'))
        print('disconnect')
elif command == 'migrate':
    run.migrationWork(periods=run.getPeriodAll())
elif command == 'forecast':
#    updater.bot.sendMessage(chat_id=VALID_USER, text= run.filteredTarget(date.today()+timedelta(days=max(run.getPeriodAll()))))
    updater.bot.sendMessage(chat_id=VALID_USER, text= run.getPotential(target_at= run.get_max_target_at() - timedelta(days=1), chan_minus=1))
else :
    print('invalid command')







