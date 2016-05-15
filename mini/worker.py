from datetime import date, timedelta
import pymysql.cursors
import datetime
import threading
import queue
import numpy
import sys
import configparser
cf = configparser.ConfigParser()
cf.read('config.cfg')
DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')

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
        self.THREAD_LIMIT_COUNT = 30
        self.CONTENT_DATA_NAME = 'contentData'
        self.MIN_WORD_LEN = 2
        self.MAX_WORD_LEN = 50
        self.WORD_NAME = 'word'
        self.PLUS_NAME = 'plus'
        self.MINUS_NAME = 'minus'
        self.FINANCE_NAME = 'finance'
        self.DATE_NAME = 'date'


    def migrationWork(self, period):
        for stock in self.getStockList() :
            targetDate = self.getLastItemDate(stock.get('id'), period)
            limitDate = self.getFirstContentDate(stock.get('id'))
            print('migration', stock.get('name'), targetDate, limitDate)
            if limitDate is None :
                print('content is none.')
                continue
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

    def insertAnalyzedResult(self, stock, targetAt, period):
        stockName = stock.get('name')
        forecastAt = targetAt + timedelta(days=period)
        if self.forecastTarget(forecastAt, stock, targetAt):
            return
        targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList = self.getAnalyzedCnt(targetAt, period, stockName, stock.get('id'))
        savedItemId = self.saveAnalyzedData(stockName, targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt,forecastAt, period)
        self.saveAnalyzedItemFinanceList(savedItemId, targetFinanceIdList)
        self.updateAnalyzedResultItem(stock)
        self.commit()
    def commit(self):
        self.connection.commit()
        print('dbm commit')
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

                # existFinance = cursor.execute(selectTargetStockSql + ' AND f.date > %s LIMIT 1', (stockName, itemTargetAt))
                # if existFinance != 0 :
                #     targetFinance = cursor.fetchone()
                #     self.updateItemPrice(targetFinance.get('id'), itemId)
    def updateItemFinanceId(self, financeId, itemId):
        cursor = self.connection.cursor()
        updateItemPriceSql = "UPDATE item SET financeId = %s WHERE id= %s"
        cursor.execute(updateItemPriceSql, (financeId, itemId))
    def saveAnalyzedItemFinanceList(self, itemId, financeIdList):
        cursor = self.connection.cursor()
        for financeId in financeIdList :
            cursor.execute("INSERT INTO chart_map (itemId, financeId) VALUES (%s, %s)", (itemId, financeId))
    def saveAnalyzedData(self, stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period):
        cursor = self.connection.cursor()
        cursor.execute('SELECT id FROM stock WHERE name = %s', stockName)
        stock = cursor.fetchone()
        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s LIMIT 1', (stock.get('id'), targetAt, period))
        item = cursor.fetchone()
        if item is None :
            cursor.execute("INSERT INTO `data`.`item` (`stockId`, `plus`, `minus`, `totalPlus`, `totalMinus`, `targetAt`, `period`) VALUES (%s, %s, %s, %s, %s, %s, %s)", (stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period))

        else :
            print(item)
        #     cursor.execute("UPDATE `data`.`item` SET `stockId`=%s, `plus`=%s, `minus`=%s, `totalPlus`=%s, `totalMinus`=%s, `targetAt`=%s, `period` WHERE `id`=%s",
        #                    (stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, item.get('id')))

        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s', (stock.get('id'), targetAt, period))
        return cursor.fetchone().get('id')

    def forecastTarget(self, forecastAt, stock, targetAt):
        stockId = stock.get('id')
        stockName = stock.get('name')
        lastScrapAt = stock.get('lastScrapAt')
        if self.checkHolyDay(forecastAt):
            print('holy day', stockName, forecastAt)
            return True
        if lastScrapAt is None or ((lastScrapAt.date() < targetAt) and (date.today() > targetAt)):
            print('not yet to scrap.', stockName, targetAt)
            return True
        cursor = self.connection.cursor()
        result = cursor.execute('SELECT id FROM item WHERE targetAt = %s and stockId = %s', (forecastAt, stockId))
        if result != 0:
            print('exist item date ', forecastAt, stockId)
            return True
        result = cursor.execute('SELECT `id` FROM `content` WHERE `date` BETWEEN %s AND %s AND `stockId` = %s', (targetAt - datetime.timedelta(days=period), targetAt + datetime.timedelta(days=1), stockId))
        if result == 0:
            print('empty content data.', targetAt, forecastAt, stockName)
            return True

        return False

    def getAnalyzedCnt(self, targetDate, period, stockName, stockId):
        totalWordIdFinanceMap = {}
        firstAt = self.selectFirstContentDate(stockId)
        contents = self.getStockNameContent(stockName, firstAt, targetDate, stockId)
        wordIdFinanceMap = self.multiThreadWordChangePriceMap(contents, stockName, period)
        self.appendWordPriceMap(wordIdFinanceMap, totalWordIdFinanceMap)

        targetStartAt = targetDate - timedelta(days=period)
        targetWordIds = self.getTargetContentWordIds(stockName, targetStartAt, targetDate, stockId)

        resultWordFinanceMap = self.getWordFinanceMap(targetWordIds, totalWordIdFinanceMap)
        targetChartList = self.getAnalyzedChartList(resultWordFinanceMap)
        totalChartList = self.getAnalyzedChartList(totalWordIdFinanceMap)
        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetFinanceIdList = self.getFinanceIdList(targetChartList)

        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList
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
                # contentsList = self.divideList(contentsList)
                # continue
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
    def getTargetContentWordIds(self, stockName, targetStartAt, targetDate, stockId):
        wordIds = []
        contents = self.getStockNameContent(stockName, targetStartAt, targetDate, stockId)
        print('target content word find. content length . ', len(contents))
        for result in contents:
            contentData = result.get(self.CONTENT_DATA_NAME)
            splitWords = self.splitStr(contentData)
            for target in splitWords:
                if self.existSplitWord(target):
                    wordId = self.getWordByStr(target)
                    wordIds.append(wordId)
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
        cursor.execute("SELECT COUNT(c.id) as cnt FROM content c WHERE c.stockId = %s and c.date between %s and %s", (stockId, startAt, limitAt))
        return cursor.fetchone()
    def getContentBetween(self, stockId, startAt, limitAt, startPos, endPos):
        cursor = self.connection.cursor()
        cursor.execute("SELECT c.title,c.contentData, c.date FROM content c WHERE c.stockId = %s and c.date > %s and c.date <= %s ORDER BY c.id DESC LIMIT %s , %s", (stockId, startAt, limitAt + timedelta(days=1), startPos, endPos))
        result = cursor.fetchall()
        if result is not None :
            return list(result)
        return result

    def getWordChangePriceMap(self, contentDataList, stockName, period,  queue, lock):
        wordIdFinanceMap = {}
        cacheFinanceChangePrices = {}

        for idx in range(len(contentDataList)):
            result = contentDataList[idx]
            contentData = result.get(self.CONTENT_DATA_NAME)
            date = result.get(self.DATE_NAME)
            sliceDate = (date + timedelta(days=period)).strftime('%Y-%m-%d')
            lock.acquire()
            try :
                financeId = self.getFinanceChangeId(sliceDate, stockName, cacheFinanceChangePrices)
            finally:
                lock.release()
            if financeId is None:
                continue
            splitWords = self.splitStr(contentData)
            for target in splitWords:
                lock.acquire()
                try :
                    existTargetWord = self.existSplitWord(target)
                finally :
                    lock.release()
                if existTargetWord :
                    lock.acquire()
                    try :
                        wordId = self.getWordByStr(target)
                    finally:
                        lock.release()
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
                print('thread start', thread.getName())
                thread.start()
            for thread in splitedThreads:
                print('thread join', thread.getName())
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

    def getUsefulStock(self, targetAt):
        cursor = self.connection.cursor()
        selectSql = "SELECT `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` FROM stock WHERE `use` = 1 AND `much` = 0 ORDER BY id asc LIMIT 1"
        cursor.execute(selectSql)
        stock = cursor.fetchone()
        if stock is None :
            today = datetime.date.today()
            cursor.execute("select id from stock where much = 0 and id not in (select s.id from item i, stock s where s.id = i.stockId and i.targetAt = %s)", today)
            done = cursor.fetchall()
            workIsDone = (len(done) == 0) #
            if workIsDone or self.checkHolyDay(targetAt):
                raise Exception('stock is none')
            else :
                print('init stock')
                self.initStock()
                cursor.execute(selectSql)
                stock = cursor.fetchone()
        self.updateStockUse(stock.get('id'), 0)
        return stock
    def initStock(self):
        self.connection.cursor().execute("UPDATE stock SET `use` = 1 WHERE `much` = 0")
        self.commit()
    def updateStockUse(self, stockId, useFlag):

        cursor = self.connection.cursor()
        cursor.execute(("UPDATE stock SET `use` = %s, `lastUseDateAt` = now() WHERE `id` = %s"), (useFlag, stockId))

        self.commit()
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
    def dailyRun(self, period, after):
        targetAt = date.today() + timedelta(days=after)
        while True :
            try :
                stock = self.getUsefulStock(targetAt)
                print(stock.get('name'), 'is start', targetAt)
                self.run(stock, targetAt, period)
                print(stock.get('name'), 'is done', targetAt)
            except Exception :
                print('work is done.')
                break
            except :
                print("unexpect error.", sys.exc_info())
                break
period = 2
run = Runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
run.dailyRun(period, 1)
#run.migration(period,'')
#run.migrationWork(period)
#run.initStock()
