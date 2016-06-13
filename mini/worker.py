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
        self.WORK_DONE = 0
        self.WORK_YET = 1



    def migrationWork(self, period):
        while True :
            item = self.selectItemByPeriodAndYet(period, self.WORK_YET)
            if item is not None :
                # try :
                self.updateItemYet(item.get('id'), self.WORK_DONE)
                self.insertAnalyzedResult(item.get('stockId'), item.get('targetAt'), period)
                # except Exception :
                #     print('work is done.',  sys.exc_info())
                #     break
                # except :
                #     print("unexpect error.", sys.exc_info())
                #     break
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
    def dailyAll(self, forecastAt):
        for item in self.getPeriodAll() :
            self.dailyRun(forecastAt, item.get('period'))
    def getPeriodAll(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT distinct(period) FROM item")
        return cursor.fetchall()


run = Runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
# run.dailyAll(forecastAt=date.today() + timedelta(days=2))
# run.dailyRun(period = 3, forecastAt=date.today() + timedelta(days=3))
# run.migration(period,'')
run.migrationWork(period=3)


