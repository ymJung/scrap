import numpy
from datetime import timedelta
import pymysql.cursors
import dictionary
import sys
from datetime import date
import threading
import queue


class MinerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class Miner:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.LIMIT_COUNT = 5
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.WORD_NAME = 'word'
        self.PLUS_NAME = 'plus'
        self.MINUS_NAME = 'minus'
        self.CONTENT_DATA_NAME = 'contentData'
        self.SPLIT_COUNT = 500
        self.START_NAME = 'start'
        self.FINAL_NAME = 'final'
        self.DATE_NAME = 'date'
        self.LIMIT_YEAR_SEPERATOR = 5
        self.INTERVAL_YEAR_SEPERATOR = 73
        self.FINANCE_NAME = 'finance'
    def commit(self):
        self.connection.commit()
    def getContent(self, stockName, startPos, endPos):
        cursor = self.connection.cursor()
        contentCursor = cursor.execute("SELECT `c`.`title`,`c`.`contentData`, `a`.`name`, `c`.`date` FROM `content` as `c`, `author` as `a` WHERE `c`.`query` = %s limit %s , %s",
            (stockName, startPos, endPos))
        if contentCursor != 0:
            return cursor.fetchall()
        else:
            raise MinerError('content is not valid.')

    def getStockNameContent(self, stockName, startAt, limitAt):
        contentsList = []
        count = 0
        cursor = self.connection.cursor()
        conditionQuery = ' WHERE c.query = %s and c.date between %s and %s'
        countCursor = cursor.execute("SELECT COUNT(c.id) as cnt FROM content c " + conditionQuery, (stockName, limitAt, startAt)) #TODO - 확인 , a 와 b ? b 와 a  ?
        if countCursor != 0:
            count = cursor.fetchone().get('cnt')
            if count == None :
                print('count is None', stockName, startAt, limitAt)
                return []
        for i in range(int((count / self.LIMIT_COUNT)) + 1):
            try:
                contentCursor = cursor.execute("SELECT c.title,c.contentData, a.name, c.date FROM content as c, author as a " + conditionQuery + " LIMIT %s , %s", (stockName, limitAt, startAt, (i * 10) + 1, (i + 1) * self.LIMIT_COUNT))
                if contentCursor != 0:
                    contents = cursor.fetchall()
                    contentsList = contentsList + contents
                else:
                    raise MinerError('content is not valid.')
            except MinerError:
                print('data is empty.')
                continue
            except MemoryError :
                print('memory error', stockName, len(contentsList))
                contentsList = self.divideList(contentsList)
                continue
        return contentsList
    def getTargetContentWordIds(self, stockName, targetDate, periodDate):
        wordIds = []
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        contents = self.getStockNameContent(stockName, targetDate, periodDate)
        print('target content word find. content length . ', len(contents))
        for result in contents:
            contentData = result.get(self.CONTENT_DATA_NAME)
            splitWords = dic.splitStr(contentData)
            for target in splitWords:
                if dic.existSplitWord(target):
                    wordId = dic.getWordByStr(target)
                    wordIds.append(wordId)
        dic.close()
        return wordIds

    def getWordFinanceMap(self, wordIds, totalWordFinances):
        print('get word price dictionary', len(wordIds))
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




    def getAnalyzedChartList(self, wordFinanceMap):
        chartList = []
        print('get analyzed chart list word map size : ', len(wordFinanceMap))
        for wordId in wordFinanceMap.keys():
            plusList = []
            minusList = []
            financeIdList = []
            for financeId in wordFinanceMap[wordId]:
                price = self.getPriceFromFinanceIds(financeId)
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

    def getFinanceChangeId(self, sliceDate, stockName, cacheFinanceChangePrices):
        try:
            return cacheFinanceChangePrices[str(sliceDate) + stockName]
        except KeyError:
            pass
        try :
            cursor = self.connection.cursor()
            financeCursor = cursor.execute("SELECT f.id FROM finance f, stock s WHERE f.stockId = s.id and s.name = %s and f.date = %s", (stockName, sliceDate))
            if financeCursor != 0:
                financeId = cursor.fetchone().get('id')  # one ? many?
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

    def getWordChangePriceMap(self, contentDataList, stockName, period,  queue, lock, dic):
        wordIdFinanceMap = {}
        cacheFinanceChangePrices = {}

        print('getWordChangePriceMap, len ', len(contentDataList))
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
            splitWords = dic.splitStr(contentData)
            for target in splitWords:
                lock.acquire()
                try :
                    existTargetWord = dic.existSplitWord(target)
                finally :
                    lock.release()
                if existTargetWord :
                    lock.acquire()
                    try :
                        wordId = dic.getWordByStr(target)
                    finally:
                        lock.release()
                    try:
                        wordIdFinanceMap[wordId].append(financeId)
                    except KeyError:
                        wordIdFinanceMap[wordId] = [financeId]
        print('put word data map to queue ', len(wordIdFinanceMap))
        queue.put(wordIdFinanceMap)

    def multiThreadWordChangePriceMap(self, contents, stockName, period):
        queueList = []
        threadList = []
        lock = threading.Lock()
        dic= dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)

        for idx in range(int(len(contents) / self.SPLIT_COUNT) + 1) :
            start = idx * self.SPLIT_COUNT
            end = idx * self.SPLIT_COUNT + self.SPLIT_COUNT
            if end > len(contents) :
                end = len(contents)
            splitedContentTarget = contents[start : end]

            resultQueue = queue.Queue()
            thread = threading.Thread(target=self.getWordChangePriceMap, args=(splitedContentTarget, stockName, period, resultQueue, lock, dic), name=stockName+str(len(splitedContentTarget)))
            threadList.append(thread)
            try :
                thread.start()
            except RuntimeError :
                if threadList :
                    # threadList[0].start()
                    threadList[0].join()
                    del threadList[0]
            queueList.append(resultQueue)
        totalWordPriceMap = {}
        for thread in threadList :
            print('thread', thread.getName())
            thread.join()

        for result in queueList :
            wordIdFinanceMap = result.get()
            self.appendWordPriceMap(wordIdFinanceMap, totalWordPriceMap)

        return totalWordPriceMap
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

    def getAnalyzedCnt(self, targetDate, period, stockName):
        today = date.today()
        totalWordIdFinanceMap = {}

        for idx in range(self.LIMIT_YEAR_SEPERATOR) : # 73 * 5
            interval = idx * self.INTERVAL_YEAR_SEPERATOR
            contents = self.getStockNameContent(stockName, today - timedelta(days=interval), today - timedelta(days=interval + self.INTERVAL_YEAR_SEPERATOR)) #TODO - 무조건 전부말고 시점 별로.
            wordIdFinanceMap = self.multiThreadWordChangePriceMap(contents, stockName, period)
            self.appendWordPriceMap(wordIdFinanceMap, totalWordIdFinanceMap)

        targetWordIds = self.getTargetContentWordIds(stockName, targetDate, targetDate - timedelta(days=period))

        resultWordFinanceMap = self.getWordFinanceMap(targetWordIds, totalWordIdFinanceMap)
        targetChartList = self.getAnalyzedChartList(resultWordFinanceMap)
        totalChartList = self.getAnalyzedChartList(totalWordIdFinanceMap)
        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetFinanceIdList = self.getFinanceIdList(targetChartList)

        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList

    def close(self):
        print('miner close')
        self.connection.close()

    def getPriceFromFinanceIds(self, financeId):
        # prices = []
        # for financeId in financeIds :
        price = self.getFinancePrice(financeId)
        return price
        # prices.append(price)
        # return numpy.nan_to_num(prices)

    def getFinancePrice(self, financeId):
        cursor = self.connection.cursor()
        cursor.execute("select start, final from finance where id = %s", financeId)
        result = cursor.fetchone()
        price = result.get('start') - result.get('final')
        return price

    def getFinanceIdList(self, chartList):
        financeIdList = []
        for chart in chartList :
            financeIds = chart.get(self.FINANCE_NAME)
            financeIdList = financeIdList + financeIds
        return financeIdList


