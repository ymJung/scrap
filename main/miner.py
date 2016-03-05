import numpy
from datetime import timedelta
# import multiprocessing
#from multiprocessing import Process, freeze_support, Lock, Queue
import pymysql.cursors
import dictionary
import sys
import dbmanager
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
        countCursor = cursor.execute("SELECT COUNT(c.id) as cnt FROM content c " + conditionQuery, (stockName, limitAt, startAt))
        if countCursor != 0:
            count = cursor.fetchone().get('cnt')
            if count == None :
                print('count is None', stockName, startAt, limitAt)
                return []
        for i in range(int((count / self.LIMIT_COUNT)) + 1):
            try:
                contentCursor = cursor.execute("SELECT c.title,c.contentData, a.name, c.date FROM content as c, author as a " + conditionQuery + " LIMIT %s , %s",
                                               (stockName, limitAt, startAt, (i * 10) + 1, (i + 1) * self.LIMIT_COUNT))
                if contentCursor != 0:
                    contents = cursor.fetchall()
                    contentsList = contentsList + contents # TODO MEMORY ERROR
                else:
                    raise MinerError('content is not valid.')
            except MinerError:
                print('data is empty.')
                continue
            except MemoryError :
                print('memory error', stockName)
                break
        return contentsList
    def getTargetContentWords(self, stockName, targetDate, periodDate):
        words = []
        dic= dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        contents = self.getStockNameContent(stockName, targetDate, periodDate)
        print('target content word find. content length . ', len(contents))
        for result in contents:
            contentData = result.get(self.CONTENT_DATA_NAME)
            splitWords = dic.splitStr(contentData)
            for target in splitWords:
                if dic.existSplitWord(target):
                    word = dic.getWordByStr(target)
                    words.append(word)
        dic.close()
        return words

    def getWordPriceMap(self, words, totalWordPrices):
        print('get word price dictionary', len(words))
        wordPriceDict = {}
        for word in words:
            try:
                totalWordPrices[word]
            except KeyError:
                continue
            try:
                wordPriceDict[word] = wordPriceDict[word] + totalWordPrices[word] # TODO - memory error.
            except KeyError:
                wordPriceDict[word] = totalWordPrices[word]

        return wordPriceDict

    def divideAvgList(self, list):
        newList = []
        try :
            for i in range(len(list)) :
                idx = i*2
                if (idx + 1) >= len(list) :
                    break
                split = list[idx:idx+1]
                newList.append(numpy.mean(split))
            return newList
        except MemoryError :
            print('divideAvgList memory error', len(list))
            return self.divideAvgList(newList)




    def getAnalyzedChartList(self, wordMap):
        chartList = []
        print('get analyzed chart list word map size : ', len(wordMap))
        for word in wordMap.keys():
            plusList = []
            minusList = []
            for price in wordMap[word]:
                price = numpy.nan_to_num(price) # --
                try :
                    if price > 0:
                        plusList.append(price)
                    if price < 0:
                        minusList.append(price)
                except MemoryError as e :
                    print('Memory Error. getAnalyzedChartList')
                    print(e)
                    plusList = self.divideAvgList(plusList)
                    minusList = self.divideAvgList(minusList)
            chart = {self.WORD_NAME: word, self.PLUS_NAME: plusList, self.MINUS_NAME: minusList}
            chartList.append(chart)
        return chartList

    def getAnalyzedAvgChartList(self, chartList):
        plusAvgList = []
        minusAvgList = []
        for chart in chartList:
            plusWordList = chart[self.PLUS_NAME]
            minusWordList = chart[self.MINUS_NAME]

            plusAvg = numpy.nan_to_num(numpy.mean(plusWordList))
            plusAvgList.append(plusAvg)
            minusAvg = numpy.nan_to_num(numpy.mean(minusWordList))
            minusAvgList.append(minusAvg)

            print(chart[self.WORD_NAME]
                  + ': PLUS: ' + str(len(plusWordList)) + ' , PLUS_AVG: ' + str(plusAvg)
                  + ' , MINUS: ' + str(len(minusWordList)) + ' , MINUS_AVG: ' + str(minusAvg))
        if len(plusAvgList) > 0 :
            plusAvgList = list(set(plusAvgList))
            plusAvgList.sort(reverse=True)
        if len(minusAvgList) > 0 :
            minusAvgList = list(set(minusAvgList))
            minusAvgList.sort()
        print(plusAvgList)
        print(minusAvgList)
        return numpy.nan_to_num(numpy.mean(plusAvgList)), numpy.nan_to_num(numpy.mean(minusAvgList))

    def getAnalyzedCountList(self, chartList):
        plusCnt = 0
        minusCnt = 0
        for chart in chartList:
            plusCnt += len(chart.get(self.PLUS_NAME))
            minusCnt += len(chart.get(self.MINUS_NAME))
        return plusCnt, minusCnt

    def getFinanceChangePrice(self, sliceDate, stockName, cacheFinanceChangePrices):
        try:
            return cacheFinanceChangePrices[str(sliceDate) + stockName]
        except KeyError:
            pass
        try :
            cursor = self.connection.cursor()
            financeCursor = cursor.execute("SELECT s.name, f.start, f.final, f.date FROM finance f, stock s WHERE f.stockId = s.id and s.name = %s and f.date = %s", (stockName, sliceDate))
            if financeCursor != 0:
                finance = cursor.fetchone()  # one ? many?
                stockPrice = int(finance.get(self.START_NAME)) - int(finance.get(self.FINAL_NAME))
                cacheFinanceChangePrices[str(sliceDate) + stockName] = stockPrice
                return stockPrice
            else:
                cacheFinanceChangePrices[str(sliceDate) + stockName] = None
                return None
        except :
            print('except', stockName, sliceDate, len(cacheFinanceChangePrices))
            print("miner unexpected error:", sys.exc_info())
            return None

    def getWordChangePriceMap(self, contentDataList, stockName, period,  queue, lock, dic):
        wordDataMap = {}
        cacheFinanceChangePrices = {}

        print('getWordChangePriceMap, len ', len(contentDataList))
        for idx in range(len(contentDataList)):
            result = contentDataList[idx]
            contentData = result.get(self.CONTENT_DATA_NAME)
            date = result.get(self.DATE_NAME)
            sliceDate = (date + timedelta(days=period)).strftime('%Y-%m-%d')
            lock.acquire()
            try :
                change = self.getFinanceChangePrice(sliceDate, stockName, cacheFinanceChangePrices)
            finally:
                lock.release()
            if change is None:
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
                        word = dic.getWordByStr(target)
                    finally:
                        lock.release()
                    try:
                        wordDataMap[word].append(change)
                    except KeyError:
                        wordDataMap[word] = [change]
        print('put word data map to queue ', len(wordDataMap))
        queue.put(wordDataMap)

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
        totalWordChangePriceMap = {}

        for thread in threadList :
            print('thread', thread.getName())
            thread.join()

        for result in queueList :
            wordMap = result.get()
            for word in wordMap.keys() :
                if wordMap[word] == None :
                    continue
                try :
                    totalWordChangePriceMap[word] = totalWordChangePriceMap[word] + wordMap[word]
                except KeyError :
                    totalWordChangePriceMap[word] = []
                    totalWordChangePriceMap[word] = totalWordChangePriceMap[word] + wordMap[word]
        return totalWordChangePriceMap
    def getAnalyzedCnt(self, targetDate, period, stockName, contents):
        totalWordPriceMap = self.multiThreadWordChangePriceMap(contents, stockName, period)
        # totalWordPriceMap = self.getWordChangePriceMap(contents, stockName, period)
        targetWords = self.getTargetContentWords(stockName, targetDate, targetDate - timedelta(days=period))

        resultWordPriceMap = self.getWordPriceMap(targetWords, totalWordPriceMap)
        targetChartList = self.getAnalyzedChartList(resultWordPriceMap)
        totalChartList = self.getAnalyzedChartList(totalWordPriceMap)

        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetPlusAvg, targetMinusAvg = self.getAnalyzedAvgChartList(targetChartList)

        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg

    def close(self):
        print('miner close')
        self.connection.close()
