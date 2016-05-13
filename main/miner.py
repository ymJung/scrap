import numpy
from datetime import timedelta

import dictionary
import sys
from datetime import date
import threading
import queue
import dbmanager
import configparser

class MinerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class Miner:
    def __init__(self):
        self.LIMIT_COUNT = 5
        self.WORD_NAME = 'word'
        self.PLUS_NAME = 'plus'
        self.MINUS_NAME = 'minus'
        self.CONTENT_DATA_NAME = 'contentData'
        self.SPLIT_COUNT = 1000
        self.START_NAME = 'start'
        self.FINAL_NAME = 'final'
        self.DATE_NAME = 'date'
        self.LIMIT_YEAR_SEPERATOR = 5
        self.INTERVAL_YEAR_SEPERATOR = 73
        self.FINANCE_NAME = 'finance'
        self.dbm = dbmanager.DBManager()
        self.dic = dictionary.Dictionary()
        self.THREAD_LIMIT_COUNT = 30

    def __del__(self):
        self.dbm.commit()
        self.dbm.close()

    def getContent(self, stockId, startPos, endPos):
        contents = self.dbm.getContent(stockId, startPos, endPos)
        if contents is not None:
            return contents
        else:
            raise MinerError('content is not valid.')

    def getStockNameContent(self, stockName, startAt, limitAt, stockId):
        contentsList = []
        count = 0
        cnt = self.dbm.countContents(stockId, limitAt, startAt)

        if cnt is not None:
            count = cnt.get('cnt')
            if count == None :
                print('count is None', stockName, startAt, limitAt)
                return []
        for i in range(int((count / self.LIMIT_COUNT)) + 1):
            try:
                startPos = (i * 10) + 1
                endPos = (i + 1) * self.LIMIT_COUNT
                contents = self.dbm.getContentBetween(stockId, limitAt, startAt, startPos, endPos)
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
    def getTargetContentWordIds(self, stockName, targetDate, periodDate, stockId):
        wordIds = []
        contents = self.getStockNameContent(stockName, targetDate, periodDate, stockId)
        print('target content word find. content length . ', len(contents))
        for result in contents:
            contentData = result.get(self.CONTENT_DATA_NAME)
            splitWords = self.dic.splitStr(contentData)
            for target in splitWords:
                if self.dic.existSplitWord(target):
                    wordId = self.dic.getWordByStr(target)
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
                price = self.dbm.getFinancePrice(financeId)
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
            finance = self.dbm.getFinanceDataByStockNameAndData(stockName, sliceDate)
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

    def getWordChangePriceMap(self, contentDataList, stockName, period,  queue, lock, dic):
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
        dic = dictionary.Dictionary()

        for idx in range(int(len(contents) / self.SPLIT_COUNT) + 1) :
            start = idx * self.SPLIT_COUNT
            end = idx * self.SPLIT_COUNT + self.SPLIT_COUNT
            if end > len(contents) :
                end = len(contents)
            splitedContentTarget = contents[start : end]

            resultQueue = queue.Queue()
            thread = threading.Thread(target=self.getWordChangePriceMap, args=(splitedContentTarget, stockName, period, resultQueue, lock, dic), name=stockName+str(len(splitedContentTarget)))
            threadList.append(thread)
            queueList.append(resultQueue)

        totalWordPriceMap = self.getTotalPriceMap(queueList, threadList)

        return totalWordPriceMap

    def getTotalPriceMap(self, queueList, threadList):
        # try:
        #     thread.start()
        # except RuntimeError:
        #     if threadList:
        #         # threadList[0].start()
        #         threadList[0].join()
        #         del threadList[0]
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

    def getAnalyzedCnt(self, targetDate, period, stockName, stockId):
        today = date.today()
        totalWordIdFinanceMap = {}

        for idx in range(self.LIMIT_YEAR_SEPERATOR) : # 73 * 5
            interval = idx * self.INTERVAL_YEAR_SEPERATOR
            contents = self.getStockNameContent(stockName, today - timedelta(days=interval), today - timedelta(days=interval + self.INTERVAL_YEAR_SEPERATOR), stockId)
            wordIdFinanceMap = self.multiThreadWordChangePriceMap(contents, stockName, period)
            self.appendWordPriceMap(wordIdFinanceMap, totalWordIdFinanceMap)

        targetWordIds = self.getTargetContentWordIds(stockName, targetDate, targetDate - timedelta(days=period), stockId)

        resultWordFinanceMap = self.getWordFinanceMap(targetWordIds, totalWordIdFinanceMap)
        targetChartList = self.getAnalyzedChartList(resultWordFinanceMap)
        totalChartList = self.getAnalyzedChartList(totalWordIdFinanceMap)
        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetFinanceIdList = self.getFinanceIdList(targetChartList)

        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList


    def getFinanceIdList(self, chartList):
        financeIdList = []
        for chart in chartList :
            financeIds = chart.get(self.FINANCE_NAME)
            financeIdList = financeIdList + financeIds
        return financeIdList


