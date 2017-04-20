import datetime
import queue
import sys
import threading
from datetime import timedelta
import numpy

import dbmanager
import dictionary


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
        self.SPLIT_COUNT = 1000
        self.START_NAME = 'start'
        self.FINAL_NAME = 'final'
        self.DATE_NAME = 'date'
        self.LIMIT_YEAR_SEPERATOR = 5
        self.INTERVAL_YEAR_SEPERATOR = 73
        self.FINANCE_NAME = 'finance'
        self.dbm = dbmanager.DBManager()
        self.dic = dictionary.Dictionary()
        self.THREAD_LIMIT_COUNT = 4

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
        cnt = self.dbm.countContents(stockId, startAt, limitAt)
        if cnt is not None:
            count = cnt.get('cnt')
            if count == None :
                print('count is None', stockName, startAt, limitAt)
                return []
        for i in range(int((count / self.LIMIT_COUNT)) + 1):
            try:
                startPos = (i * 10) + 1
                endPos = (i + 1) * self.LIMIT_COUNT
                contents = self.dbm.getContentBetween(stockId, startAt, limitAt, startPos, endPos)
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
                cacheFinanceChangePrices[str(sliceDate) + stockName] = financeId
                return financeId
            else:
                cacheFinanceChangePrices[str(sliceDate) + stockName] = None
                return None
        except :
            print('except', stockName, sliceDate, len(cacheFinanceChangePrices))
            print("miner unexpected error:", sys.exc_info())
            return None

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
        totalWordIdFinanceMap = {}
        start = datetime.datetime.now()
        firstAt = self.dbm.selectFirstContentDate(stockId)
        contents = self.getStockNameContent(stockName, firstAt, targetDate, stockId)
        wordIdFinanceMap = self.multiThreadWordChangePriceMap(contents, stockName, period)
        self.appendWordPriceMap(wordIdFinanceMap, totalWordIdFinanceMap)

        targetStartAt = self.dbm.getTargetStartAt(targetDate, period)
        targetWordIds = self.getTargetContentWordIds(stockName, targetStartAt, targetDate, stockId)

        resultWordFinanceMap = self.getWordFinanceMap(targetWordIds, totalWordIdFinanceMap)
        targetChartList = self.getAnalyzedChartList(resultWordFinanceMap)
        totalChartList = self.getAnalyzedChartList(totalWordIdFinanceMap)
        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetFinanceIdList = self.getFinanceIdList(targetChartList)
        end = datetime.datetime.now()
        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetChartList, totalChartList, targetFinanceIdList, end - start


    def getFinanceIdList(self, chartList):
        financeIdList = []
        for chart in chartList :
            financeIds = chart.get(self.FINANCE_NAME)
            financeIdList = financeIdList + financeIds
        return financeIdList

    def getContentWordIdAndWork(self, contentId, yet):
        print(contentId, yet)
        if yet == self.dbm.WORK_YET:
            content = self.dbm.getContentById(contentId)
            if content.get('yet') == self.dbm.WORK_YET :
                print('yet')
                self.dbm.updateContentYet(contentId, self.dbm.WORK_DONE)
                splitWords = self.dic.splitStr(content.get('contentData'))
                for targetWord in splitWords:
                    existTargetWord = self.dic.existSplitWord(targetWord)
                    if existTargetWord :
                        wordId = self.dic.getWordByStr(targetWord)
                        self.dbm.insertContentMap(contentId, wordId)
        wordIds = []
        for contentWordId in self.dbm.getContentWordIds(contentId):
            wordIds.append(contentWordId.get('wordId'))
        return wordIds


