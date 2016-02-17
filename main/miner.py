import numpy
from datetime import timedelta
import multiprocessing
import threading
import pymysql.cursors
import dictionary


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
        self.ASYNC_ARRAY_NUM = 3000
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
        for i in range(int((count / self.LIMIT_COUNT)) + 1):
            try:
                contentCursor = cursor.execute("SELECT c.title,c.contentData, a.name, c.date FROM content as c, author as a " + conditionQuery + " LIMIT %s , %s",
                                               (stockName, limitAt, startAt, (i * 10) + 1, (i + 1) * self.LIMIT_COUNT))
                if contentCursor != 0:
                    contents = cursor.fetchall()
                    contentsList = contentsList + contents
                else:
                    raise MinerError('content is not valid.')
            except MinerError:
                print('data is empty.')
                continue
        return contentsList
    def getTargetContentWords(self, stockName, targetDate, periodDate):
        words = []
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH) #TODO -- multi .
        contents = self.getStockNameContent(stockName, targetDate, periodDate)
        for result in contents:
            contentData = result.get(self.CONTENT_DATA_NAME)
            splitWords = dic.splitStr(contentData)
            for target in splitWords:
                if dic.existSplitWord(target):
                    word = dic.getWordByStr(target)
                    words.append(word)
        return words

    def getWordPriceMap(self, words, totalWordPrices):
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
        for i in range(list) :
            idx = i*2
            if (idx + 1) >= len(list) :
                break
            split = list[idx:idx+1]
            newList.append(numpy.mean(split))
        return newList


    def getAnalyzedChartList(self, wordMap):
        chartList = []
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
            print('found new finance data. ' + str(sliceDate) + stockName)
        cursor = self.connection.cursor()
        financeCursor = cursor.execute("SELECT s.name, f.start, f.final, f.date FROM finance f, stock s WHERE f.stockId = s.id and s.name = %s and f.date like %s", (stockName, sliceDate + "%"))
        if financeCursor != 0:
            finance = cursor.fetchone()  # one ? many?
            stockPrice = int(finance.get(self.START_NAME)) - int(finance.get(self.FINAL_NAME))
            cacheFinanceChangePrices[str(sliceDate) + stockName] = stockPrice
            return stockPrice
        else:
            cacheFinanceChangePrices[str(sliceDate) + stockName] = None
            print('finance data not found.' + sliceDate)

    def getWordChangePriceMap(self, contentDataList, stockName, period, lock, queue):
        wordDataMap = {}
        cacheFinanceChangePrices = {}
        print('getWordChangePriceMap, len ', len(contentDataList))
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        for idx in range(len(contentDataList)):
            result = contentDataList[idx]
            print(idx,'/',len(contentDataList))
            contentData = result.get(self.CONTENT_DATA_NAME)
            date = result.get(self.DATE_NAME)
            sliceDate = (date + timedelta(days=period)).strftime('%Y-%m-%d')
            lock.acquire()
            try :
                change = self.getFinanceChangePrice(sliceDate, stockName, cacheFinanceChangePrices)
            finally :
                lock.release()
            if change is None:
                continue
            splitWords = dic.splitStr(contentData)
            for target in splitWords:
                if dic.existSplitWord(target):
                    word = dic.getWordByStr(target)
                    try:
                        wordDataMap[word].append(change)
                    except KeyError:
                        wordDataMap[word] = [change]
        queue.put(wordDataMap)

    def multiThreadWordChangePriceMap(self, contents, stockName, period):
        queueList = []
        threadList = []
        for idx in range(int(len(contents) / self.ASYNC_ARRAY_NUM) + 1) :
            start = idx * self.ASYNC_ARRAY_NUM
            end = idx * self.ASYNC_ARRAY_NUM + self.ASYNC_ARRAY_NUM
            if end > len(contents) :
                end = len(contents)
            splitedContentTarget = contents[start : end]
            queue = multiprocessing.Queue()
            lock = threading.Lock()
            thread = threading.Thread(target=self.getWordChangePriceMap, args=(splitedContentTarget, stockName, period, lock, queue))
            thread.start()
            threadList.append(thread)
            queueList.append(queue)
        totalWordChangePriceMap = {}

        for thread in threadList :
            thread.join()

        for queue in queueList :
            wordMap = queue.get()
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
