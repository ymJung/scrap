import numpy
from datetime import date, timedelta
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
        self.CHANGE_PRICE_LIST_NAME = 'changePriceList'
        self.WORD_NAME = 'word'
        self.PLUS_NAME = 'plus'
        self.MINUS_NAME = 'minus'
        self.CONTENT_DATA_NAME = 'contentData'
        self.DATE_NAME = 'date'
        self.START_NAME = 'start'
        self.FINAL_NAME = 'final'

    def finalize(self):
        self.connection.commit()
        self.connection.close()


    def getContent(self, stockName, startPos, endPos):
        cursor = self.connection.cursor()
        contentCursor = cursor.execute(
            "SELECT `c`.`title`,`c`.`contentData`, `a`.`name`, `c`.`date` FROM `content` as `c`, `author` as `a` WHERE `c`.`query` = %s limit %s , %s",
            (stockName, startPos, endPos))
        if contentCursor != 0:
            return cursor.fetchall()
        else:
            raise MinerError('content is not valid.')

    def getWordChangePriceMap(self, contentDataList, stockName, period):
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        wordDataMap = {}
        cacheFinanceChangePrices = {}
        for result in contentDataList:
            contentData = result.get(self.CONTENT_DATA_NAME)
            date = result.get(self.DATE_NAME)
            sliceDate = self.getTargetFinanceData(date, period)
            change = self.getFinanceChangePrice(sliceDate, stockName, cacheFinanceChangePrices)
            if change is None:
                continue

            splitWords = dic.splitStr(contentData)
            print('split words len ' + str(len(splitWords)))
            for target in splitWords:
                if dic.existSplitWord(target):
                    word = dic.getWordByStr(target)
                    try:
                        wordDataMap[word].append(change)
                    except KeyError:
                        wordDataMap[word] = [change]
        return wordDataMap

    def getFinanceChangePrice(self, sliceDate, stockName, cacheFinanceChangePrices):
        try:
            return cacheFinanceChangePrices[str(sliceDate) + stockName]
        except KeyError:
            print('found new finance data. ' + str(sliceDate) + stockName)
        cursor = self.connection.cursor()
        financeCursor = cursor.execute(
            "SELECT s.name, f.start, f.final, f.date FROM finance f, stock s WHERE f.stockId = s.id and s.name = %s and f.date like %s",
            (stockName, sliceDate + "%"))
        if financeCursor != 0:
            finance = cursor.fetchone()  # one ? many?
            stockPrice = int(finance.get(self.START_NAME)) - int(finance.get(self.FINAL_NAME))
            cacheFinanceChangePrices[str(sliceDate) + stockName] = stockPrice
            return stockPrice
        else:
            cacheFinanceChangePrices[str(sliceDate) + stockName] = None
            print('finance data not found.' + sliceDate)

    def getTargetFinanceData(self, date, period):
        # yyyy-MM-dd and plus period
        plusPeridDate = date + timedelta(days=period)
        sliceDate = plusPeridDate.strftime('%Y-%m-%d')
        return sliceDate

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

    def work(self, stockName, period):
        contents = self.getStockNameContent(stockName, date.today(), date.today() - timedelta(days=3 * 365))
        wordPriceMap = self.getWordChangePriceMap(contents, stockName, period)
        return wordPriceMap

    def getTargetContentWords(self, stockName, targetDate, periodDate):
        contents = self.getStockNameContent(stockName, targetDate, periodDate)
        words = []
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        for result in contents:
            contentData = result.get(self.CONTENT_DATA_NAME)
            splitWords = dic.splitStr(contentData)
            for target in splitWords:
                if dic.existSplitWord(target):
                    word = dic.getWordByStr(target)
                    words.append(word)
        dic.finalize()
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

    def getAnalyzedChartList(self, wordMap):
        chartList = []
        for word in wordMap.keys():
            plusList = []
            minusList = []
            for price in wordMap[word]:
                price = numpy.nan_to_num(price)
                if price > 0:
                    plusList.append(price)
                if price < 0:
                    minusList.append(price) # TODO - memory error.
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

        plusAvgList = list(set(plusAvgList))
        minusAvgList = list(set(minusAvgList))
        plusAvgList.sort(reverse=True)
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

    def getAnalyzedCnt(self, targetDate, period, stockName):
        targetWords = self.getTargetContentWords(stockName, targetDate, targetDate - timedelta(days=period))
        totalWordPriceMap = self.work(stockName, period)
        resultWordPriceMap = self.getWordPriceMap(targetWords, totalWordPriceMap)
        targetChartList = self.getAnalyzedChartList(resultWordPriceMap)
        totalChartList = self.getAnalyzedChartList(totalWordPriceMap)

        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetPlusAvg, targetMinusAvg = self.getAnalyzedAvgChartList(targetChartList) #TODO   warnings.warn("Mean of empty slice.", RuntimeWarning)

        return targetPlusCnt, targetMinusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg
