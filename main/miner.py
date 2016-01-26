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
        self.LIMIT = 5
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
        self.TODAY = date.today()
        self.LIMIT_PAST_DATE = self.TODAY - timedelta(days=3 * 365)

    def finalize(self):
        self.connection.commit()
        self.connection.close()

    def getCountContent(self, content, date):
        cursor = self.connection.cursor()
        countCursor = cursor.execute("select count(*) as c from content where query = %s and date > %s", #TODO -- between 사용.
                                     (content, date))
        if countCursor != 0:
            return cursor.fetchone().get('c')
        else:
            return 0

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

    def getStockNameContent(self, stockName, limitDate):
        contentsList = []
        count = self.getCountContent(stockName, limitDate)
        for i in range(int((count / self.LIMIT)) + 1):
            try:
                contents = self.getContent(stockName, (i * 10) + 1, (i + 1) * self.LIMIT)  # paging.
                contentsList = contentsList + contents
            except MinerError:
                print('data is empty.')
                continue
        return contentsList

    def work(self, stockName, period):
        contents = self.getStockNameContent(stockName, self.LIMIT_PAST_DATE)
        wordPriceMap = self.getWordChangePriceMap(contents, stockName, period)
        return wordPriceMap

    def getTargetContentWords(self, stockName, date):
        contents = self.getStockNameContent(stockName, date)
        words = []
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
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

    def getAnalyzedCnt(self, period, stockName): #TODO - period 를 between 으로 찾자 , 한번에 여러 데이터를 넣어놓자.
        targetWords = self.getTargetContentWords(stockName, date.today() - timedelta(days=period))
        totalWordPriceMap = self.work(stockName, period)
        resultWordPriceMap = self.getWordPriceMap(targetWords, totalWordPriceMap)
        targetChartList = self.getAnalyzedChartList(resultWordPriceMap)
        totalChartList = self.getAnalyzedChartList(totalWordPriceMap)

        targetPlusCnt, targetMinusCnt = self.getAnalyzedCountList(targetChartList)
        totalPlusCnt, totalMinusCnt = self.getAnalyzedCountList(totalChartList)
        targetPlusAvg, targetMinusAvg = self.getAnalyzedAvgChartList(targetChartList)
        plusCnt = 0
        if totalPlusCnt != 0:
            plusCnt = (targetPlusCnt / totalPlusCnt)
        minusCnt = 0
        if totalMinusCnt != 0:
            minusCnt = (targetMinusCnt / totalMinusCnt)
        return plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg
