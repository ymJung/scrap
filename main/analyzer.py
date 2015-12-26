DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

import pymysql.cursors
from datetime import date, timedelta
import re
from bs4 import BeautifulSoup
import urllib
from urllib.request import urlopen
import time
import random


class AnalyzerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class Analyzer:
    def __init__(self):
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def finalize(self):
        self.connection.commit()
        self.connection.close()

    def analyze(self):
        for target in self.targetMapList():
            print('start : ' + str(target.get('id')))
            self.analyzeDictionary(target.get('contentData'), target.get('id'), 0)
            self.updateAnalyzeFlag(target.get('id'), 'Y')
            print('fin : ' + str(target.get('id')))
            self.connection.commit()
        self.finalize()

    def targetMapList(self):
        cursor = self.connection.cursor()
        contentSelectSql = "SELECT `id`,`title`,`contentData`,`authorId`,`date`,`analyze`,`createdAt`" \
                           " FROM `content` WHERE `analyze`=%s"
        contentDataCursor = cursor.execute(contentSelectSql, ('N'))
        if contentDataCursor != 0:
            return cursor.fetchall()
        return []

    def updateAnalyzeFlag(self, contentId, analyzeFlag):
        cursor = self.connection.cursor()
        contentUpdateSql = "UPDATE `content` SET `analyze`=%s WHERE `id`=%s"
        cursor.execute(contentUpdateSql, (analyzeFlag, contentId))

    def insertDelimiter(self, data):
        cursor = self.connection.cursor()
        delimiterIdSelectSql = "SELECT `id` FROM `delimiter` WHERE `word`=%s"
        delimiterIdCursor = cursor.execute(delimiterIdSelectSql, (data))
        if delimiterIdCursor == 0:
            delimiterInsertSql = "INSERT INTO `delimiter` (`word`) VALUES (%s)"
            cursor.execute(delimiterInsertSql, (data))

        return delimiterIdCursor.fetchone().get('id')

    def analyzeDictionary(self, data, contentId, idx):
        dic = Dictionary()
        try:
            splitStrings = dic.splitStr(data)
            for i in range(len(splitStrings)):
                if idx > i:
                    print('start in middle ' + str(idx))
                    i = idx
                splitString = splitStrings[i]
                if dic.existSplitWord(splitString) is False:
                    findWord = False
                    for j in range(len(splitString)):
                        subStr = splitString[0:len(splitString) - j]
                        subStr = dic.getRegularExpression(subStr)
                        if dic.isTargetWord(subStr) and dic.isInsertTarget(subStr) is True:
                            dic.insertWord(subStr)
                            findWord = True

                    if findWord is False and dic.existWord(splitString) is False:
                        dic.insertGarbageWord(splitString, contentId)
                    idx = i
            idx = 0
        except urllib.error.URLError as e:
            print(e)
            print('retry analyzeDictionary ' + str(idx))
            dic.connection.commit()
            self.analyzeDictionary(data, contentId, idx)
        except:
            print('uncaught except')
            dic.connection.commit()
        finally:
            dic.connection.commit()


class Dictionary:
    def __init__(self):
        self.count = 0
        self.url = 'http://krdic.naver.com/small_search.nhn?kind=keyword&query='
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.MIN_WORD_LEN = 2
        self.MAX_WORD_LEN = 50
        self.REGULAR_EXP = '[^가-힝0-9a-zA-Z]'

    def finalize(self):
        self.connection.commit()
        self.connection.close()

    def splitStr(self, str):
        str = str.replace('\n', ' ')
        return str.split()

    def getDictionary(self, text):
        interval = random.randrange(300, 1200) / 1000
        time.sleep(interval)
        self.count += 1
        print(text + ' bs count(get) ' + str(self.count) + ' interval(sec) ' + str(interval))
        soup = BeautifulSoup(
            urllib.request.urlopen(self.url + urllib.parse.quote(text)), "lxml")
        return soup

    def existWord(self, data):
        cursor = self.connection.cursor()
        selectWordIdSql = "SELECT `id` FROM `word` WHERE `word`=%s"
        selectWordIdCursor = cursor.execute(selectWordIdSql, (data))
        if selectWordIdCursor != 0:
            return True
        else:
            return False

    def existSplitWord(self, fullWord):
        if self.getExistWordIdx(fullWord) > 0:
            return True
        return False

    def getExistWordIdx(self, fullWord):
        for i in range(len(fullWord) - self.MIN_WORD_LEN):
            if self.existWord(fullWord[0:i + self.MIN_WORD_LEN]):
                return i + self.MIN_WORD_LEN
        return 0

    def getWordByStr(self, fullWord):
        idx = self.getExistWordIdx(fullWord)
        if idx > 0:
            return fullWord[0:idx]
        else:
            return ''

    def existGarbageWord(self, data):
        cursor = self.connection.cursor()
        selectGarbageIdSql = "SELECT `id` FROM `garbage` WHERE `word`=%s"
        selectGarbageIdCursor = cursor.execute(selectGarbageIdSql, (data))
        if selectGarbageIdCursor != 0:
            return True
        else:
            return False

    def insertWord(self, data):
        cursor = self.connection.cursor()
        insertWordSql = "INSERT INTO `word` (`word`) VALUES (%s)"
        cursor.execute(insertWordSql, (data))
        print('insert word ' + data)

    def insertGarbageWord(self, data, contentId):
        data = self.getRegularExpression(data)
        if self.isTargetWord(data) and self.existGarbageWord(data) is False and self.existWord(data) is False:
            cursor = self.connection.cursor()
            insertGarbageSql = "INSERT INTO `garbage` (`word`,`contentId`) VALUES (%s, %s)"
            cursor.execute(insertGarbageSql, (data, contentId))
            print('insert garbage ' + data)

    def isTargetWord(self, text):
        text = self.getRegularExpression(text)
        if len(text) >= self.MIN_WORD_LEN and len(text) < self.MAX_WORD_LEN:
            return True
        return False

    def getRegularExpression(self, text):
        return re.sub(self.REGULAR_EXP, '', text).replace(' ', '')

    def isInsertTarget(self, text):
        text = self.getRegularExpression(text)
        if self.existWord(text):
            return False
        response = self.getDictionary(text)
        for tag in response.find_all('a'):
            if tag.find('strong') is \
                    not None:
                compare = tag.text
                if tag.find('sup') is not None and (tag.find('sup').text != ''):
                    compare = tag.find('strong').text
                if compare == text or text == self.getRegularExpression(compare):
                    return True
                else:
                    continue
        return False


class Miner:
    def __init__(self):
        self.LIMIT = 5
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
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
        countCursor = cursor.execute("select count(*) as c from content where contentData like %s and date > %s",
                                     ('%' + content + '%', date))
        if countCursor != 0:
            return cursor.fetchone().get('c')
        else:
            return 0

    def getContent(self, stockName, startPos, endPos):
        cursor = self.connection.cursor()
        contentCursor = cursor.execute(
            "SELECT `c`.`title`,`c`.`contentData`, `a`.`name`, `c`.`date` FROM `content` as `c`, `author` as `a` WHERE `c`.`contentData` like %s limit %s , %s",
            ('%' + stockName + '%', startPos, endPos))
        if contentCursor != 0:
            return cursor.fetchall()
        else:
            raise AnalyzerError('content is not valid.')

    def getWordChangePriceList(self, contentDataList, stockName, period):
        dic = Dictionary()
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
            contents = self.getContent(stockName, (i * 10) + 1, (i + 1) * self.LIMIT)  # paging.
            contentsList = contentsList + contents
        return contentsList

    def work(self, stockName, period):
        contents = self.getStockNameContent(stockName, self.LIMIT_PAST_DATE)
        wordPrices = self.getWordChangePriceList(contents, stockName, period)
        return wordPrices

    def getTargetContentWords(self, stockName, date):
        contents = self.getStockNameContent(stockName, date)
        words = []
        dic = Dictionary()
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
                wordPriceDict[word] = wordPriceDict[word] + totalWordPrices[word]
            except KeyError:
                wordPriceDict[word] = totalWordPrices[word]
        return wordPriceDict

    def extractTargetWord(self, stockName, limitDate):
        targetWords = self.getTargetContentWords(stockName, limitDate)  # 개선할수있지 않을까?
        resultMap = {}
        if len(targetWords) > 0:
            totalWordPrices = self.work(stockName, period)
            resultMap = self.getWordPriceMap(targetWords, totalWordPrices)
        else:
            print('none')
        return resultMap

    def printAnalyze(self, wordMap):
        chartList = []
        plusTotalCnt = 0
        minusTotalCnt = 0
        for word in wordMap.keys():
            plusList = []
            minusList = []
            for price in wordMap[word]:
                price = numpy.nan_to_num(price)
                if price > 0:
                    plusList.append(price)
                if price < 0:
                    minusList.append(price)
            plusTotalCnt += len(plusList)
            minusTotalCnt += len(minusList)

            chart = {self.WORD_NAME: word, self.PLUS_NAME: plusList, self.MINUS_NAME: minusList}
            chartList.append(chart)
        plusAvgList = []
        minusAvgList = []
        for chart in chartList:
            plusAvg = numpy.nan_to_num(numpy.mean(chart[self.PLUS_NAME]))
            plusAvgList.append(plusAvg)
            minusAvg = numpy.nan_to_num(numpy.mean(chart[self.MINUS_NAME]))
            print(chart[self.WORD_NAME]
                  + ', PLUS, ' + str(len(chart[self.PLUS_NAME])) + ' , PLUS_AVG, ' + str(plusAvg)
                  + ' , MINUS , ' + str(len(chart[self.MINUS_NAME])) + ' , MINUS_AVG , ' + str(minusAvg))
            minusAvgList.append(minusAvg)

        plusAvgList = list(set(plusAvgList))
        minusAvgList = list(set(minusAvgList))
        plusAvgList.sort(reverse=True)
        minusAvgList.sort()
        print(plusTotalCnt)
        print(plusAvgList)
        print(minusTotalCnt)

        print(minusAvgList)


import numpy

anal = Analyzer()
anal.analyze()

miner = Miner()
period = 4
stockName = ""
wordMap = miner.extractTargetWord(stockName, date.today() - timedelta(days=period))
miner.printAnalyze(wordMap)
