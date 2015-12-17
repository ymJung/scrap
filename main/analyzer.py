DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

import pymysql.cursors


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


import re
from bs4 import BeautifulSoup
import urllib
from urllib.request import Request, urlopen
import time
import random


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
        return re.sub('[^가-힝0-9a-zA-Z]', '', text).replace(' ', '')

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


from datetime import date, timedelta


class Miner:
    def __init__(self):
        self.count = 0
        self.url = 'http://krdic.naver.com/small_search.nhn?kind=keyword&query='
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.CHANGE_PRICE_LIST_NAME = 'changePriceList'
        self.WORD_NAME = 'word'
        self.CONTENT_DATA_NAME = 'contentData'
        self.DATE_NAME = 'date'
        self.START_NAME = 'start'
        self.FINAL_NAME = 'final'

    def finalize(self):
        self.connection.commit()
        self.connection.close()

    def getContent(self, content, startPos, endPos):
        cursor = self.connection.cursor()
        contentCursor = cursor.execute(
            "SELECT `c`.`title`,`c`.`contentData`, `a`.`name`, `c`.`date` FROM `content` as `c`, `author` as `a` WHERE `c`.`contentData` like %s limit %s , %s",
            ('%' + content + '%', startPos, endPos))
        if contentCursor != 0:
            return cursor.fetchall()
        else:
            raise AnalyzerError('content is not valid.')

    def getWordChangePriceList(self, contentDataList, stockName, period):
        dic = Dictionary()
        wordDatas = []
        for result in contentDataList:
            contentData = result.get(self.CONTENT_DATA_NAME)
            date = result.get(self.DATE_NAME)
            sliceDate = self.getTargetFinanceData(date, period)
            change = self.getFinanceChangePrice(sliceDate, stockName)

            for target in dic.splitStr(contentData):
                if dic.existSplitWord(target):
                    word = dic.getWordByStr(target)
                    self.putWordDatas(word, change, wordDatas)
        return wordDatas

    def getFinanceChangePrice(self, sliceDate, stockName):
        cursor = self.connection.cursor()
        financeCursor = cursor.execute(
            "select s.name, f.start, f.final, f.date from finance f, stock s where f.stockId = s.id and s.name = %s and f.date like %s",
            (stockName, sliceDate + "%"))
        if financeCursor != 0:
            finance = cursor.fetchone()  # one ? many?
            return int(finance.get(self.START_NAME)) - int(finance.get(self.FINAL_NAME))
        else:
            raise AnalyzerError('finance data not found.')

    def getTargetFinanceData(self, date, period):
        # yyyy-MM-dd and plus period
        plusPeridDate = date + timedelta(days=period)
        sliceDate = str(plusPeridDate)[0:9]
        return sliceDate

    def putWordDatas(self, word, changePrice, wordDatas):
        exist = False
        for wordMap in wordDatas:
            if wordMap.get(word) is not None:
                exist = True
                wordMap.get(self.CHANGE_PRICE_LIST_NAME).append(changePrice)
                break
        if not exist:
            changeList = []
            changeList.append(changePrice)
            newWordMap = {
                self.WORD_NAME: word,
                self.CHANGE_PRICE_LIST_NAME: changeList
            }
            wordDatas.append(newWordMap)
