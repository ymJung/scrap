DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

import pymysql.cursors


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
            splitStrings = data.split(' ')
            for i in range(len(splitStrings)):
                if idx > i:
                    print('start in middle ' + str(idx))
                    i = idx
                splitString = splitStrings[i]
                findWord = False
                for j in range(len(splitString)):
                    subStr = splitString[0:len(splitString) - j]
                    subStr = dic.getRegularExpression(subStr)
                    if dic.isTargetWord(subStr) and dic.isInsertTarget(subStr) is True:
                        dic.insertWord(subStr)
                        findWord = True

                if dic.isTargetWord(splitString) and findWord is False and dic.existWord(splitString) is False:
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

    def finalize(self):
        self.connection.commit()
        self.connection.close()

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
        if self.isTargetWord(data) and self.existGarbageWord(data) is False and self.existWord(data) is True:
            cursor = self.connection.cursor()
            insertGarbageSql = "INSERT INTO `garbage` (`word`,`contentId`) VALUES (%s, %s)"
            cursor.execute(insertGarbageSql, (data, contentId))
            print('insert garbage ' + data)

    def isTargetWord(self, text):
        text = self.getRegularExpression(text)
        if len(text) >= 2 and len(text) < 50:
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


anal = Analyzer()
anal.analyze()
# anal.analyzeDictionary("상회", 1)
