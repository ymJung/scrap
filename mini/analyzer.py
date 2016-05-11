import pymysql

class DBManager:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
    def selectAnalyze(self, analyzed):
        cursor = self.connection.cursor()
        contentSelectSql = "SELECT `id`,`title`,`contentData`,`authorId`,`date`,`analyze`,`createdAt` FROM `content` WHERE `analyze`=%s LIMIT 1"
        cursor.execute(contentSelectSql, (analyzed))
        return cursor.fetchone()

    def updateContentAnalyzeFlag(self, analyzeFlag, contentId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `content` SET `analyze`=%s WHERE `id`=%s", (analyzeFlag, contentId))
    def commit(self):
        self.connection.commit()
        print('dbm commit')
    def close(self):
        print('dbm close')
        self.connection.close()
    def selectWord(self, word):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id` FROM `word` WHERE `word`=%s", (word))
        return cursor.fetchone()

    def insertWord(self, word):
        result = self.selectWord(word)
        if result is None :
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO word (word) VALUES (%s)", (word))
            return True
        return False
    def insertGarbage(self, word, contentId) :
        cursor = self.connection.cursor()
        insertGarbageSql = "INSERT INTO `garbage` (`word`,`contentId`) VALUES (%s, %s)"
        cursor.execute(insertGarbageSql, (word, contentId))
    def selectGarbageWord(self, word):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id` FROM `garbage` WHERE `word`=%s", (word))
        return cursor.fetchone()

class Analyzer:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.dbm = DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
        self.dic = Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)


    def commit(self):
        self.dbm.commit()
        self.dic.commit()

    def __del__(self):
        self.dbm.commit()
        self.dbm.close()
        self.dic.dbm.close()

    def analyze(self):
        while True:
            target = self.dbm.selectAnalyze('N')
            if target is None:
                time.sleep(60) # sleep 1min 
                continue
            self.dbm.updateContentAnalyzeFlag('Y', target.get('id'))
            self.commit()
            print('start : ' + str(target.get('id')))
            self.analyzeDictionary(target.get('contentData'), target.get('id'), 0)
            self.commit()
            print('fin : ' + str(target.get('id')))

    def analyzeDictionary(self, data, contentId, idx):
        try:
            splitStrings = self.dic.splitStr(data)
            for i in range(len(splitStrings)):
                if idx > i:
                    print('start in middle ' + str(idx))
                    i = idx
                splitString = self.dic.getRegularExpression(splitStrings[i])
                if self.dic.existSplitWord(splitString) is False:
                    findWord = False
                    for j in range(len(splitString)):
                        subStr = splitString[0:len(splitString) - j]
                        if self.dic.isTargetWord(subStr) and self.dic.isInsertTarget(subStr) is True:
                            self.dic.insertWord(subStr)
                            findWord = True

                    if findWord is False and self.dic.existWord(splitString) is False:
                        self.dic.insertGarbageWord(splitString, contentId)
                    idx = i
            idx = 0
        except urllib.error.URLError as e:
            print(e)
            print('retry analyzeDictionary ' + str(idx))
            self.analyzeDictionary(data, contentId, idx)
        except:
            print('uncaught except')
        finally:
            self.dic.commit()

import re
from bs4 import BeautifulSoup
from urllib.request import urlopen
import time
import random
import urllib


class Dictionary:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.count = 0
        self.url = 'http://krdic.naver.com/small_search.nhn?kind=keyword&query='
        self.MIN_WORD_LEN = 2
        self.MAX_WORD_LEN = 50
        self.REGULAR_EXP = '[^가-힝]'
        self.CONTENT_DATA_NAME = 'contentData'
        self.dbm = DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
    def commit(self):
        self.dbm.commit()


    def splitStr(self, str):
        if str is None:
            return []
        str = str.replace('\n', ' ')
        return str.split()

    def getDictionary(self, text):
        interval = random.randrange(300, 1200) / 1000
        time.sleep(interval)
        self.count += 1
        soup = BeautifulSoup(
            urllib.request.urlopen(self.url + urllib.parse.quote(text)), "lxml")
        return soup

    def getWordId(self, data):
        return self.dbm.selectWord(data).get('id')

    def existWord(self, data):
        result = self.dbm.selectWord(data)
        return result is not None

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
            return self.getWordId(fullWord[0:idx])
        else:
            return ''

    def existGarbageWord(self, data):
        garbage = self.dbm.selectGarbageWord(data)
        return garbage is not None

    def insertWord(self, data):
        print('insert word ' + data)
        self.dbm.insertWord(data)

    def insertGarbageWord(self, data, contentId):
        data = self.getRegularExpression(data)
        if self.isTargetWord(data) and self.existGarbageWord(data) is False and self.existWord(data) is False:
            print('insert garbage ' + data)
            self.dbm.insertGarbage(data, contentId)

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
DB_IP = "192.168.11.6"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

analyzer = Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH)
analyzer.analyze()
