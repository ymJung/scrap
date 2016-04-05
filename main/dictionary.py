import re
from bs4 import BeautifulSoup
from urllib.request import urlopen
import time
import random
import urllib
import dbmanager


class Dictionary:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.count = 0
        self.url = 'http://krdic.naver.com/small_search.nhn?kind=keyword&query='
        self.MIN_WORD_LEN = 2
        self.MAX_WORD_LEN = 50
        # self.REGULAR_EXP = '[^가-힝0-9a-zA-Z]'
        self.REGULAR_EXP = '[^가-힝]'
        self.CONTENT_DATA_NAME = 'contentData'
        self.dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
    def commit(self):
        self.dbm.commit()

    def __del__(self):
        self.dbm.commit()
        self.dbm.close()

    def splitStr(self, str):
        if str is None:
            return []
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

