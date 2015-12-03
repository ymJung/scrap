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

    def analyze(self, data):
        print(data)

    def insertDelimiter(self, data):
        cursor = self.connection.cursor()
        delimiterIdSelectSql = "SELECT `id` FROM `delimiter` WHERE `word`=%s"
        delimiterIdCursor = cursor.execute(delimiterIdSelectSql, (data))
        if delimiterIdCursor == 0:
            delimiterInsertSql = "INSERT INTO `delimiter` (`word`) VALUES (%s)"
            cursor.execute(delimiterInsertSql, (data))

        return delimiterIdCursor.fetchone().get('id')


import re
from bs4 import BeautifulSoup
import urllib


class Dictionary:
    def __init__(self):
        self.url = 'http://krdic.naver.com/small_search.nhn?kind=keyword&query='
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def getDictionary(self, text):
        soup = BeautifulSoup(
            urllib.request.urlopen(self.url + urllib.parse.quote(text)))
        # response = self.session.get(
        #   'http://openapi.naver.com/search?key=' + self.key + '&query=' + text + '&target=encyc&start=1&display=1')
        return soup

    def existWord(self, data):
        cursor = self.connection.cursor()
        selectWordIdSql = "SELECT `id` FROM `word` WHERE `word`=%s"
        selectWordIdCursor = cursor.execute(selectWordIdSql, (data))
        if selectWordIdCursor != 0 :
            return True
        else :
            return False

    def insertWord(self, data):
        cursor = self.connection.cursor()
        insertWordSql = "INSERT INTO `word` (`word`) VALUES (%s)"
        insertWordSqlCursor = cursor.execute(insertWordSql, (data))
        cursor.execute(insertWordSqlCursor, (data))

    def insertWaitWord(self, data):
        cursor = self.connection.cursor()
        insertWaitSql = "INSERT INTO `wait` (`word`) VALUES (%s)"
        insertWaitSqlCursor = cursor.execute(insertWaitSql, (data))
        cursor.execute(insertWaitSqlCursor, (data))



    def exist(self, text):
        if len(text) < 2:
            return False
        text = re.sub('[^가-힝0-9a-zA-Z\\s]', '', text)
        if self.existWord(text) :
            return True
        response = self.getDictionary(text)
        for tag in response.find_all('a'):
            if tag.find('strong') is not None:
                compare = tag.text
                if tag.find('sup') is not None and (tag.find('sup').text != ''):
                    compare = tag.find('strong').text
                if compare == text:
                    return True
                else:
                    continue
        return False


# http://openapi.naver.com/search?key=c1b406b32dbbbbeee5f2a36ddc14067f&query=독도&target=encyc&start=1&display=10

# an = Analyzer()
# an.insertDelimiter('는')

## 미분류 DB 만들기
# space 단위 분리
# > 한글자씩 줄여가며 사전api 호출
# >> 검색되면 - 단어 추가
# >> 검색안되면 - 패스.
# > 처리된 content는 플래그 처리


data = u'눈여겨 보는 종목입니다 어제 한번 출렁하면서 음봉10% 나왔다가 양봉으로 마무리하는 모습 보였는데 실탄이 있었다면... 아쉽더군요 외인의 매수세도 늘어가는거 같고 한달에 10주에서 20주씩 적금든다 생각하고 한 2년 투자하면 어떨까하는데 고수님들 생각은 어떠신지요'
# data list 를 db에서 N인걸로 가져오자.
# word 를 insert 하자.
# 미분류된 것들을 분석함에 넣자.


dic = Dictionary()
for splitString in data.split(' '):
    #insertWaitWord = True
    for i in range(len(splitString)):
        subStr = splitString[0:len(splitString) - i]
        if dic.exist(subStr):
            dic.insertWord(subStr)
            insertWaitWord = False
            print(subStr)

    #if insertWaitWord is True :
    #    dic.insertWaitWord(splitString)
print('end')
