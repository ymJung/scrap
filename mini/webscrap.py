import datetime
import sys
import configparser
cf = configparser.ConfigParser()
cf.read('config.cfg')
DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')
PAX_URL = cf.get('PAXNET', 'list')
PAX_LINK = cf.get('PAXNET', 'link')
NAVER_URL = cf.get('NAVER_STOCK', 'list')
NAVER_LINK = cf.get('NAVER_STOCK', 'link')
DAUM_URL = cf.get('DAUM_STOCK', 'list')
DAUM_LINK = cf.get('DAUM_STOCK', 'link')
KAKAO_LINK1 = cf.get('KAKAO_STOCK', 'link1')
KAKAO_LINK2 = cf.get('KAKAO_STOCK', 'link2')

SEQ = "seq"
TITLE = "title"
CONTENT_DATA = "contentData"
DATE = "date"
WRITER = "writer"
COMMENT_LIST = "commentList"

from bs4 import BeautifulSoup
from urllib.request import urlopen
import urllib
import random
import re


class Paxnet:
    def __init__(self):
        self.SEQ = SEQ
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y%m%d%H:%M'
        self.LIMIT = datetime.datetime.now() - datetime.timedelta(days=365)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = 'PAXNET'

    def getTrendByCode(self, code, lastScrapAt):
        if lastScrapAt is not None:
            self.LIMIT = datetime.datetime(lastScrapAt.year, lastScrapAt.month, lastScrapAt.day)
        code = re.sub(self.CODE_EXP, '', code).replace(' ', '')
        data = []
        page = 1
        seq = 0
        while True:
            try :
                url = PAX_URL + code + '&page=' + str(page)
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                clds = soup.find(id='communityListData').findAll('dl')
                links = []
                breakFlag = False
                for cld in clds:
                    links.append(PAX_LINK + cld.find('a').get('href'))
                    date = self.convertDate(cld.find('div').text.split(' ')[1])
                    if date < self.LIMIT:
                        breakFlag = True
                        break
                    else :
                        print('paxnet link date : ' + str(date))
                if clds is None or len(clds) == 0:
                    breakFlag = True
                for link in links:
                    try:
                        interval = random.randrange(300, 1500) / 1000
                        time.sleep(interval)
                        seq += 1
                        lsoup = BeautifulSoup(urllib.request.urlopen(link), 'lxml')
                        title = lsoup.find('h3').text
                        content = lsoup.find('div', class_='view_article').text
                        writer = lsoup.find('strong').text
                        date = self.convertDate(
                            lsoup.findAll('div', class_='hd')[1].text.replace(' ', '').replace('.', '').split('\n')[4])

                        commentData = []
                        for commentSoup in lsoup.find('div', class_='comment').findAll('dl'):
                            commentWriter = commentSoup.find('strong').text
                            commentContent = commentSoup.find('dd').find('p').text
                            commentDate = self.convertDate(
                                commentSoup.find('dt').text.replace('\t', '').replace(' ', '').replace('.', '').split('\n')[3][0:13])
                            commentMap = {
                                self.SEQ: seq,
                                self.WRITER: commentWriter,
                                self.DATE: commentDate,
                                self.CONTENT_DATA: commentContent}
                            commentData.append(commentMap)

                        dataMap = {
                            self.SEQ: seq,
                            self.TITLE: title,
                            self.CONTENT_DATA: content,
                            self.WRITER: writer,
                            self.DATE: date,
                            self.COMMENT_LIST: commentData}
                        data.append(dataMap)
                    except:
                        print('something are wrong. but continue.')
                        continue
                if breakFlag:
                    print('end')
                    break
                else:
                    page += 1
            except urllib.error.URLError as e :
                time.sleep(0.3)
                print('url error')
                print(e)
        return data

    def convertDate(self, param):
        if len(param) == 10:
            return datetime.datetime.strptime(param, '%Y.%m.%d')
        if len(param) == 5:
            return datetime.datetime(datetime.datetime.now().year, datetime.datetime.now().month,
                                     datetime.datetime.now().day, int(param[0:1]), int(param[3:4]), 0)
        try:
            return datetime.datetime.strptime(param, self.DATE_FORMAT)
        except:
            return self.DEFAULT_DATE


class NaverStock:
    def __init__(self):
        self.SEQ = SEQ
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y.%m.%d %H:%M'
        self.LIMIT = datetime.datetime.now() - datetime.timedelta(days=30*6)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = "NAVER_STOCK"
        self.LIMIT_CONTENT_LEN = 10000

    def getTrendByCode(self, code, lastScrapAt):
        if lastScrapAt is not None:
            self.LIMIT = datetime.datetime(lastScrapAt.year, lastScrapAt.month, lastScrapAt.day)
        code = re.sub(self.CODE_EXP, '', code).replace(' ', '')
        data = []
        page = 1
        seq = 0
        while True:
            try :
                url = NAVER_URL + code + '&page=' + str(page)
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                listTD = soup.findAll('td', class_='title')
                links = []
                breakFlag = False
                for td in listTD:
                    links.append(NAVER_LINK + td.find('a').get('href'))
                if listTD is None or len(listTD) == 0 or len(data) > self.LIMIT_CONTENT_LEN:
                    breakFlag = True
                for link in links:
                    try:
                        interval = random.randrange(300, 1500) / 1000
                        time.sleep(interval)
                        seq += 1
                        lsoup = BeautifulSoup(urllib.request.urlopen(link), 'lxml')
                        title = lsoup.find('table', class_='view').find('strong').text
                        content = lsoup.find('table', class_='view').find(id='body').text
                        writer = lsoup.find('strong').text
                        date = self.convertDate(lsoup.find('table', class_='view').find('th', class_='gray03 p9 tah').text)
                        if date < self.LIMIT:
                            breakFlag = True
                            break
                        else :
                            print('naver stock link date : ' + str(date))

                        dataMap = {
                            self.SEQ: seq,
                            self.TITLE: title,
                            self.CONTENT_DATA: content,
                            self.WRITER: writer,
                            self.DATE: date,
                            self.COMMENT_LIST: []}
                        data.append(dataMap)
                    except:
                        print('some thing are wrong. but continue.')
                        continue
                if breakFlag:
                    print('end')
                    break
                else:
                    page += 1
            except urllib.error.URLError as e :
                time.sleep(0.3)
                print('url error')
                print(e)
            except :
                print('some thing are wrong. will return.', sys.exc_info())
                return data
        return data

    def convertDate(self, param):
        try:
            return datetime.datetime.strptime(param, self.DATE_FORMAT)  # '2016.01.06 21:23'
        except:
            return self.DEFAULT_DATE


class DaumStock:
    def __init__(self):
        self.SEQ = SEQ
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y.%m.%d %H:%M'
        self.LIMIT = datetime.datetime.now() - datetime.timedelta(days=30*6)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = "DAUM_STOCK"
        self.LIMIT_CONTENT_LEN = 10000

    def getTrendByCode(self, code, lastScrapAt):
        if lastScrapAt is not None:
            self.LIMIT = datetime.datetime(lastScrapAt.year, lastScrapAt.month, lastScrapAt.day)
        code = re.sub(self.CODE_EXP, '', code).replace(' ', '')
        data = []
        page = 1
        seq = 0
        while True:
            try :

                url = DAUM_URL + str(page) + '&objCate2=2-' + code
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                listTD = soup.findAll('td', class_='subj')
                links = []
                breakFlag = False
                for td in listTD:
                    links.append(DAUM_LINK + td.find('a').get('href'))
                if listTD is None or len(listTD) == 0 or len(data) > self.LIMIT_CONTENT_LEN:
                    breakFlag = True
                for link in links:
                    try:
                        interval = random.randrange(300, 1500) / 1000
                        time.sleep(interval)
                        seq += 1
                        lsoup = BeautifulSoup(urllib.request.urlopen(link), 'lxml')
                        title = lsoup.find(id='bbsTitle').text
                        content = lsoup.find(id='bbsContent').text
                        writer = lsoup.find(id='bbsInfo').find('a').text
                        date = self.convertDate(lsoup.find(id='bbsInfo').find(class_='datetime').text)
                        if date < self.LIMIT:
                            breakFlag = True
                            break
                        else :
                            print('daum stock link date : ' + str(date))

                        dataMap = {
                            self.SEQ: seq,
                            self.TITLE: title,
                            self.CONTENT_DATA: content,
                            self.WRITER: writer,
                            self.DATE: date,
                            self.COMMENT_LIST: []}
                        data.append(dataMap)
                    except:
                        print('some thing are wrong. but continue.')
                        continue
                if breakFlag:
                    print('end')
                    break
                else:
                    page += 1
            except urllib.error.URLError as e :
                print('url error')
                time.sleep(0.3)
                print(e)
            except :
                print('some thing are wrong. will return', sys.exc_info())
                return data
        return data

    def convertDate(self, param):
        try:
            return datetime.datetime.strptime(param, self.DATE_FORMAT)  # 2016.02.13 20:30
        except:
            return self.DEFAULT_DATE

import json



class KakaoStock:
    def __init__(self):
        self.SEQ = SEQ
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y-%m-%dT%H:%M:%S' #2016-05-06T00:37:53.000+00:00
        self.LIMIT = datetime.datetime.now() - datetime.timedelta(days=365)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = "KAKAO_STOCK"
        self.LIMIT_CONTENT_LEN = 10000

    def getTrendByCode(self, code, lastUseDateAt):
        if lastUseDateAt is not None:
            self.LIMIT = datetime.datetime(lastUseDateAt.year, lastUseDateAt.month, lastUseDateAt.day)
        data = []
        cursor = 0
        seq = 0
        breakFlag = False
        while True:
            try :
                print('code(' + code + ') seq : ' + str(seq) + ' cursor : ' + str(cursor) + ' data len : ' + str(len(data)))
                url = KAKAO_LINK1 +code+KAKAO_LINK2 + str(cursor)
                soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')
                jls = json.loads(soup.text)
                cursor = jls['nextCursor']
                posts = jls['posts']
                for jl in posts:
                    writer = jl['writer']['name']
                    content = jl['content']
                    date = self.convertDate(jl['createdAt'])
                    if self.LIMIT > date :
                        breakFlag = True
                    dataMap = {
                            self.SEQ: seq,
                            self.TITLE: content[0:20],
                            self.CONTENT_DATA: content,
                            self.WRITER: writer,
                            self.DATE: date,
                            self.COMMENT_LIST: []
                    }
                    data.append(dataMap)
                if breakFlag:
                    print('end')
                    break
            except urllib.error.URLError as e :
                print('url error')
                time.sleep(0.3)
                print(e)
            except :
                print('some thing are wrong. will return', sys.exc_info())
                return data
        return data

    def convertDate(self, param):
        try:
            return datetime.datetime.strptime(param[0:18], self.DATE_FORMAT)  # 2016.02.13 20:30
        except:
            return self.DEFAULT_DATE

import pymysql
import time

class DBManager:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.LIMIT_HOUR = 16
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.SEQ = "seq"
        self.TITLE = "title"
        self.CONTENT_DATA = "contentData"
        self.DATE = "date"
        self.WRITER = "writer"
        self.COMMENT_LIST = "commentList"
        self.REGULAR_EXP = '[^가-힝0-9a-zA-Z]'
    def commit(self):
        self.connection.commit()
        print('dbm commit')
    def close(self):
        print('dbm close')
        self.connection.close()
    def initStock(self):
        self.connection.cursor().execute("UPDATE stock SET `scrap` = 1 WHERE `much` = 0")
        self.commit()
    def getUsefulStock(self):
        cursor = self.connection.cursor()
        selectSql = "SELECT `id`, `code`, `name`, `lastScrapAt` FROM stock WHERE `scrap` = 1 AND `much` = 0 ORDER BY id asc LIMIT 1"
        cursor.execute(selectSql)
        stock = cursor.fetchone()
        if stock is None :
            print('init stock')
            self.initStock()
            cursor.execute(selectSql)
            stock = cursor.fetchone()
            time.sleep(10 * 60) # 1 hour sleep
        cursor.execute(("UPDATE stock SET `scrap` = 0 WHERE `id` = %s"), stock.get('id'))
        self.commit()
        return stock
    def updateLastUseDate(self, stock):
        cursor = self.connection.cursor()
        updateLastUseDateSql = "UPDATE `data`.`stock` SET `lastScrapAt`= now() WHERE `id`= %s"
        result = cursor.execute(updateLastUseDateSql, (stock.get('id')))
        print('update' + str(result))
        self.commit()
    def saveData(self, site, results, stockName, stockId):
        print('save data. ', stockName, len(results))
        for each in results:
            authorName = each.get(self.WRITER)
            title = each.get(self.TITLE)
            contentData = each.get(self.CONTENT_DATA)
            date = each.get(self.DATE)
            if len(authorName) == 0 or len(title) == 0 or len(contentData) == 0:
                print('data is wrong' + str(each))
                continue

            authorId = self.saveAuthorAndGetId(site, authorName)
            contentId = self.saveContentAndGetId(site, authorId, contentData, date, title, stockName, stockId)

            commentList = each.get(self.COMMENT_LIST)
            if len(commentList) > 0:
                for comment in commentList:
                    commentWriter = comment.get(self.WRITER)
                    commentAuthorId = self.saveAuthorAndGetId(site, commentWriter)
                    commentDate = comment.get(self.DATE)
                    commentContent = comment.get(self.CONTENT_DATA)
                    self.insertComment(commentAuthorId, commentContent, contentId, commentDate, stockName, stockId)
        self.commit()

    def insertComment(self, commentAuthorId, commentContent, contentId, commentDate, stockName, stockId):
        try:
            cursor = self.connection.cursor()
            commentIdSql = "SELECT `id` FROM `comment` WHERE `authorId`=%s AND `commentData`=%s"
            commentId = cursor.execute(commentIdSql, (commentAuthorId, commentContent))

            if commentId == 0:
                commentDataInsertSql = "INSERT INTO `comment` (`authorId`, `commentData`, `contentId`, `date`, `query`, `stockId`) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate, stockName, stockId))
        except pymysql.err.InternalError as e:
            print(e)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            pass
    def replaceEscape(self, text):
        return re.sub(self.REGULAR_EXP, ' ', text)

    def saveContentAndGetId(self, site, authorId, contentData, date, title, stockName, stockId):
        cursor = self.connection.cursor()
        title = self.replaceEscape(title)
        contentData = self.replaceEscape(contentData)
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
        contentId = cursor.execute(contentIdSql, (authorId, title))
        if contentId == 0:
            contentDataInsertSql = "INSERT INTO `content` (`title`, `contentData`, `authorId`, `date`, `query`, `site`, `stockId`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(contentDataInsertSql, (title, contentData, authorId, date, stockName, site, stockId))
            cursor.execute(contentIdSql, (authorId, title))
            contentId = cursor.fetchone().get('id')
        else:
            contentId = cursor.fetchone().get('id')
        return contentId

    def saveAuthorAndGetId(self, site, authorName):
        cursor = self.connection.cursor()
        authorIdSql = "SELECT `id` FROM `author` WHERE `name`= %s"
        authorId = cursor.execute(authorIdSql, (authorName))
        if authorId == 0:
            authorDataInsertSql = "INSERT INTO `author` (`name`, `site`) VALUES (%s, %s)"
            cursor.execute(authorDataInsertSql, (authorName, site))
            cursor.execute(authorIdSql, (authorName))
            authorId = cursor.fetchone().get('id')
        else:
            authorId = cursor.fetchone().get('id')
        return authorId


class Runner:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.dbm = DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)

    def insertPaxnetResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnet = Paxnet()
        paxnetResult = paxnet.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(paxnet.SITE, paxnetResult, stockName, stock.get('id'))

    def insertNaverResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ns.SITE, naverResult, stockName, stock.get('id'))

    def insertDaumResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ds = DaumStock()
        daumResult = ds.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ds.SITE, daumResult, stockName, stock.get('id'))
    def insertKakaoResult(self, stock):
        lastScrapAt = stock.get('lastScrapAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ks = KakaoStock()
        kakaoResult = ks.getTrendByCode(stockCode, lastScrapAt)
        self.dbm.saveData(ks.SITE, kakaoResult, stockName, stock.get('id'))

    def run(self, stock):
        if stock is None:
            return
        self.insertPaxnetResult(stock)
        self.insertNaverResult(stock)
        self.insertDaumResult(stock)
        self.insertKakaoResult(stock)
        self.dbm.updateLastUseDate(stock)
        self.dbm.commit()

print('start')
while True :
    try :
        run = Runner(DB_IP, DB_USER, DB_PWD, DB_SCH)
        stock = run.dbm.getUsefulStock()
        print(stock.get('name'), 'is start')
        run.run(stock)
        print(stock.get('name'), 'is done')
        run.dbm.close()
    except :
        print("unexpect error.", sys.exc_info())
        break
