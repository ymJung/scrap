__author__ = 'YoungMin'

import datetime
from dateutil.relativedelta import relativedelta
import sys
import configparser
import json
from urllib.request import Request, urlopen

SEQ = "seq"
TITLE = "title"
CONTENT_DATA = "contentData"
DATE = "date"
WRITER = "writer"
COMMENT_LIST = "commentList"
cf = configparser.ConfigParser()
cf.read('config/config.cfg')
EVENT_URL = cf.get('url', 'event')
API_HEADER = cf.get('url', 'apiheader')
API_KEY = cf.get('url', 'apikey')

from bs4 import BeautifulSoup
from urllib.request import urlopen
import urllib
import time
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
        self.LIMIT = datetime.datetime.now() - relativedelta(years=1)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = 'PAXNET'

    def getTrendByCode(self, code, lastUseDateAt):
        if lastUseDateAt is not None:
            self.LIMIT = datetime.datetime(lastUseDateAt.year, lastUseDateAt.month, lastUseDateAt.day)
        code = re.sub(self.CODE_EXP, '', code).replace(' ', '')
        data = []
        page = 1
        seq = 0
        while True:
            try :
                print('code(' + code + ') seq : ' + str(seq) + ' page : ' + str(page) + ' data len : ' + str(len(data)))
                url = cf.get(self.SITE, 'list') + code + '&page=' + str(page)
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                clds = soup.find(id='communityListData').findAll('dl')
                links = []
                breakFlag = False
                for cld in clds:
                    links.append(cf.get(self.SITE, 'link') + cld.find('a').get('href'))
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
        self.LIMIT = datetime.datetime.now() - relativedelta(month=3)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = "NAVER_STOCK"
        self.LIMIT_CONTENT_LEN = 10000

    def getTrendByCode(self, code, lastUseDateAt):
        if lastUseDateAt is not None:
            self.LIMIT = datetime.datetime(lastUseDateAt.year, lastUseDateAt.month, lastUseDateAt.day)
        code = re.sub(self.CODE_EXP, '', code).replace(' ', '')
        data = []
        page = 1
        seq = 0
        while True:
            try :
                print('code(' + code + ') seq : ' + str(seq) + ' page : ' + str(page) + ' data len : ' + str(len(data)))
                url = cf.get(self.SITE, 'list') + code + '&page=' + str(page)
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                listTD = soup.findAll('td', class_='title')
                links = []
                breakFlag = False
                for td in listTD:
                    links.append(cf.get(self.SITE, 'link') + td.find('a').get('href'))
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
        self.LIMIT = datetime.datetime.now() - relativedelta(month=3)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.CODE_EXP = '[^0-9]'
        self.SITE = "DAUM_STOCK"
        self.LIMIT_CONTENT_LEN = 10000

    def getTrendByCode(self, code, lastUseDateAt):
        if lastUseDateAt is not None:
            self.LIMIT = datetime.datetime(lastUseDateAt.year, lastUseDateAt.month, lastUseDateAt.day)
        code = re.sub(self.CODE_EXP, '', code).replace(' ', '')
        data = []
        page = 1
        seq = 0
        while True:
            try :

                print('code(' + code + ') seq : ' + str(seq) + ' page : ' + str(page) + ' data len : ' + str(len(data)))
                url = cf.get(self.SITE, 'list') + str(page) + '&objCate2=2-' + code
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                listTD = soup.findAll('td', class_='subj')
                links = []
                breakFlag = False
                for td in listTD:
                    links.append(cf.get(self.SITE, 'link') + td.find('a').get('href'))
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


class KakaoStock:
    def __init__(self):
        self.SEQ = SEQ
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y-%m-%dT%H:%M:%S' #2016-05-06T00:37:53.000+00:00
        self.LIMIT = datetime.datetime.now() - relativedelta(years=1)
        self.LIMIT_COUNT = 500
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
                url = cf.get(self.SITE, 'link1') +code+ cf.get(self.SITE, 'link2') + str(cursor)
                soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')
                jls = json.loads(soup.text)

                cursor = jls['nextCursor']
                posts = jls['posts']
                for jl in posts:
                    writer = jl['writer']['name']
                    content = jl['content']
                    date = self.convertDate(jl['createdAt'])
                    if self.LIMIT > date or len(data) > self.LIMIT_CONTENT_LEN :
                        breakFlag = True
                    dataMap = {
                            self.SEQ: seq,
                            self.TITLE: content[0:20],
                            self.CONTENT_DATA: content,
                            self.WRITER: writer,
                            self.DATE: date,
                            self.COMMENT_LIST: []}
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

    def getPriceInfos(self, code, lastUseDateAt):
        if lastUseDateAt is None :
            lastUseDateAt = datetime.date.today() - datetime.timedelta(days=365 * 3)
        else :
            lastUseDateAt = lastUseDateAt.date()
        todate = (datetime.datetime.today() + datetime.timedelta(days=1)).date()
        limit = (todate - lastUseDateAt).days  #last 로부터 오늘까지 day 수
        breakFlag = False
        data = list()
        dates = set()
        while True :
            if limit > self.LIMIT_COUNT :
                nowLimit = (limit % self.LIMIT_COUNT)
                if nowLimit == 0 :
                    nowLimit = self.LIMIT_COUNT
                limit = limit - nowLimit
            else :
                breakFlag = True
                nowLimit = limit
            todateStr = todate.strftime('%Y-%m-%d')#yyyy-MM-dd 형식 less
            try :
                print('code(' , code ,') todateStr : ' , todateStr ,' nowLimit : ' ,nowLimit)
                url = cf.get(self.SITE, 'link1') +code+ cf.get(self.SITE, 'link3') + str(nowLimit) + cf.get(self.SITE, 'link4') + todateStr
                soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'lxml')
                jls = json.loads(soup.text)

                dayCandles = jls['dayCandles']
                for jl in dayCandles:
                    date = self.convertDate(jl['date'])
                    tradePrice = jl['tradePrice']
                    changePrice = jl['changePrice']
                    dataMap = {
                        'date': date,
                        'start': tradePrice - changePrice,
                        'final': tradePrice
                    }
                    if date in dates:
                        continue
                    else:
                        dates.add(date)
                    data.append(dataMap)
                if breakFlag is True:
                    break
            except urllib.error.URLError as e :
                print('url error')
                time.sleep(0.3)
                print(e)
            except :
                print('some thing are wrong. will return', sys.exc_info())
        return data




class Holyday:
    def filterEventDay(self, limit):
        holydays = []
        for idx in range(limit):
            targetAt = datetime.datetime.today() + datetime.timedelta(days=idx)
            target = False
            reason = ''
            try :
                if targetAt.weekday() in [5, 6] :
                    target = True
                req = Request(EVENT_URL + str(targetAt.year) + '&month=' + str(targetAt.month) + '&day=' + str(targetAt.day))
                req.add_header(API_HEADER, API_KEY)
                res = urlopen(req).read().decode(encoding='utf-8')
                jls = json.loads(res)
                if jls['totalResult'] > 0 :
                    for jl in jls['results'] :
                        if jl['type'] in ['h'] :
                            target = True
                            reason = reason + jl['name']
            except Exception:
                continue
            if target is True:
                holyday = {'targetAt': targetAt, 'reason': reason}
                print(holyday)
                holydays.append(holyday)
        return holydays
