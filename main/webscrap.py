__author__ = 'YoungMin'

from selenium.common.exceptions import NoSuchElementException, UnexpectedAlertPresentException, \
    StaleElementReferenceException
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import datetime
from dateutil.relativedelta import relativedelta

SEQ = "seq"
TITLE = "title"
CONTENT_DATA = "contentData"
DATE = "date"
WRITER = "writer"
COMMENT_LIST = "commentList"

class Ppomppu:
    def __init__(self):
        self.SEQ = SEQ
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y-%m-%d %H:%M'
        self.LIMIT = datetime.datetime.now() - relativedelta(years=3)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.SITE = "PPOMPPU"

    def getTrend(self, id, password, value, lastUseDateAt):
        if lastUseDateAt is not None:
            self.LIMIT = datetime.datetime(lastUseDateAt.year, lastUseDateAt.month, lastUseDateAt.day)
        login_url = "http://m.ppomppu.co.kr/new/login.php?s_url=/new/"
        stock_url = "http://m.ppomppu.co.kr/new/bbs_list.php?id=stock"
        driver = None
        try :
            if driver is None :
                driver = webdriver.Firefox()
        except OSError as  e:
            print('driver get error.' + str(e))
            return []

        driver.get(login_url)
        driver.find_element_by_id("user_id").send_keys(id)
        driver.find_element_by_id("password").send_keys(password)
        form = driver.find_element_by_name("zb_login")
        form.submit()

        driver.get(stock_url)
        search = driver.find_element_by_xpath('//*[@id="wrap"]/div[4]/form/div/p/input')
        search.send_keys(value)
        search.send_keys(Keys.ENTER)

        data = []
        stop = False

        while True:
            listUrl = driver.current_url

            links = driver.find_elements_by_class_name('noeffect')
            linkUrlList = []
            for link in links:
                linkUrlList.append(link.get_attribute('href'))

            seq = 0
            for linkUrl in linkUrlList:
                try:
                    driver.get(linkUrl)
                    dataHtml = driver.find_element_by_css_selector('#wrap > div.ct > div > h4')
                    dataHtml = dataHtml.text

                    title = dataHtml.split('\n')[0]
                    writer = dataHtml.split('\n')[1]
                    date = self.convertDate(dataHtml.split('\n')[2].split('|')[2].strip())
                    if date < self.LIMIT:
                        stop = True
                        break

                    content = driver.find_element_by_id('KH_Content').text

                    seq += 1
                    comments = driver.find_element_by_css_selector('#wrap > div.ct > div > div.cmAr')  # 개선 필요.
                    comments = comments.text
                    comments = comments.split("덧글")
                except NoSuchElementException as e:
                    print(e)
                    continue
                except UnexpectedAlertPresentException as e:
                    print(e)
                    continue
                except StaleElementReferenceException as e:
                    print(e)
                    continue
                commentData = []
                for comment in comments:
                    try:
                        comment = comment.strip()
                        if comment.find('추천') > 0:
                            commentWriter = comment.split('\n')[-4]
                            commentContent = comment.split('\n')[-2].replace('\n', '')
                            commentDate = comment.split('\n')[-1].replace('|', '').strip()
                            commentDate = self.convertDate(commentDate)
                            commentMap = {self.SEQ: seq,
                                          self.WRITER: commentWriter,
                                          self.DATE: commentDate,
                                          self.CONTENT_DATA: commentContent}
                            commentData.append(commentMap)
                    except NoSuchElementException as e:
                        print(e)
                        continue
                    except IndexError as e:
                        print(e)
                        continue

                dataMap = {self.SEQ: seq,
                           self.TITLE: title,
                           self.CONTENT_DATA: content,
                           self.WRITER: writer,
                           self.DATE: date,
                           self.COMMENT_LIST: commentData}
                data.append(dataMap)
            if stop :
                print('end')
                break
            driver.get(listUrl)
            try:
                driver.find_element_by_css_selector('#paging_menu > a.next > img').click()
            except NoSuchElementException as e:
                print(e)
                break

        driver.close()
        return data

    def convertDate(self, target):
        try:
            return datetime.datetime.strptime(target, self.DATE_FORMAT)
        except:
            return self.DEFAULT_DATE


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
                url = 'http://board.moneta.co.kr/cgi-bin/mpax/bulList.cgi?boardid=' + code + '&page=' + str(page)
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                clds = soup.find(id='communityListData').findAll('dl')
                links = []
                breakFlag = False
                for cld in clds:
                    links.append('http://board.moneta.co.kr/cgi-bin/mpax/' + cld.find('a').get('href'))
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
        self.LIMIT = datetime.datetime.now() - relativedelta(month=2)
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
                url = 'http://finance.naver.com/item/board.nhn?code=' + code + '&page=' + str(page)
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                listTD = soup.findAll('td', class_='title')
                links = []
                breakFlag = False
                for td in listTD:
                    links.append('http://finance.naver.com' + td.find('a').get('href'))
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
        self.LIMIT = datetime.datetime.now() - relativedelta(month=2)
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
                url = 'http://board2.finance.daum.net/gaia/do/stock/list?bbsId=stock&pageIndex=' + str(page) + '&objCate2=2-' + code
                soup = BeautifulSoup(urllib.request.urlopen(url), 'lxml')
                listTD = soup.findAll('td', class_='subj')
                links = []
                breakFlag = False
                for td in listTD:
                    links.append('http://board2.finance.daum.net/gaia/do/stock/' + td.find('a').get('href'))
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
        return data

    def convertDate(self, param):
        try:
            return datetime.datetime.strptime(param, self.DATE_FORMAT)  # 2016.02.13 20:30
        except:
            return self.DEFAULT_DATE
