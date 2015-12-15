from selenium.common.exceptions import NoSuchElementException, UnexpectedAlertPresentException

PPOMPPU_ID = ""
PPOMPPU_PWD = ""

__author__ = 'YoungMin'

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import datetime
from dateutil.relativedelta import relativedelta

TITLE = "title"
CONTENT_DATA = "contentData"
DATE = "date"
WRITER = "writer"
COMMENT_LIST = "commentList"


class Ppomppu:
    def __init__(self):
        self.SEQ = "seq"
        self.TITLE = TITLE
        self.CONTENT_DATA = CONTENT_DATA
        self.DATE = DATE
        self.WRITER = WRITER
        self.COMMENT_LIST = COMMENT_LIST
        self.DATE_FORMAT = '%Y-%m-%d %H:%M'
        self.LIMIT = datetime.datetime.now() - relativedelta(years=3)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)

    def GetTrend(self, id, password, value):
        global data
        login_url = "http://m.ppomppu.co.kr/new/login.php?s_url=/new/"
        stock_url = "http://m.ppomppu.co.kr/new/bbs_list.php?id=stock"
        driver = webdriver.Firefox()
        # login
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
                        break

                    content = driver.find_element_by_id('KH_Content').text

                    seq += 1
                    comments = driver.find_element_by_css_selector('#wrap > div.ct > div > div.cmAr') # 개선 필요.
                    comments = comments.text
                    comments = comments.split("덧글")
                except NoSuchElementException as e:
                    print(e)
                    continue
                except UnexpectedAlertPresentException as e:
                     print(e)
                     continue
                commentData = []
                for comment in comments:
                    try:
                        comment = comment.strip()
                        if comment.find('추천') > 0:
                            commentWriter = comment.split('\n')[-4]
                            commentContent = comment.split('\n')[-2].replace('\n','')
                            commentDate = comment.split('\n')[-1].replace('|','').strip()
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
        except :
            return self.DEFAULT_DATE


import pymysql.cursors
import sys

DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

class DBManager:
    def __init__(self):
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db='data',
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)


    def saveData(self, result):
        connection = self.connection
        cursor = connection.cursor()

        for each in result:
            authorName = each.get(WRITER)
            title = each.get(TITLE)
            contentData = each.get(CONTENT_DATA)
            date = each.get(DATE)

            authorId = self.getAuthorId(authorName, cursor)
            contentId = self.getContentId(authorId, contentData, cursor, date, title)
            commentList = each.get(COMMENT_LIST)

            for comment in commentList:
                commentWriter = comment.get(WRITER)
                commentAuthorId = self.getAuthorId(commentWriter, cursor)
                commentDate = comment.get(DATE)
                commentContent = comment.get(CONTENT_DATA)

                self.insertComment(cursor, commentAuthorId, commentContent, contentId, commentDate)

        self.finalize()

    def insertComment(self, cursor, commentAuthorId, commentContent, contentId, commentDate):
        try :

            commentIdSql = "SELECT `id` FROM `comment` WHERE `authorId`=%s AND `commentData`=%s"
            commentId = cursor.execute(commentIdSql, (commentAuthorId, commentContent))

            if commentId == 0:
                commentDataInsertSql = "INSERT INTO `comment` (`authorId`, `commentData`, `contentId`, `date`) VALUES (%s, %s, %s, %s)"
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate))
        except pymysql.err.InternalError as e:
            print(e)
        except :
            print("Unexpected error:", sys.exc_info()[0])
            pass




    def getContentId(self, authorId, contentData, cursor, date, title):
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
        contentId = cursor.execute(contentIdSql, (authorId, title))
        if contentId == 0:
            contentDataInsertSql = "INSERT INTO `content` (`title`, `contentData`, `authorId`, `date`) VALUES (%s, %s, %s, %s)"
            cursor.execute(contentDataInsertSql, (title, contentData, authorId, date))
            cursor.execute(contentIdSql, (authorId, title))
            contentId = cursor.fetchone().get('id')
        else:
            contentId = cursor.fetchone().get('id')
        return contentId

    def getAuthorId(self, authorName, cursor):
        authorIdSql = "SELECT `id` FROM `author` WHERE `name`=%s"
        authorId = cursor.execute(authorIdSql, (authorName))
        if authorId == 0:
            authorDataInsertSql = "INSERT INTO `author` (`name`) VALUES (%s)"
            cursor.execute(authorDataInsertSql, (authorName))
            cursor.execute(authorIdSql, (authorName))
            authorId = cursor.fetchone().get('id')
        else:
            authorId = cursor.fetchone().get('id')
        return authorId

    def finalize(self):
        self.connection.commit()
        self.connection.close()


result = Ppomppu().GetTrend(PPOMPPU_ID, PPOMPPU_PWD, "")  # id , password , search
DBManager().saveData(result)
print('end')
