from selenium.common.exceptions import NoSuchElementException
import time

__author__ = 'YoungMin'

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
DATE_FORMAT = '%Y-%m-%d %H:%M'


class Ppomppu:
    def __init__(self):
        print(self)

    def GetTrend(self, id, password, value):
        global data
        LIMIT = datetime.datetime.now() - relativedelta(years=3)
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
                driver.get(linkUrl)
                try:
                    dataHtml = driver.find_element_by_css_selector('#wrap > div.ct > div > h4')
                    dataHtml = dataHtml.text

                    title = dataHtml.split('\n')[0]
                    writer = dataHtml.split('\n')[1]
                    date = dataHtml.split('\n')[2].split('|')[2].strip()
                    if datetime.datetime.strptime(date, DATE_FORMAT) < LIMIT:
                        break

                    content = driver.find_element_by_id('KH_Content').text

                    seq += 1
                    comments = driver.find_element_by_css_selector('#wrap > div.ct > div > div.cmAr')
                    comments = comments.text
                    comments = comments.split("덧글")
                except NoSuchElementException as e:
                    print(e)
                    continue
                commentData = []
                for comment in comments:
                    try:
                        comment = comment.strip()
                        if comment.find('추천') > 0:
                            commentWriter = comment.split('추천')[0].strip()
                            commentContent = ''.join(comment.split('추천')[1].strip().split('\n')[0:-1])
                            date = comment.split('추천')[1].split('\n')[-1].replace('|', '').strip()
                            commentMap = {SEQ: seq, WRITER: commentWriter, DATE: date, CONTENT_DATA: commentContent}
                            commentData.append(commentMap)
                    except NoSuchElementException as e:
                        print(e)
                        continue
                dataMap = {SEQ: seq, TITLE: title, CONTENT_DATA: content, WRITER: writer, DATE: date,
                           COMMENT_LIST: commentData}
                data.append(dataMap)
            driver.get(listUrl)
            try:
                driver.find_element_by_css_selector('#paging_menu > a.next > img').click()
            except NoSuchElementException as e:
                print(e)
                break

        driver.close()
        return data


result = Ppomppu().GetTrend("", "", "")  # id , password , search

print(result)
