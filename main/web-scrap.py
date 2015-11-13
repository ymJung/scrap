from selenium.common.exceptions import NoSuchElementException

__author__ = 'YoungMin'

from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class Ppomppu:
    def __init__(self):
        print(self)

    def GetTrend(self, id, password, value):
        global data, commentData
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
        commentData = []


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
                    date = dataHtml.split('\n')[2].split('|')[2]
                    content = driver.find_element_by_id('KH_Content').text

                    seq += 1
                    dataMap = {"dataId": seq, "title": title, "content": content, "writer": writer, "date": date}
                    data.append(dataMap)

                    comments = driver.find_element_by_css_selector('#wrap > div.ct > div > div.cmAr')
                    comments = comments.text
                    comments = comments.split("덧글")
                except NoSuchElementException as e:
                    print(e)
                    continue

                for comment in comments:
                    try:
                        comment = comment.strip()
                        if comment.find('추천') > 0:
                            commentWriter = comment.split('추천')[0].strip()
                            commentContent = ''.join(comment.split('추천')[1].strip().split('\n')[0:-1])
                            date = comment.split('추천')[1].split('\n')[-1].replace('|', '').strip()
                            commentMap = {"dataId": seq, "writer": commentWriter, "date": date,
                                          "content": commentContent}
                            commentData.append(commentMap)
                    except NoSuchElementException as e:
                        print(e)
                        continue


            driver.get(listUrl)
            try :
                driver.find_element_by_css_selector('#paging_menu > a.next > img').click()
            except NoSuchElementException as e:
                print(e)
                break

        self.returnData = {"data": data, "comments": commentData}
        driver.close()
        return self.returnData


result = Ppomppu().GetTrend("", "", "")
print(result)
