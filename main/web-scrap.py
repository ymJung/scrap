__author__ = 'YoungMin'

login_url = "http://m.ppomppu.co.kr/new/login.php?s_url=/new/"
stock_url = "http://m.ppomppu.co.kr/new/bbs_list.php?id=stock"
userId = ""
password = ""
value = "삼익악기"

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Firefox()
# login
driver.get(login_url)
driver.find_element_by_id("user_id").send_keys(userId)
driver.find_element_by_name("password").send_keys(password)
form = driver.find_element_by_name("zb_login")
form.submit()

driver.get(stock_url)
search = driver.find_element_by_xpath('//*[@id="wrap"]/div[4]/form/div/p/input')
search.send_keys(value)
search.send_keys(Keys.ENTER)

pcv_url = driver.find_element_by_link_text('PC버전').get_attribute('href')

links = driver.find_elements_by_class_name('noeffect')
seq = 0
for link in links:
    linkUrl = link.get_attribute('href')
    driver.get(linkUrl)
    date = driver.find_element_by_css_selector('#wrap > div.ct > div > h4 > div > span.hi').text.rsplit('|')[-1]
    writer = driver.find_elements_by_css_selector('#wrap > div.ct > div > h4 > div > span.ct > a').text

    driver.get(pcv_url)
    title = driver.find_element_by_class_name('view_title2').text
    content = driver.find_element_by_css_selector("#bbs_wrap > div:nth-child(1) > table:nth-child(13) > tbody > tr:nth-child(1) > td > table > tbody > tr > td > table > tbody > tr > td").text

    seq += 1
    data = {"dataId": seq, "title": title, "content": content, "writer": writer, "date": date}
    print(data)
    comments = driver.find_elements_by_id('commentContent')
    for comment in comments:
        commentData = comment.text()
        commentWriter = ''
    comment = {"writer": commentWriter, "date": date, "content": commentData}
commentDatas = {"dataId": seq, "comment": comment}
driver.close()
