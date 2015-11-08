__author__ = 'YoungMin'

login_url = "http://www.ppomppu.co.kr/index.php"
stock_url = "http://www.ppomppu.co.kr/zboard/zboard.php?id=stock"
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
search = driver.find_element_by_xpath("//*[@id='bbs_wrap']/div/table[6]/tbody/tr[2]/td/table/tbody/tr/td[2]/input")
search.send_keys(value)
search.send_keys(Keys.ENTER)

links = driver.find_elements_by_css_selector('#revolution_main_table > tbody > tr > .list_vspace >a')
dataId = 0
for link in links:
    linkUrl = link.get_attribute('href')
    driver.get(linkUrl)
    title = driver.find_element_by_class_name(".view_title").text
    content = driver.find_element_by_css_selector(
        "#bbs_wrap > div:nth-child(1) > table:nth-child(13) > tbody > tr:nth-child(1) > td > table > tbody > tr > td > table > tbody > tr > td").text
    writer = driver.find_element_by_xpath("//*[@id='wrap']/div[4]/div/h4/div/span[1]/a").text
    date = driver.find_element_by_css_selector("#wrap > div.ct > div > h4 > div > span.hi").text

    dataId += 1
    data = {"dataId": dataId, "title": title, "content": content, "writer": writer, "date": date}
    print(data)
    comments = driver.find_elements_by_id('*commentContent')
    for comment in comments:
        commentData = comment.text()
        commentWriter = ''
    comment = {"writer": commentWriter, "date": date, "content": commentData}
    commentDatas = {"dataId": dataId, "comment": comment}
driver.close()
