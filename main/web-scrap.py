__author__ = 'YoungMin'

login_url = "http://m.ppomppu.co.kr/new/login.php?s_url=/new/"
stock_url = "http://m.ppomppu.co.kr/new/bbs_list.php?id=stock"
userId = ""
password = ""
value = "삼익악기"
fileName = "text.txt"

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

driver = webdriver.Firefox()
# login
driver.get(login_url)
driver.find_element_by_id("user_id").send_keys(userId)
driver.find_element_by_id("password").send_keys(password)
form = driver.find_element_by_name("zb_login")
form.submit()

driver.get(stock_url)
search = driver.find_element_by_xpath('//*[@id="wrap"]/div[4]/form/div/p/input')
search.send_keys(value)
search.send_keys(Keys.ENTER)

links = driver.find_elements_by_class_name('noeffect')
seq = 0
sep = "|"

for link in links:
    linkUrl = link.get_attribute('href')
    driver.get(linkUrl)
    data = driver.find_element_by_css_selector('#wrap > div.ct > div > h4').text

    title = data.split('\n')[0]
    writer = data.split('\n')[1]
    date = data.split('\n')[2].split('|')[2]
    content = driver.find_element_by_id('KH_Content').text

    seq += 1
    data = {"dataId": seq, "title": title, "content": content, "writer": writer, "date": date}

    comments = driver.find_element_by_css_selector('#wrap > div.ct > div > div.cmAr').text
    comments = comments.split("덧글")
    for commentData in comments:
        commentWriter = ''
        comment = {"writer": commentWriter, "date": date, "content": comment}
        commentData = {"dataId": seq, "comment": comment}

driver.close()

