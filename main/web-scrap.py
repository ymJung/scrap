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
linkUrlList = []
for link in links:
    linkUrlList.append(link.get_attribute('href'))


seq = 0
sep = "|"
data = []
commentData = []
for linkUrl in linkUrlList:
    driver.get(linkUrl)
    dataHtml = driver.find_element_by_css_selector('#wrap > div.ct > div > h4').text

    title = dataHtml.split('\n')[0]
    writer = dataHtml.split('\n')[1]
    date = dataHtml.split('\n')[2].split('|')[2]
    content = driver.find_element_by_id('KH_Content').text

    seq += 1
    dataMap = {"dataId": seq, "title": title, "content": content, "writer": writer, "date": date}
    data.append(dataMap)

    comments = driver.find_element_by_css_selector('#wrap > div.ct > div > div.cmAr').text
    comments = comments.split("덧글")
    for comment in comments:
        comment = comment.strip()
        if comment.find('추천') > 0:
            commentWriter = comment.split('추천')[0].strip()
            commentContent = ''.join(comment.split('추천')[1].strip().split('\n')[0:-1])
            date = comment.split('추천')[1].split('\n')[-1].replace('|', '').strip()
            commentMap = {"dataId": seq, "writer": commentWriter, "date": date, "content": commentContent}
            commentData.append(commentMap)

print(data)
print(commentData)
driver.close()
