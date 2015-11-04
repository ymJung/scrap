__author__ = 'YoungMin'

# url +
login_url = "http://m.ppomppu.co.kr/new/login.php?s_url=/new/"
stock_url = "http://m.ppomppu.co.kr/new/bbs_list.php?id=stock"

import urllib
from bs4 import BeautifulSoup as bs
import requests as req
import urllib.request as ur

import http.cookiejar as jar

## login session, Html file reading ##

url = "http://www.ppomppu.co.kr/zboard/login_check.php"
login_form = {"user_id": "", "password": ""}
session = req.session()
session.post(url, data=login_form)
cookie = session.cookies

cj = jar.LWPCookieJar()
opener = ur.request.build_opener(ur.HTTPCookieProcessor(cj))
urllib.request.install_opener(opener)
params = urllib.parse.urlencode(login_form)
params = params.encode('utf-8')
req = ur.Request(login_url, params)
res = opener.open(req)
result = res.read()

