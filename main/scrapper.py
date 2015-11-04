__author__ = 'YoungMin'

import requests as req

form = {"user_id" : "metal0", "password" : "jym8602"}
login_url = "http://m.ppomppu.co.kr/new/login.php?s_url=/new/"
stock_url = "http://m.ppomppu.co.kr/new/bbs_list.php?id=stock"

with req.session() as session:
    post = session.post(login_url, data=form)
    cookie = post.cookies
    get = session.get(stock_url, cookies=cookie)
    get.encoding = 'utf-8'
    print(get.json())



