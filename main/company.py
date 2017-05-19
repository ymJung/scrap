import re
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup


HEADERS = ['매출액', '영업이익', '세전계속사업이익', '당기순이익', '당기순이익(지배)', '당기순이익(비지배)', '자산총계',
  '부채총계', '자본총계', '자본총계(지배)', '자본총계(비지배)', '자본금', '영업활동현금흐름', '투자활동현금흐름', '재무활동현금흐름',
  'CAPEX', 'FCF', '이자발생부채', '영업이익률', '순이익률', 'ROE(%)', 'ROA(%)', '부채비율', '자본유보율', 'EPS(원)', 'PER(배)',
  'BPS(원)', 'PBR(배)', '현금DPS(원)', '현금배당수익률', '현금배당성향(%)', '발행주식수(보통주)']
DB_COLUMNS = ['retailPrice', 'businessProfit', 'beforeTaxBuisinessProfit', 'netProfitDuringTheTerm', 'controlNetProfitDuringTheTerm', 'uncontrolNetProfitDuringTheTerm', 'totalAsset', 'totalDebt', 'totalCapital', 'totalControlCapital', 'totalUncontrolCapital', 'capital', 'businessCashFlow', 'investmentCashFlow', 'financeCashFlow', 'capitalExpenditures', 'freeCashFlow', 'interestBearingDebt', 'buisinessProfitRate', 'clearProfitRate', 'returnOnEquityRate', 'returnOnAssetsRate', 'debtRate', 'deferCapital', 'earningsPerShareWon', 'priceEarningsRatio', 'bookvaluePerShareWon', 'priceBookvalueRatio', 'cashDividendPerShare', 'cashDividendYield', 'cashPropensityDividendRate', 'shareOutstandingQty']
'''
get_date_str(s) - 문자열 s 에서 "YYYY/MM" 문자열 추출
'''
def get_date_str(s):
    date_str = ''
    r = re.search("\d{4}/\d{2}", s)
    if r:
        date_str = r.group()
        date_str = date_str.replace('/', '-')

    return date_str

'''
* code: 종목코드
* fin_type = '0': 재무제표 종류 (0: 주재무제표, 1: GAAP개별, 2: GAAP연결, 3: IFRS별도, 4:IFRS연결)
* freq_type = 'Y': 기간 (Y:년, Q:분기)
headers --
[['매출액', '영업이익', '세전계속사업이익', '당기순이익', '당기순이익(지배)', '당기순이익(비지배)', '자산총계',
  '부채총계', '자본총계', '자본총계(지배)', '자본총계(비지배)', '자본금', '영업활동현금흐름', '투자활동현금흐름', '재무활동현금흐름',
  'CAPEX', 'FCF', '이자발생부채', '영업이익률', '순이익률', 'ROE(%)', 'ROA(%)', '부채비율', '자본유보율', 'EPS(원)', 'PER(배)',
  'BPS(원)', 'PBR(배)', '현금DPS(원)', '현금배당수익률', '현금배당성향(%)', '발행주식수(보통주)']]

'''
def get_finstate_naver(code, fin_type='0', freq_type='Y'):
    url_tmpl = 'http://companyinfo.stock.naver.com/v1/company/ajax/cF1001.aspx?' \
                   'cmp_cd=%s&fin_typ=%s&freq_typ=%s'

    url = url_tmpl % (code, fin_type, freq_type)
    #print(url)

    dfs = pd.read_html(url, encoding="utf-8", flavor='html5lib')
    df = dfs[0]
    if df.ix[0,0].find('해당 데이터가 존재하지 않습니다') >= 0:
        return None

    df.rename(columns={'주요재무정보':'date'}, inplace=True)
    df.set_index('date', inplace=True)

    cols = list(df.columns)
    if '연간' in cols: cols.remove('연간')
    if '분기' in cols: cols.remove('분기')
    cols = [get_date_str(x) for x in cols]
    df = df.ix[:, :-1]
    df.columns = cols
    dft = df.T
    dft.index = pd.to_datetime(dft.index)

    # remove if index is NaT
    dft = dft[pd.notnull(dft.index)]
    return dft


import pymysql
import configparser

cf = configparser.ConfigParser()
cf.read('config.cfg')

DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')


class DBManager:
    def __init__(self):
        self.conn = conn = pymysql.connect(host=DB_IP, user=DB_USER, password=DB_PWD, db=DB_SCH, charset='utf8mb4')

    def __del__(self):
        self.conn.close()

    def get_codes(self):
        query = "SELECT DISTINCT code FROM data.daily_stock"
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def insert_code_date(self, code, date):
        cursor = self.conn.cursor()
        cursor.execute("select count(id) as cnt from data.company_finance where code = %s and date = %s", (code, date))
        cnt = cursor.fetchone()[0]
        if cnt == 0:
            print('insert code date', code, date)
            cursor.execute("INSERT INTO `data`.`company_finance` (`code`, `date`) VALUES (%s, %s)", (code, date))
            self.conn.commit()

    def update_company(self, code, date, col_name, value):
        self.insert_code_date(code, date)
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, " + col_name + " FROM data.company_finance WHERE code = %s and date = %s",
                       (code, date))
        result = cursor.fetchone()
        cf_id = result[0]
        origin = result[1]
        if str(origin) != str(value):
            print('update', code, date, col_name, value)
            cursor.execute("UPDATE data.company_finance SET " + col_name + " = %s WHERE id = %s", (value, cf_id))
            self.conn.commit()

import math
import re
def insert_code_finance_data(db, code):
    df = get_finstate_naver(code)
    for header in HEADERS:
        data = df[header]
        for idx in range(len(data)):
            keys = data.keys()
            date = pd.to_datetime(keys[idx], format='%Y%m%d', errors='ignore').date()
            column = DB_COLUMNS[HEADERS.index(header)]
            value = data[idx].item()
            if math.isnan(value):
                print('nan')
                continue
            db.update_company(code, date, column, value)


import time
import random

db = DBManager()
for code in db.get_codes():
    time.sleep(random.randrange(300, 1500) / 1000)
    num_code = re.findall('\d+', code[0])[0]
    insert_code_finance_data(db, num_code)