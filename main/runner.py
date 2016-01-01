DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

STOCK_NAME = ""
PPOMPPU_ID = ""
PPOMPPU_PWD = ""
CONTENT = ""
stockName = ""

import analyzer

anal = analyzer.Analyzer(DB_IP, DB_USER, DB_PWD, DB_SCH)
anal.analyze()
miner = analyzer.Miner(DB_IP, DB_USER, DB_PWD, DB_SCH)
period = 2
miner.printAnalyzed(period, stockName)



import stockscrap

ds = stockscrap.DSStock(DB_IP, DB_USER, DB_PWD, DB_SCH)
stock = ds.getStock(STOCK_NAME)
datas = ds.getChartDataList(stock.get('code'), 365 * 2)
ds.insertFinanceData(datas, str(stock.get('id')))
ds.finalize()

import webscrap
result = webscrap.Ppomppu().GetTrend(PPOMPPU_ID, PPOMPPU_PWD, CONTENT)  # id , password , search
webscrap.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH).saveData(result)
#


