import stockscrap
import dbmanager
import analyzer
import miner
import webscrap
from datetime import date, timedelta

class runner :
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC) :
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH

        self.PPOMPPU_ID = PPOMPPU_ACC.get('id')
        self.PPOMPPU_PWD = PPOMPPU_ACC.get('pwd')

    def insertFinance(self, stock) :
        stockCode = stock.get('code')
        stockId = stock.get('id')
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        datas = ds.getChartDataList(stockCode, 365 * 2)
        ds.insertFinanceData(datas, str(stockId))
        ds.finalize()
    def insertPpomppuResult(self, stock):
        lastUseDateAt = stock.get('lastUseDateAt')
        stockName = stock.get('name')
        ppomppuResult = webscrap.Ppomppu().getTrend(self.PPOMPPU_ID, self.PPOMPPU_PWD, stockName, lastUseDateAt)  # id , password , search
        saveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        saveDbm.saveData(ppomppuResult, stockName)
        saveDbm.finalize()
    def insertPaxnetResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        paxnetResult = webscrap.Paxnet().getTrendByCode(stockCode, lastUseDateAt)
        paxnetSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        paxnetSaveDbm.saveData(paxnetResult, stockName)
        paxnetSaveDbm.finalize()
    def insertNaverResult(self, stock) :
        lastUseDateAt = stock.get('lastUseDateAt')
        stockCode = stock.get('code')
        stockName = stock.get('name')
        ns = webscrap.NaverStock()
        naverResult = ns.getTrendByCode(stockCode, lastUseDateAt)
        naverSaveDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        naverSaveDbm.saveData(naverResult, stockName)
        naverSaveDbm.finalize()
    def insertAnalyzedResult(self, stock) :
        stockDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        stockName = stock.get('name')
        anal = analyzer.Analyzer(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        anal.analyze()
        mine = miner.Miner(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)

        period = 2
        plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetPlusAvg, targetMinusAvg = mine.getAnalyzedCnt(period, stockName)
        result = {'name' : stockName, 'pluscnt': plusCnt, 'minuscnt':minusCnt}
        stockDbm.saveAnalyzedData(stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, date.today() + timedelta(days=period), targetPlusAvg, targetMinusAvg)
        print(str(result))
        stockDbm.finalize()

    def run(self, stock):
        runDbm = dbmanager.DBManager(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        self.insertFinance(stock)
        self.insertPpomppuResult(stock)
        self.insertPaxnetResult(stock)
        self.insertNaverResult(stock)

        self.insertAnalyzedResult(stock)
        runDbm.updateLastUseDate(stock)
        runDbm.updateAnalyzedResultItem(stock)
        runDbm.finalize()

    def getNewItem(self, stockName):
        ds = stockscrap.DSStock(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        insert = ds.getStock(stockName)
        datas = ds.getChartDataList(insert.get('code'), 365 * 2)
        ds.insertFinanceData(datas, str(insert.get('id')))
        ds.finalize()


DB_IP = "localhost" #해당날짜에 값이 없으면 어디가 기준인가?
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"
PPOMPPU_ACC = { 'id': "", 'pwd' : ""}
dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)
stocks = dbm.getUsefulStockList()
for stock in stocks :
     work = runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC)
     work.run(stock)

#stockName = '잇츠스킨'
#runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC).getNewItem(stockName)
#runner(DB_IP, DB_USER, DB_PWD, DB_SCH, PPOMPPU_ACC).printAnalyzed(stockName)
#select i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.final from item i, stock s, finance f where i.stockId = s.id and i.financeId = f.id order by i.id desc;
#TODO targetat 에 해당하는 finance final 을 가져오자.
#select i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId from item i, stock s where i.stockId = s.id order by i.id desc;



