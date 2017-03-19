import datetime
import pymysql.cursors
import sys
import re
import configparser
from datetime import timedelta


class DBManagerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class DBManager:
    def __init__(self):
        cf = configparser.ConfigParser()
        cf.read('config/config.cfg')
        self.LIMIT_HOUR = 16
        self.connection = pymysql.connect(host=cf.get('db','DB_IP'),
                                          user=cf.get('db','DB_USER'),
                                          password=cf.get('db','DB_PWD'),
                                          db=cf.get('db','DB_SCH'),
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)
        self.DEFAULT_DATE = datetime.datetime(1970, 12, 31, 23, 59, 59)
        self.SEQ = "seq"
        self.TITLE = "title"
        self.CONTENT_DATA = "contentData"
        self.DATE = "date"
        self.WRITER = "writer"
        self.COMMENT_LIST = "commentList"
        self.LIMIT_COUNT = 5
        self.REGULAR_EXP = '[^가-힝0-9a-zA-Z]'
        self.WORK_DONE = 0
        self.WORK_YET = 1

    def commit(self):
        self.connection.commit()
        print('dbm commit')
    def close(self):
        print('dbm close')
        self.connection.close()
    def saveData(self, site, results, stockName, stockId):
        print('save data. ', stockName, len(results))
        for each in results:
            authorName = each.get(self.WRITER)
            title = each.get(self.TITLE)
            contentData = each.get(self.CONTENT_DATA)
            date = each.get(self.DATE)
            if len(authorName) == 0 or len(title) == 0 or len(contentData) == 0:
                print('data is wrong' + str(each))
                continue

            authorId = self.saveAuthorAndGetId(site, authorName)
            contentId = self.saveContentAndGetId(site, authorId, contentData, date, title, stockName, stockId)

            commentList = each.get(self.COMMENT_LIST)
            if len(commentList) > 0:
                for comment in commentList:
                    commentWriter = comment.get(self.WRITER)
                    commentAuthorId = self.saveAuthorAndGetId(site, commentWriter)
                    commentDate = comment.get(self.DATE)
                    commentContent = comment.get(self.CONTENT_DATA)
                    self.insertComment(commentAuthorId, commentContent, contentId, commentDate, stockName, stockId)
        self.updateLastScrapDate(stockId)

    def insertComment(self, commentAuthorId, commentContent, contentId, commentDate, stockName, stockId):
        try:
            cursor = self.connection.cursor()
            commentIdSql = "SELECT `id` FROM `comment` WHERE `authorId`=%s AND `commentData`=%s"
            commentId = cursor.execute(commentIdSql, (commentAuthorId, commentContent))

            if commentId == 0:
                commentDataInsertSql = "INSERT INTO `comment` (`authorId`, `commentData`, `contentId`, `date`, `query`, `stockId`) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate, stockName, stockId))
        except pymysql.err.InternalError as e:
            print(e)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            pass
    def replaceEscape(self, text):
        return re.sub(self.REGULAR_EXP, ' ', text)



    def saveContentAndGetId(self, site, authorId, contentData, date, title, stockName, stockId):
        cursor = self.connection.cursor()
        title = self.replaceEscape(title)
        contentData = self.replaceEscape(contentData)
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
        print(u'saveContentAndGetId', authorId, title)
        contentId = cursor.execute(contentIdSql, (authorId, title))
        if contentId == 0:
            contentDataInsertSql = "INSERT INTO `content` (`title`, `contentData`, `authorId`, `date`, `query`, `site`, `stockId`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(contentDataInsertSql, (title, contentData, authorId, date, stockName, site, stockId))
            cursor.execute(contentIdSql, (authorId, title))
            contentId = cursor.fetchone().get('id')
        else:
            contentId = cursor.fetchone().get('id')
        return contentId

    def saveAuthorAndGetId(self, site, authorName):
        cursor = self.connection.cursor()
        authorIdSql = "SELECT `id` FROM `author` WHERE `name`= %s"
        authorId = cursor.execute(authorIdSql, (authorName))
        if authorId == 0:
            authorDataInsertSql = "INSERT INTO `author` (`name`, `site`) VALUES (%s, %s)"
            cursor.execute(authorDataInsertSql, (authorName, site))
            cursor.execute(authorIdSql, (authorName))
            authorId = cursor.fetchone().get('id')
        else:
            authorId = cursor.fetchone().get('id')
        return authorId

    def getStockList(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` FROM stock where `much` = 0 ORDER BY id ASC")
        return cursor.fetchall()

    def initStock(self):
        self.connection.cursor().execute("UPDATE stock SET `use` = 1 WHERE `much` = 0")
        self.commit()

    def getUsefulStockList(self, targetAt, period):
        cursor = self.connection.cursor()
        cursor.execute('select `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` from stock where much = 0 and id not in  '
                       '(select s.id from item i, stock s where s.id = i.stockId and i.targetAt = %s and i.period = %s) order by id asc', (targetAt, period))
        return cursor.fetchall()
    def getUsefulStock(self, forecastAt, period):
        cursor = self.connection.cursor()
        selectSql = "select `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt` from stock where much = 0 and id not in " \
                    "(select s.id from item i, stock s where s.id = i.stockId and i.targetAt = %s and i.period = %s) order by id asc limit 1"
        cursor.execute(selectSql, (forecastAt, period))
        stock = cursor.fetchone()
        if stock is None :
            stock = cursor.execute('select s.* from item i, stock s where i.yet = %s and i.period = %s and i.targetAt = %s order by i.createdAt asc limit 1', (self.WORK_YET, period, forecastAt))
        if stock is None or self.checkHolyDay(forecastAt) or self.isNotForecastTarget(stock, datetime.date.today(), period) :
            raise DBManagerError('stock is none')
        # self.insertItemDefault(stock.get('id'), forecastAt, period)
        return stock

    def saveAnalyzedData(self, stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, duration):
        cursor = self.connection.cursor()
        cursor.execute('SELECT id FROM stock WHERE name = %s', stockName)
        stock = cursor.fetchone()
        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s ', (stock.get('id'), targetAt, period))
        items = cursor.fetchall()
        if len(items) == 0:
            cursor.execute("INSERT INTO `data`.`item` (`stockId`, `plus`, `minus`, `totalPlus`, `totalMinus`, `targetAt`, `period`, `duration`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, duration))
        else :
            for item in items :
                cursor.execute("UPDATE item SET `plus`=%s, `minus`=%s, `totalPlus`=%s, `totalMinus`=%s, `yet`=%s, `duration`=%s WHERE `id`= %s",
                               (plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, self.WORK_DONE, duration, item.get('id')))
        self.commit()
        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s', (stock.get('id'), targetAt, period))
        return cursor.fetchone().get('id')

    def updateLastScrapDate(self, stockId):
        cursor = self.connection.cursor()
        updateLastUseDateSql = "UPDATE `data`.`stock` SET `lastScrapAt`= now() WHERE `id`= %s"
        result = cursor.execute(updateLastUseDateSql, (stockId))
        print('update scrap date. ', result)
        self.commit()

    def updateAnalyzedResultItem(self, stock):
        cursor = self.connection.cursor()
        stockName = stock.get('name')
        stockId = stock.get('id')

        selectTargetItemsSql = 'SELECT id, targetAt FROM item WHERE financeId IS NULL AND stockId = %s'
        cursor.execute(selectTargetItemsSql, (stockId))
        itemList = cursor.fetchall()
        for item in itemList:
            itemId = item.get('id')
            itemTargetAt = item.get('targetAt')
            cursor.execute('SELECT f.id, s.name, f.date, f.start, f.final FROM finance f, stock s WHERE f.stockId = s.id AND s.name = %s AND f.date = %s ORDER BY f.createdAt DESC LIMIT 1', (stockName, itemTargetAt))
            targetFinance = cursor.fetchone()
            if targetFinance is not None:
                print('update finance date.', stock.get('name'), itemTargetAt)
                self.updateItemFinanceId(targetFinance.get('id'), itemId)

    def updateItemFinanceId(self, financeId, itemId):
        cursor = self.connection.cursor()
        updateItemPriceSql = "UPDATE item SET financeId = %s WHERE id= %s"
        cursor.execute(updateItemPriceSql, (financeId, itemId))

    def analyzedSqlTargetAt(self, stockName, targetAt, period):
        cursor = self.connection.cursor()
        selectAnalyzedSql = 'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final FROM item i, stock s, finance f ' \
                            'WHERE i.stockId = s.id AND f.id = i.financeId AND s.name = %s AND i.targetAt = %s AND i.period = %s AND f.final IS NOT NULL GROUP BY i.targetAt'
        cursor.execute(selectAnalyzedSql, (stockName, targetAt, period))
        analyzedResult = cursor.fetchone()

        return analyzedResult

    def analyzedSql(self, stockName, period):
        cursor = self.connection.cursor()
        selectAnalyzedSql = 'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final FROM item i, stock s, finance f WHERE i.stockId = s.id AND f.id = i.financeId AND s.name = %s AND i.period = %s AND i.yet = 0 GROUP BY i.targetAt ORDER BY i.targetAt desc'
        cursor.execute(selectAnalyzedSql, (stockName, period))
        analyzedResult = cursor.fetchall()

        return analyzedResult

    def isNotForecastTarget(self, stock, targetAt, period):
        stockId = stock.get('id')
        stockName = stock.get('name')
        lastScrapAt = stock.get('lastScrapAt')
        if self.checkHolyDay(targetAt):
            print('exist holy day', stockName, targetAt)
            return True
        if lastScrapAt is None or lastScrapAt.date() < targetAt - datetime.timedelta(days=period):
            print('not yet to scrap.', stockName, targetAt)
            return True
        cursor = self.connection.cursor()
        result = cursor.execute('SELECT id FROM item WHERE targetAt = %s and stockId = %s and period = %s and %s', (targetAt, stockId, period, self.WORK_DONE))
        if result != 0:
            print('exist item date ', targetAt, stockId)
            return True
        targetStartAt = self.getTargetStartAt(targetAt, period)
        targetEndAt = targetAt - datetime.timedelta(days=period)
        result = cursor.execute('SELECT `id` FROM `content` WHERE  `date` > %s and `date` <= %s AND `stockId` = %s', (targetStartAt, targetEndAt, stockId))
        if result == 0:
            print('empty content data.', targetStartAt, targetEndAt, stockName)
            return True
        return False

    def saveAnalyzedItemFinanceList(self, itemId, financeIdList):
        cursor = self.connection.cursor()
        for financeId in financeIdList :
            cursor.execute("INSERT INTO chart_map (itemId, financeId) VALUES (%s, %s)", (itemId, financeId))

    def getFinanceListFromItemId(self, itemId):
        cursor = self.connection.cursor()
        financeIds = cursor.execute('SELECT distinct(financeId) FROM chart_map WHERE itemId=%s', (itemId))
        results = []
        if financeIds != 0 :
            for each in cursor.fetchall() :
                results.append(each.get('financeId'))
                results = list(set(results))
        return results

    def getFinanceDataIn(self, financeIdList):
        if len(financeIdList) == 0 :
            return []
        cursor = self.connection.cursor()
        selectFinanceQuery = 'SELECT f.id, f.start, f.final, f.date, f.createdAt FROM finance f WHERE f.id IN (%s)'
        inquery = ', '.join(list(map(lambda x: '%s', financeIdList)))
        selectFinanceQuery = selectFinanceQuery % inquery
        cursor.execute(selectFinanceQuery, financeIdList)
        return cursor.fetchall()

    def getForecastResult(self, stockName, startAt, period):
        cursor = self.connection.cursor()
        selectForecastSql =  'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt FROM item i, stock s ' \
                             'WHERE i.stockId = s.id AND s.name = %s AND i.targetAt >= %s AND i.period = %s AND i.financeId IS NULL ORDER BY i.id DESC' # AND i.financeId IS NULL
        cursor.execute(selectForecastSql, (stockName, startAt, period))
        return cursor.fetchall()

    def getUnfilterdGarbageWord(self):
        cursor = self.connection.cursor()
        cursor.execute('SELECT `id`, `word` FROM `garbage` WHERE `useful` = "N" ORDER BY `id` ASC LIMIT 1')
        return cursor.fetchone()
    def updateGarbageStatus(self, id, status):
        self.connection.cursor().execute("UPDATE garbage SET `useful` = %s WHERE `id` = %s", (status, id))

    def getGarbageWord(self, garbageId):
        cursor = self.connection.cursor()
        cursor.execute('SELECT `id`, `word`, `useful` FROM `garbage` WHERE `id` = %s', (garbageId))
        return cursor.fetchone()

    def selectWord(self, word):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id` FROM `word` WHERE `word`=%s", (word))
        return cursor.fetchone()
    def selectGarbageWord(self, word):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id` FROM `garbage` WHERE `word`=%s", (word))
        return cursor.fetchone()

    def insertWord(self, word):
        result = self.selectWord(word)
        if result is None :
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO word (word) VALUES (%s)", (word))
            return True
        return False

    def getMaxGarbageWord(self):
        cursor = self.connection.cursor()
        cursor.execute('SELECT `id`, `word` FROM `garbage` ORDER BY `id` DESC LIMIT 1')
        return cursor.fetchone()

    def getFinanceDataByStockNameAndData(self, stockName, sliceDate):
        cursor = self.connection.cursor()
        cursor.execute("SELECT f.id, f.start, f.final FROM finance f, stock s WHERE f.stockId = s.id and s.name = %s and f.date = %s", (stockName, sliceDate))
        return cursor.fetchone()

    def getContent(self, stockId, startPos, endPos):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `c`.`title`,`c`.`contentData`, `a`.`name`, `c`.`date` FROM `content` as `c`, `author` as `a` WHERE `c`.`stockId` = %s limit %s , %s", (stockId, startPos, endPos))
        return cursor.fetchall()

    def countContents(self, stockId, startAt, limitAt):
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(c.id) as cnt FROM content c WHERE c.stockId = %s and c.date > %s and c.date <= %s", (stockId, startAt, self.getPlusOneDay(limitAt)))
        return cursor.fetchone()

    def getContentBetween(self, stockId, startAt, limitAt, startPos, endPos):
        cursor = self.connection.cursor()
        cursor.execute("SELECT c.id, c.yet, c.date FROM content c WHERE c.stockId = %s and c.date > %s and c.date <= %s ORDER BY c.id DESC LIMIT %s , %s", (stockId, startAt, self.getPlusOneDay(limitAt), startPos, endPos))
        result = cursor.fetchall()
        if result is not None :
            return list(result)
        return result

    def getFinancePrice(self, financeId):
        if financeId is None :
            print('financeId is none')
            return 0
        cursor = self.connection.cursor()
        cursor.execute("select final - start as result from finance where id = %s", financeId)
        result = cursor.fetchone()
        return result.get('result')
    def insertGarbage(self, word, contentId) :
        cursor = self.connection.cursor()
        insertGarbageSql = "INSERT INTO `garbage` (`word`,`contentId`) VALUES (%s, %s)"
        cursor.execute(insertGarbageSql, (word, contentId))

    def selectStockByCode(self, stockCode):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`code`,`name`,`lastUseDateAt`, `hit`, `lastScrapAt` FROM `stock` WHERE `code` like %s", ('%'+stockCode))
        return cursor.fetchone()

    def selectFinanceByStockIdAndDate(self, stockId, date):
        cursor = self.connection.cursor()
        selectSql = "SELECT `id`,`stockId`,`date` FROM `finance` WHERE `stockId`=%s AND date = %s"
        cursor.execute(selectSql, (stockId, date))
        return cursor.fetchone()

    def insertFinance(self, stockId, date, high, low, start, final):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`finance` (`stockId`,`date`,`high`,`low`,`start`,`final`) VALUES (%s, %s, %s, %s, %s, %s);", (stockId, date, high, low, start, final))

    def getFinanceDataByDay(self, stockId, date, dateStr):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`stockId`,`date` FROM `finance` WHERE `stockId`=%s AND date = %s AND createdAt < %s", (stockId, date, dateStr))
        return cursor.fetchone()

    def updateFinance(self, high, low, start, final, financeId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `data`.`finance` SET `high`=%s,`low`=%s,`start`=%s,`final`=%s WHERE `id`=%s;", (high, low, start, final, financeId))

    def insertStock(self, code, name):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`stock` (`code`,`name`,`use`, `scrap`) VALUES (%s, %s, 1, 1);", (code, name))

    def updateStockHit(self, hit, stockId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `stock` SET `hit`=%s WHERE `id`=%s", (hit, stockId))

    def selectAnalyze(self, analyzed):
        cursor = self.connection.cursor()
        contentSelectSql = "SELECT `id`,`title`,`contentData`,`authorId`,`date`,`analyze`,`createdAt` FROM `content` WHERE `analyze`=%s LIMIT 1"
        cursor.execute(contentSelectSql, (analyzed))
        return cursor.fetchone()

    def updateContentAnalyzeFlag(self, analyzeFlag, contentId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `content` SET `analyze`=%s WHERE `id`=%s", (analyzeFlag, contentId))

    def initScrapStock(self):
        self.connection.cursor().execute("UPDATE stock SET `scrap` = 1 WHERE `much` = 0")
        self.commit()

    def selectLastestFinance(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT date FROM finance WHERE stockId =%s ORDER BY date DESC LIMIT 1", stockId)
        result = cursor.fetchone()
        if result is not None :
            return result.get('date')
        return None

    def updateStockName(self, stockId, dsName, dsCode):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `stock` SET `name`=%s, `code`=%s WHERE `id`=%s", (dsName, dsCode, stockId))

    def selectContentIdList(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id FROM content WHERE stockId=%s", stockId)
        return cursor.fetchall()

    def updateContentQuery(self, contentId, stockId):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `content` SET `stockId`=%s WHERE `id`=%s", (stockId, contentId))

    def updateStockUse(self, stockId, useFlag):
        cursor = self.connection.cursor()
        cursor.execute(("UPDATE stock SET `use` = %s, `lastUseDateAt` = now() WHERE `id` = %s"), (useFlag, stockId))
        self.commit()

    def selectLastestItem(self, stockId, period):
        cursor = self.connection.cursor()
        cursor.execute("select targetAt from item where period = %s and stockId = %s order by targetAt DESC limit 1", (period, stockId))
        result = cursor.fetchone()
        if result is not None :
            return result.get('targetAt').date()
        return None

    def selectFirstContentDate(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("select date from content where stockId = %s and date !='1970-12-31 23:59:59' order by date asc limit 1;", (stockId))
        result = cursor.fetchone()
        if result is not None :
            return result.get('date').date()
        return None

    def insertOrUpdateStockPotential(self, stockId, period, potential, count):
        cursor = self.connection.cursor()
        cursor.execute("select id from potential where stockId = %s and period = %s", (stockId, period))
        result = cursor.fetchone()
        if result is not None :
            cursor.execute("update potential set potential = %s, count = %s where id = %s ", (potential, count, result.get('id')))
        else :
            cursor.execute("INSERT INTO potential (`stockId`,`period`,`potential`, `count`) VALUES (%s, %s, %s, %s)", (stockId, period, potential, count))

    def selectPotentialStock(self, stockId, period):
        cursor = self.connection.cursor()
        cursor.execute("select stockId, potential, count from potential where stockId = %s and period = %s", (stockId, period))
        return cursor.fetchone()

    def updateStockMuch(self, stockId, much):
        cursor = self.connection.cursor()
        cursor.execute("update stock set much = %s where id = %s ", (much, stockId))

    def getAllStockList(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`, `code`, `name`, `lastUseDateAt`, `lastScrapAt`, `much` FROM stock ORDER BY id ASC")
        return cursor.fetchall()

    def selectItemByFinanceIsNull(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, targetAt FROM item WHERE stockId = %s AND financeId is NULL", (stockId))
        return cursor.fetchall()

    def checkHolyDay(self, targetAt):
        cursor = self.connection.cursor()
        cursor.execute("select id from holyday where date = %s", (targetAt))
        results = cursor.fetchall()
        return len(results) > 0


    def getItemTargetAtList(self):
        cursor = self.connection.cursor()
        cursor.execute("select id, targetAt from item where targetAt >= '2016-05-16' order by createdAt desc;")
        return cursor.fetchall()

    def insertHolyday(self, targetAt, reason):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `holyday` (`date`, `weekday`, `name`) VALUES (%s, %s, %s)", (targetAt, targetAt.weekday(), reason))

    def insertItemDefault(self, stockId, forecastAt, period):
        cursor = self.connection.cursor()
        cursor.execute("select id from item where stockId = %s and targetAt = %s and period = %s", (stockId, forecastAt, period))
        result = cursor.fetchall()
        if len(result) == 0 :
            cursor.execute("INSERT INTO item (`stockId`, `targetAt`, `period`, `yet`) VALUES (%s, %s, %s, %s)", (stockId, forecastAt, period, self.WORK_YET))

    def selectItemByTargetAtAndPeriodAndYet(self, forecastAt, period, yet):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE targetAt = %s AND period = %s AND yet = %s LIMIT  1", (forecastAt, period, yet))
        return cursor.fetchone()

    def selectStockById(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`code`,`name`,`lastUseDateAt`, `hit`, `lastScrapAt` FROM `stock` WHERE `id` = %s", (stockId))
        return cursor.fetchone()

    def updateItemYet(self, itemId, yet):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `data`.`item` SET `yet`=%s WHERE `id`=%s", (yet, itemId))
        self.commit()

    def selectItemListByCnt(self, plus, minus, plusTot, minusTot):
        cursor = self.connection.cursor()
        cursor.execute("select `id` from `item` where `plus`= %s and `minus` = %s and `totalPlus` = %s and `totalMinus` = %s and duration is null", (plus, minus, plusTot, minusTot))
        return cursor.fetchall()
    def updateDefaultItemList(self):
        items = self.selectItemListByCnt(0, 0, 0, 0)
        for item in items :
            self.updateItemYet(item.get('id'), self.WORK_YET)

    def selectItemByPeriodAndYet(self, period, yet):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE period = %s AND yet = %s LIMIT  1", (period, yet))
        return cursor.fetchone()

    def getPlusOneDay(self, limitAt):
         return limitAt + datetime.timedelta(days=1)

    def getTargetStartAt(self, targetAt, period):
        return targetAt - datetime.timedelta(days=period) - datetime.timedelta(days=period)

    def getWorkYetItems(self, forecastAt, period):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE period = %s AND yet = %s AND targetAt = %s", (period, self.WORK_YET, forecastAt))
        return cursor.fetchall()

    def selectItem(self, id):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, stockId, period, targetAt, yet FROM item WHERE id=%s ORDER BY createdAt ASC", (id))
        return cursor.fetchone()

    def insertContentMap(self, contentId, wordId):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`content_map` (`contentId`, `wordId`) VALUES (%s, %s)", (contentId, wordId))

    def getContentWordIds(self, contentId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, contentId, wordId FROM content_map WHERE contentId=%s", (contentId))
        return cursor.fetchall()

    def updateContentYet(self, contentId, yet):
        cursor = self.connection.cursor()
        cursor.execute("UPDATE `data`.`content` SET `yet`=%s WHERE `id`=%s", (yet, contentId))
        self.commit()

    def getContentById(self, contentId):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id, contentData, yet FROM content WHERE id=%s", (contentId))
        return cursor.fetchone()

    def getPeriodAll(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT distinct(period) FROM item")
        results = list()
        for each in cursor.fetchall():
            results.append(each.get('period'))
        return results

    def deleteItemDefault(self, stockId, forecastAt, period):
        cursor = self.connection.cursor()
        cursor.execute("select id from item where stockId = %s and targetAt = %s and period = %s and yet = 1", (stockId, forecastAt, period))
        results = cursor.fetchall()
        if len(results) > 0:
            for each in results:
                print('delete', each.get('id'))
                cursor.execute('DELETE FROM `data`.`item` WHERE `id`=%s', (each.get('id')))

    def insertFinanceNullDates(self):
        cursor = self.connection.cursor()
        cursor.execute("select distinct(i.targetAt) from item i, stock s where i.financeId is null and s.id = i.stockId and s.much = 0 and i.targetAt < (select targetAt from item where financeId is not null order by targetAt desc limit 1)")
        results = cursor.fetchall()
        for each in results :
            self.insertHolyday(each.get('targetAt'), 'finance is null')
        self.commit()

    def getBeforeFinanceId(self, itemId):
        cursor = self.connection.cursor()
        item = self.getItem(itemId)
        cursor.execute("select * from finance where stockId = %s and date < %s order by date desc limit 1", (item.get('stockId'), item.get('targetAt')))
        result = cursor.fetchone()
        if result is not None :
            return result.get('id')
        else:
            return None

    def getItem(self, itemId):
        cursor = self.connection.cursor()
        cursor.execute("select i.id, i.stockId, i.financeId, i.targetAt from item i where i.id = %s", (itemId))
        return cursor.fetchone()

    def selectItemList(self, stockId):
        cursor = self.connection.cursor()
        cursor.execute("select * from data.item where stockId = %s", (stockId))
        return cursor.fetchall()

    def hasEventWord(self, itemId, wordId):
        cursor = self.connection.cursor()
        cursor.execute("select count(id) as c from data.event where itemId = %s and wordId = %s", (itemId, wordId))
        cnt = cursor.fetchone().get('c')
        return cnt == 0

    def insertEvent(self, itemId, wordId):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO `data`.`event` (`itemId`, `wordId`) VALUES (%s, %s)", (itemId, wordId))

    def selectSitePerCount(self, startAt, endAt):
        cursor = self.connection.cursor()
        cursor.execute("select site, count(id) from content where date between %s and date %s group by site", (startAt, endAt))
        return cursor.fetchall()

    def getPotentialDatas(self, limitRate):
        cursor = self.connection.cursor()
        cursor.execute("select max(analyzeAt) as analyzeMax, max(evaluate) as evaluateMax from data.forecast")
        result = cursor.fetchone()
        target_at = result.get('analyzeMax') - timedelta(days=result.get('evaluateMax'))
        query = "select type, code, analyzeAt, potential, volume from data.forecast where analyzeAt > %s and potential > %s order by analyzeAt, code asc"
        cursor.execute(query, (target_at, str(limitRate)))
        return cursor.fetchall()

    def updateDailyStocksByCode(self, code, name):
        cursor = self.connection.cursor()
        cursor.execute("select id, name from data.daily_stock where code = %s", (code))
        results = cursor.fetchall()
        for result in results:
            if result.get('name') != name :
                cursor.execute("UPDATE `data`.`daily_stock` SET `name`=%s WHERE `id`=%s", (name, result.get('id')))

