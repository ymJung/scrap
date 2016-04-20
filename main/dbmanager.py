import datetime
import pymysql.cursors
import sys
import re

class DBManagerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)

class DBManager:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.LIMIT_HOUR = 16
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
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
    def commit(self):
        self.connection.commit()
        print('dbm commit')
    def close(self):
        print('dbm close')
        self.connection.close()
    def saveData(self, site, results, stockName):
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
            contentId = self.saveContentAndGetId(site, authorId, contentData, date, title, stockName)

            commentList = each.get(self.COMMENT_LIST)
            if len(commentList) > 0:
                for comment in commentList:
                    commentWriter = comment.get(self.WRITER)
                    commentAuthorId = self.saveAuthorAndGetId(site, commentWriter)
                    commentDate = comment.get(self.DATE)
                    commentContent = comment.get(self.CONTENT_DATA)
                    self.insertComment(commentAuthorId, commentContent, contentId, commentDate, stockName)

    def insertComment(self, commentAuthorId, commentContent, contentId, commentDate, stockName):
        try:
            cursor = self.connection.cursor()
            commentIdSql = "SELECT `id` FROM `comment` WHERE `authorId`=%s AND `commentData`=%s"
            commentId = cursor.execute(commentIdSql, (commentAuthorId, commentContent))

            if commentId == 0:
                commentDataInsertSql = "INSERT INTO `comment` (`authorId`, `commentData`, `contentId`, `date`, `query`) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate, stockName))
        except pymysql.err.InternalError as e:
            print(e)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            pass
    def replaceEscape(self, text):
        return re.sub(self.REGULAR_EXP, ' ', text)



    def saveContentAndGetId(self, site, authorId, contentData, date, title, stockName):
        cursor = self.connection.cursor()
        title = self.replaceEscape(title)
        contentData = self.replaceEscape(contentData)
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
        print(u'saveContentAndGetId', authorId, title)
        contentId = cursor.execute(contentIdSql, (authorId, title))
        if contentId == 0:
            contentDataInsertSql = "INSERT INTO `content` (`title`, `contentData`, `authorId`, `date`, `query`, `site`) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(contentDataInsertSql, (title, contentData, authorId, date, stockName, site))
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
        cursor.execute("SELECT `id`, `code`, `name`, `lastUseDateAt` FROM stock where `much` = 0 ORDER BY id ASC")
        return cursor.fetchall()

    def initStock(self):
        self.connection.cursor().execute("UPDATE stock SET `use` = 1 WHERE `much` = 0")
        self.commit()

    def getUsefulStock(self, checkDate):
        cursor = self.connection.cursor()
        selectSql = "SELECT `id`, `code`, `name`, `lastUseDateAt` FROM stock WHERE `use` = 1 AND `much` = 0 ORDER BY id asc LIMIT 1"
        cursor.execute(selectSql)
        stock = cursor.fetchone()
        if stock is None :
            cursor.execute("select lastUseDateAt from stock order by lastUseDateAt desc limit 1")
            lastUseDateAt = cursor.fetchone().get('lastUseDateAt')
            today = datetime.date.today()
            workIsDone = (today.year == lastUseDateAt.year) and (today.month == lastUseDateAt.month) and (today.day == lastUseDateAt.day) # and datetime.datetime.now().hour > self.LIMIT_HOUR
            if checkDate:
                if workIsDone:
                    raise DBManagerError('stock is none')
                else :
                    print('init stock')
                    self.initStock()
                    cursor.execute(selectSql)
                    stock = cursor.fetchone()
        cursor.execute(("UPDATE stock SET `use` = 0 WHERE `id` = %s"), stock.get('id'))
        self.commit()
        return stock

    def saveAnalyzedData(self, stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period):
        cursor = self.connection.cursor()
        cursor.execute('SELECT id FROM stock WHERE name = %s', stockName)
        stock = cursor.fetchone()
        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s LIMIT 1', (stock.get('id'), targetAt, period))
        item = cursor.fetchone()
        if item is  None :
            cursor.execute("INSERT INTO `data`.`item` (`stockId`, `plus`, `minus`, `totalPlus`, `totalMinus`, `targetAt`, `period`) VALUES (%s, %s, %s, %s, %s, %s, %s)", (stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period))

        # else :
        #     cursor.execute("UPDATE `data`.`item` SET `stockId`=%s, `plus`=%s, `minus`=%s, `totalPlus`=%s, `totalMinus`=%s, `targetAt`=%s, `period` WHERE `id`=%s",
        #                    (stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, period, item.get('id')))

        cursor.execute('SELECT id FROM item WHERE `stockId` = %s AND `targetAt` = %s AND `period` = %s', (stock.get('id'), targetAt, period))
        return cursor.fetchone().get('id')

    def updateLastUseDate(self, stock):
        cursor = self.connection.cursor()
        updateLastUseDateSql = "UPDATE `data`.`stock` SET `lastUseDateAt`= now() WHERE `id`= %s"
        result = cursor.execute(updateLastUseDateSql, (stock.get('id')))
        print('update' + str(result))
        self.connection.commit()

    def updateAnalyzedResultItem(self, stock):
        cursor = self.connection.cursor()
        stockName = stock.get('name')
        stockId = stock.get('id')

        selectTargetItemsSql = 'SELECT id, targetAt FROM item WHERE financeId IS NULL AND stockId = %s'
        cursor.execute(selectTargetItemsSql, (stockId))
        targets = cursor.fetchall()
        for target in targets:
            updated = False
            selectTargetStockSql = 'SELECT f.id, s.name, f.date, f.start, f.final, f.date FROM finance f, stock s WHERE f.stockId = s.id AND s.name = %s'
            cursor.execute(selectTargetStockSql + ' AND f.date = %s', (stockName, target.get('targetAt')))
            targetFinances = cursor.fetchall()

            for targetFinance in targetFinances: # 아마 로직에 변경 없으면 1개임.
                updated = True
                self.updateItemPrice(targetFinance.get('id'), target.get('id'))
            if updated is False :
                existFinance = cursor.execute(selectTargetStockSql +  ' AND f.date > %s LIMIT 1' , (stockName, target.get('targetAt')))
                if existFinance != 0 :
                    targetFinance = cursor.fetchone()
                    self.updateItemPrice(targetFinance.get('id'), target.get('id'))

    def updateItemPrice(self, financeId, itemId):
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

    def analyzedSql(self, stockName):
        cursor = self.connection.cursor()
        selectAnalyzedSql = 'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final FROM item i, stock s, finance f WHERE i.stockId = s.id AND f.id = i.financeId AND s.name = %s group by i.targetAt order by i.targetAt desc'
        cursor.execute(selectAnalyzedSql, (stockName))
        analyzedResult = cursor.fetchall()

        return analyzedResult

    def forecastTarget(self, forecastAt, stock, targetAt):
        stockId = stock.get('id')
        stockName = stock.get('name')
        lastUseDateAt = stock.get('lastUseDateAt')
        if lastUseDateAt.date() < targetAt :
            print('not yet to scrap.', stockName, targetAt)
            return True
        cursor = self.connection.cursor()
        result = cursor.execute('SELECT id FROM item WHERE targetAt = %s and stockId = %s', (forecastAt, stockId))
        if result != 0:
            print('exist item date ', forecastAt, stockId)
            return True
        result = cursor.execute('SELECT `id` FROM `content` WHERE `date` BETWEEN %s AND %s AND `query` = %s', (targetAt, forecastAt + datetime.timedelta(days=1), stockName))
        if result == 0:
            print('empty content data.', targetAt, forecastAt, stockName)
            return True

        return False



    def saveAnalyzedItemFinanceList(self, itemId, financeIdList):
        cursor = self.connection.cursor()
        for financeId in financeIdList :
            cursor.execute("INSERT INTO chart_map (itemId, financeId) VALUES (%s, %s)", (itemId, financeId))

    def getFinanceListFromItemId(self, itemId):
        cursor = self.connection.cursor()
        financeIds = cursor.execute('SELECT cm.financeId FROM chart_map cm WHERE cm.itemId=%s', (itemId))
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

    def getForecastResult(self, stockName, limitAt, period):
        cursor = self.connection.cursor()
        selectForecastSql =  'SELECT i.id, s.name,i.plus,i.minus, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt FROM item i, stock s ' \
                             'WHERE i.stockId = s.id AND s.name = %s AND i.targetAt = %s AND i.period = %s ORDER BY i.id DESC' # AND i.financeId IS NULL
        cursor.execute(selectForecastSql, (stockName, limitAt, period))
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

    def getContent(self, stockName, startPos, endPos):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `c`.`title`,`c`.`contentData`, `a`.`name`, `c`.`date` FROM `content` as `c`, `author` as `a` WHERE `c`.`query` = %s limit %s , %s", (stockName, startPos, endPos))
        return cursor.fetchall()

    def countContents(self, stockName, limitAt, startAt):
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(c.id) as cnt FROM content c WHERE c.query = %s and c.date between %s and %s", (stockName, limitAt, startAt))
        return cursor.fetchone()

    def getContentBetween(self, stockName, startAt, limitAt, startPos, endPos):
        cursor = self.connection.cursor()
        cursor.execute("SELECT c.title,c.contentData, c.date FROM content c WHERE c.query = %s and c.date > %s and c.date <= %s ORDER BY c.id DESC LIMIT %s , %s", (stockName, startAt, limitAt + datetime.timedelta(days=1), startPos, endPos))
        result = cursor.fetchall()
        if result is not None :
            return list(result)
        return result

    def getFinancePrice(self, financeId):
        cursor = self.connection.cursor()
        cursor.execute("select start, final from finance where id = %s", financeId)
        result = cursor.fetchone()
        price = result.get('start') - result.get('final')
        return price
    def insertGarbage(self, word, contentId) :
        cursor = self.connection.cursor()
        insertGarbageSql = "INSERT INTO `garbage` (`word`,`contentId`) VALUES (%s, %s)"
        cursor.execute(insertGarbageSql, (word, contentId))

    def selectStockByCode(self, stockCode):
        cursor = self.connection.cursor()
        cursor.execute("SELECT `id`,`code`,`name`,`lastUseDateAt`, `hit` FROM `stock` WHERE `code` like %s", ('%'+stockCode))
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






