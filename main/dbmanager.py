import datetime
import pymysql.cursors
import sys


class DBManager:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
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

    def saveData(self, site, results, stockName):
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
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate))
        except pymysql.err.InternalError as e:
            print(e)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            pass

    def saveContentAndGetId(self, site, authorId, contentData, date, title, stockName):
        cursor = self.connection.cursor()
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
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

    def finalize(self):
        self.connection.commit()
        self.connection.close()
        print('end')

    def getUsefulStockList(self):
        cursor = self.connection.cursor()
        usefulStockSql = "SELECT `id`, `code`, `name`, `lastUseDateAt` FROM stock WHERE `use` = 1"
        cursor.execute(usefulStockSql)
        stocks = cursor.fetchall()
        return stocks

    def saveAnalyzedData(self, stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, targetPlusAvg,
                         targetMinusAvg):
        cursor = self.connection.cursor()
        authorDataInsertSql = "INSERT INTO `data`.`item` (`stockId`, `plus`, `minus`, `totalPlus`, `totalMinus`, `targetAt`, `plusAvg`, `minusAvg`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute('SELECT id FROM stock WHERE name = %s', stockName)
        stock = cursor.fetchone()
        cursor.execute(authorDataInsertSql, (
        stock.get('id'), plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt, float(targetPlusAvg), float(targetMinusAvg)))

    def updateLastUseDate(self, stock):
        cursor = self.connection.cursor()
        updateLastUseDateSql = "UPDATE `data`.`stock` SET `lastUseDateAt`= now() WHERE `id`= %s"
        cursor.execute(updateLastUseDateSql, (stock.get('id')))

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



    def analyzedSql(self, stockName):
        cursor = self.connection.cursor()
        selectAnalyzedSql = 'SELECT i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt, i.financeId, f.start, f.final FROM item i, stock s, finance f WHERE i.stockId = s.id AND f.id = i.financeId AND s.name = %s order by i.id desc';
        cursor.execute(selectAnalyzedSql, (stockName))
        analyzedResult = cursor.fetchall()
        selectForecastSql =  'SELECT i.id, s.name,i.plus,i.minus,i.plusAvg,i.minusAvg, i.totalPlus, i.totalMinus, i.targetAt,i.createdAt FROM item i, stock s WHERE i.stockId = s.id AND s.name = %s AND i.financeId IS NULL order by i.id desc'
        cursor.execute(selectForecastSql, (stockName))
        forecastResult = cursor.fetchall()
        return analyzedResult, forecastResult


