
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

    def saveData(self, results, stockName):
        for each in results:
            authorName = each.get(self.WRITER)
            title = each.get(self.TITLE)
            contentData = each.get(self.CONTENT_DATA)
            date = each.get(self.DATE)
            if len(authorName) == 0 or len(title) == 0 or len(contentData) == 0 :
                print('data is wrong' + str(each))
                continue

            authorId = self.saveAuthorAndGetId(authorName)
            contentId = self.saveContentAndGetId(authorId, contentData, date, title, stockName)
            commentList = each.get(self.COMMENT_LIST)

            for comment in commentList:
                commentWriter = comment.get(self.WRITER)
                commentAuthorId = self.saveAuthorAndGetId(commentWriter)
                commentDate = comment.get(self.DATE)
                commentContent = comment.get(self.CONTENT_DATA)

                self.insertComment(commentAuthorId, commentContent, contentId, commentDate, stockName)

    def insertComment(self, commentAuthorId, commentContent, contentId, commentDate, stockName):
        try :
            cursor = self.connection.cursor()
            commentIdSql = "SELECT `id` FROM `comment` WHERE `authorId`=%s AND `commentData`=%s"
            commentId = cursor.execute(commentIdSql, (commentAuthorId, commentContent))

            if commentId == 0:
                commentDataInsertSql = "INSERT INTO `comment` (`authorId`, `commentData`, `contentId`, `date`, `query`) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate))
        except pymysql.err.InternalError as e:
            print(e)
        except :
            print("Unexpected error:", sys.exc_info()[0])
            pass


    def saveContentAndGetId(self, authorId, contentData, date, title, stockName):
        cursor = self.connection.cursor()
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
        contentId = cursor.execute(contentIdSql, (authorId, title))
        if contentId == 0:
            contentDataInsertSql = "INSERT INTO `content` (`title`, `contentData`, `authorId`, `date`, `query`) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(contentDataInsertSql, (title, contentData, authorId, date, stockName))
            cursor.execute(contentIdSql, (authorId, title))
            contentId = cursor.fetchone().get('id')
        else:
            contentId = cursor.fetchone().get('id')
        return contentId

    def saveAuthorAndGetId(self, authorName):
        cursor = self.connection.cursor()
        authorIdSql = "SELECT `id` FROM `author` WHERE `name`= %s"
        authorId = cursor.execute(authorIdSql, (authorName))
        if authorId == 0:
            authorDataInsertSql = "INSERT INTO `author` (`name`) VALUES (%s)"
            cursor.execute(authorDataInsertSql, (authorName))
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

    def saveAnalyzedData(self, stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt):
        cursor = self.connection.cursor()
        authorDataInsertSql = "INSERT INTO `data`.`item` (`query`, `plus`, `minus`, `totalPlus`, `totalMinus`, `targetAt`) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(authorDataInsertSql, (stockName, plusCnt, minusCnt, totalPlusCnt, totalMinusCnt, targetAt))

    def updateLastUseDate(self, stock):
        cursor = self.connection.cursor()
        updateLastUseDateSql = "UPDATE `data`.`stock` SET `lastUseDateAt`= now() WHERE `id`= %s"
        cursor.execute(updateLastUseDateSql, (stock.get('id')))

    def updateAnalyzedResultItem(self, stock): #TODO - fix error
        cursor = self.connection.cursor()
        selectTargetDatesSql  = 'select targetAt from item where originPrice is null and query = %s'
        cursor.execute(selectTargetDatesSql, (stock))
        targets = cursor.fetchall()
        for target in targets :
            targetAt = target.get('targetAt')
            selectTargetStockSql = 'select s.name, f.date, f.start, f.final from finance f, stock s where f.stockId = s.id and f.date = %s and s.name = %s'
            cursor.execute(selectTargetStockSql, (targetAt, stock))
            targetStocks = cursor.fetchall()

            for targetStock in targetStocks :
                targetStock.get('')
                ## update.calc




