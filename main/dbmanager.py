
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

    def saveData(self, result):
        connection = self.connection
        cursor = connection.cursor()

        for each in result:
            authorName = each.get(self.WRITER)
            title = each.get(self.TITLE)
            contentData = each.get(self.CONTENT_DATA)
            date = each.get(self.DATE)

            authorId = self.getAuthorId(authorName, cursor)
            contentId = self.getContentId(authorId, contentData, cursor, date, title)
            commentList = each.get(self.COMMENT_LIST)

            for comment in commentList:
                commentWriter = comment.get(self.WRITER)
                commentAuthorId = self.getAuthorId(commentWriter, cursor)
                commentDate = comment.get(self.DATE)
                commentContent = comment.get(self.CONTENT_DATA)

                self.insertComment(cursor, commentAuthorId, commentContent, contentId, commentDate)

        self.finalize()

    def insertComment(self, cursor, commentAuthorId, commentContent, contentId, commentDate):
        try :

            commentIdSql = "SELECT `id` FROM `comment` WHERE `authorId`=%s AND `commentData`=%s"
            commentId = cursor.execute(commentIdSql, (commentAuthorId, commentContent))

            if commentId == 0:
                commentDataInsertSql = "INSERT INTO `comment` (`authorId`, `commentData`, `contentId`, `date`) VALUES (%s, %s, %s, %s)"
                cursor.execute(commentDataInsertSql, (commentAuthorId, commentContent, contentId, commentDate))
        except pymysql.err.InternalError as e:
            print(e)
        except :
            print("Unexpected error:", sys.exc_info()[0])
            pass


    def getContentId(self, authorId, contentData, cursor, date, title):
        contentIdSql = "SELECT `id` FROM `content` WHERE `authorId`=%s AND `title`=%s"
        contentId = cursor.execute(contentIdSql, (authorId, title))
        if contentId == 0:
            contentDataInsertSql = "INSERT INTO `content` (`title`, `contentData`, `authorId`, `date`) VALUES (%s, %s, %s, %s)"
            cursor.execute(contentDataInsertSql, (title, contentData, authorId, date))
            cursor.execute(contentIdSql, (authorId, title))
            contentId = cursor.fetchone().get('id')
        else:
            contentId = cursor.fetchone().get('id')
        return contentId

    def getAuthorId(self, authorName, cursor):
        authorIdSql = "SELECT `id` FROM `author` WHERE `name`=%s"
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
