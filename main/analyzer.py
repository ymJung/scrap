
import pymysql.cursors
import urllib
import dictionary


class AnalyzerError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class Analyzer:
    def __init__(self, DB_IP, DB_USER, DB_PWD, DB_SCH):
        self.DB_IP = DB_IP
        self.DB_USER = DB_USER
        self.DB_PWD = DB_PWD
        self.DB_SCH = DB_SCH
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def commit(self):
        self.connection.commit()
    def analyze(self):
        while True :
            target = self.analyzeTarget()
            if target is None :
                break
            self.updateAnalyzeFlag(target.get('id'), 'Y')
            self.connection.commit()
            print('start : ' + str(target.get('id')))
            self.analyzeDictionary(target.get('contentData'), target.get('id'), 0)
            self.connection.commit()
            print('fin : ' + str(target.get('id')))

    def analyzeTarget(self):
        cursor = self.connection.cursor()
        contentSelectSql = "SELECT `id`,`title`,`contentData`,`authorId`,`date`,`analyze`,`createdAt` FROM `content` WHERE `analyze`=%s LIMIT 1"
        contentDataCursor = cursor.execute(contentSelectSql, ('N'))
        if contentDataCursor != 0:
            result = cursor.fetchone()

            return result
        return None

    def updateAnalyzeFlag(self, contentId, analyzeFlag):
        cursor = self.connection.cursor()
        contentUpdateSql = "UPDATE `content` SET `analyze`=%s WHERE `id`=%s"
        cursor.execute(contentUpdateSql, (analyzeFlag, contentId))

    def insertDelimiter(self, data):
        cursor = self.connection.cursor()
        delimiterIdSelectSql = "SELECT `id` FROM `delimiter` WHERE `word`=%s"
        delimiterIdCursor = cursor.execute(delimiterIdSelectSql, (data))
        if delimiterIdCursor == 0:
            delimiterInsertSql = "INSERT INTO `delimiter` (`word`) VALUES (%s)"
            cursor.execute(delimiterInsertSql, (data))

        return delimiterIdCursor.fetchone().get('id')

    def analyzeDictionary(self, data, contentId, idx):
        dic = dictionary.Dictionary(self.DB_IP, self.DB_USER, self.DB_PWD, self.DB_SCH)
        try:
            splitStrings = dic.splitStr(data)
            for i in range(len(splitStrings)):
                if idx > i:
                    print('start in middle ' + str(idx))
                    i = idx
                splitString = dic.getRegularExpression(splitStrings[i])
                if dic.existSplitWord(splitString) is False:
                    findWord = False
                    for j in range(len(splitString)):
                        subStr = splitString[0:len(splitString) - j]
                        if dic.isTargetWord(subStr) and dic.isInsertTarget(subStr) is True:
                            dic.insertWord(subStr)
                            findWord = True

                    if findWord is False and dic.existWord(splitString) is False:
                        dic.insertGarbageWord(splitString, contentId)
                    idx = i
                dic.connection.commit()
            idx = 0
        except urllib.error.URLError as e:
            print(e)
            print('retry analyzeDictionary ' + str(idx))
            dic.connection.commit()
            self.analyzeDictionary(data, contentId, idx)
        except:
            print('uncaught except')
            dic.connection.commit()
        finally:
            dic.connection.commit()
