import urllib
import dictionary
import dbmanager


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
        self.dbm = dbmanager.DBManager(DB_IP, DB_USER, DB_PWD, DB_SCH)

    def commit(self):
        self.dbm.commit()

    def __del__(self):
        self.dbm.commit()
        self.dbm.close()

    def analyze(self):
        while True:
            target = self.dbm.selectAnalyze('N')
            if target is None:
                break
            self.dbm.updateContentAnalyzeFlag('Y', target.get('id'))
            self.commit()
            print('start : ' + str(target.get('id')))
            self.analyzeDictionary(target.get('contentData'), target.get('id'), 0)
            self.commit()
            print('fin : ' + str(target.get('id')))

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
                dic.commit()
            idx = 0
        except urllib.error.URLError as e:
            print(e)
            print('retry analyzeDictionary ' + str(idx))
            dic.commit()
            self.analyzeDictionary(data, contentId, idx)
        except:
            print('uncaught except')
            dic.commit()
        finally:
            dic.commit()
