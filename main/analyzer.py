DB_IP = "localhost"
DB_USER = "root"
DB_PWD = "1234"
DB_SCH = "data"

import pymysql.cursors
class Analyzer:
    def __init__(self):
        self.connection = pymysql.connect(host=DB_IP,
                                          user=DB_USER,
                                          password=DB_PWD,
                                          db=DB_SCH,
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def analyze(self, data):
        print(data)

    def insertDelimiter(self, data):
        cursor = self.connection.cursor()
        delimiterIdSelectSql = "SELECT `id` FROM `delimiter` WHERE `word`=%s"
        delimiterIdCursor = cursor.execute(delimiterIdSelectSql, (data))
        if delimiterIdCursor == 0:
            delimiterInsertSql = "INSERT INTO `delimiter` (`word`) VALUES (%s)"
            cursor.execute(delimiterInsertSql, (data))

        return delimiterIdCursor.fetchone().get('id')


data = u'눈여겨 보는 종목입니다 어제 한번 출렁하면서 음봉10% 나왔다가 양봉으로 마무리하는 모습 보였는데 실탄이 있었다면... 아쉽더군요 외인의 매수세도 늘어가는거 같고 한달에 10주에서 20주씩 적금든다 생각하고 한 2년 투자하면 어떨까하는데 고수님들 생각은 어떠신지요'

an = Analyzer()
an.insertDelimiter('는')

# space 단위 분리
# > 한글자씩 줄여가며 사전api 호출
# >> 검색되면 - 단어 추가
# >> 검색안되면 - 패스.
# > 처리된 content는 플래그 처리