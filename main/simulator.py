import pymysql
import configparser

cf = configparser.ConfigParser()
cf.read('config/config.cfg')
DB_IP = cf.get('db', 'DB_IP')
DB_USER = cf.get('db', 'DB_USER')
DB_PWD = cf.get('db', 'DB_PWD')
DB_SCH = cf.get('db', 'DB_SCH')

connection = pymysql.connect(host=DB_IP,
                             user=DB_USER,
                             password=DB_PWD,
                             db=DB_SCH,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

def select_distinct_stocks():
    cursor = connection.cursor()
    cursor.execute("select distinct(code) from daily_stock")
    results = list()
    for code in cursor.fetchall():
        results.append(code.get('code'))
    return results

def get_potential_datas(limit_rate, code):
    cursor = connection.cursor()
    query = "SELECT ds.name, f.type, f.code, f.analyzeAt, f.potential, f.volume , f.percent, f.evaluate FROM data.forecast f, data.daily_stock ds " \
            "WHERE f.type = 3 AND ds.code = f.code AND potential > %s AND f.code = %s group by f.id ORDER BY f.analyzeAt, f.code ASC"
    cursor.execute(query, (str(limit_rate), code))
    return cursor.fetchall()


def is_compare_chain_minus(code, analyze_at, day_cnt):
    cursor = connection.cursor()
    cursor.execute("select date from data.daily_stock ds "
                   "where ds.code = %s and ds.date < %s order by ds.id desc limit %s", (code, analyze_at, day_cnt))
    dates = cursor.fetchall()

    result = True
    for date in dates:
        cursor.execute(
            "select (ds.close-ds.open) as compare from data.daily_stock ds where ds.code = %s and ds.date = %s",
            (code, date.get('date')))
        compare = cursor.fetchone().get('compare')
        if compare > 0:
            result = False
    return result


use_val = 100
datas = get_potential_datas(0.70, 'A000050')
for data in datas:
    if is_compare_chain_minus(code=data.get('code'), analyze_at=data.get('analyzeAt'), day_cnt=1):
        percent = data.get('percent')
        use_val = use_val + (use_val * percent)
        print(use_val)
print(use_val)

#코드에 해당하는 forecast 의 시작일부터 종료일까지
