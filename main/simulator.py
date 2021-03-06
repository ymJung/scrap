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
    cursor.execute("select name, code from daily_stock group by code")
    return cursor.fetchall()


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
        if compare > 0 and result is True:
            result = False
    return result


def forecast_result(code, name):
    foreacast_rate = 100
    datas = get_potential_datas(0.70, code)
    for data in datas:
        if is_compare_chain_minus(code=code, analyze_at=data.get('analyzeAt'), day_cnt=1):
            percent = data.get('percent')
            foreacast_rate = foreacast_rate + (foreacast_rate * percent)
    return code, name, round(foreacast_rate,  1)

def get_code(param):
    param = param.strip()
    cursor = connection.cursor()
    cursor.execute("SELECT distinct(code), name FROM data.daily_stock WHERE name = %s", (param))
    result = cursor.fetchone()
    if result is not None:
        return result.get('code'), result.get('name')
    cursor.execute("SELECT distinct(code), name FROM data.daily_stock WHERE code = %s", (param))
    result = cursor.fetchone()
    if result is not None:
        return result.get('code'), result.get('name')
    return None


def simulator(code):
    code, name = get_code(code)
    if code is None:
        return None
    code, name, forecast_val = forecast_result(code, name)
    return '[' + code + '][' + name + '] [' + str(forecast_val) + ']'


def get_max_target_at():
    cursor = connection.cursor()
    cursor.execute("select max(evaluate) as evaluateMax from data.forecast")
    evaluateMax = cursor.fetchone().get('evaluateMax')
    cursor.execute("select analyzeAt from data.forecast group by analyzeAt order by analyzeAt desc limit %s",
                   (evaluateMax))
    results = cursor.fetchall()
    if len(results) >= evaluateMax:
        return results[evaluateMax - 1].get('analyzeAt')
    return datetime.date.today()


def get_potential_data_results(target_at, limit_rate):
    cursor = connection.cursor()
    query = "SELECT ds.name, f.type, f.code, f.analyzeAt, f.potential, f.volume , f.percent, f.evaluate FROM data.forecast f, data.daily_stock ds " \
            "WHERE f.type = 3 AND ds.code = f.code AND analyzeAt > %s and potential > %s group by f.id ORDER BY f.analyzeAt, f.code ASC"
    cursor.execute(query, (target_at, str(limit_rate)))
    return cursor.fetchall()


def get_potential(target_at, chan_minus, limit_rate):
    datas = get_potential_data_results(target_at, limit_rate)
    potens = list()
    for data in datas:
        compare = is_compare_chain_minus(data.get('code'), data.get('analyzeAt'), chan_minus)
        if compare:
            potens.append(data)
    return potens

def print_potentials(datas):
    msg = ''
    for data in datas:
        result = simulator(data.get('code'))
        msg += (data.get('analyzeAt').strftime("%Y-%m-%d")
                + ' [' + str(data.get('evaluate'))
                + '] [' + str(data.get('percent'))
                + '] ' + result
                + '] [' + str(data.get('type'))
                + '] [' + str(data.get('potential'))
                + '] [' + str(data.get('volume'))

                + ']\n')
    print(msg)
    return msg


import datetime


datas = get_potential(target_at=get_max_target_at() - datetime.timedelta(days=1), chan_minus=1, limit_rate=0.70)
print_potentials(datas)
