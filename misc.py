from _csv import reader
from multiprocessing import cpu_count
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import ThreadedConnectionPool
from config import dbname, dbpasswd, dbuser


def newDBConn():
    conn = psycopg2.connect(database='nova', user='nova', password='emeth', host='172.17.0.1', port=5432)
    conn.autocommit = False
    cur = conn.cursor()
    return conn, cur


def newDBPool():
    return ThreadedConnectionPool(minconn=1, maxconn=cpu_count() * 4, database=dbname, user=dbuser, password=dbpasswd,
                                  host='172.17.0.1', port=5432)


class DBPool:
    def __init__(self):
        self.pool = newDBPool()

    def __del__(self):
        self.pool.closeall()

    def PoolConn(self):
        conn = self.pool.getconn()
        cur = conn.cursor()
        return conn, cur

    def putConn(self, con):
        self.pool.putconn(con)

    def closeDBConn(self, con):
        con.close()


def FloatOrElse(col, default=0.0):
    try:
        return float(col)
    except ValueError:
        return default


def BatchInsert(args):
    data, query = args
    con, cur = newDBConn()
    try:
        execute_batch(cur, query, data, 128)
        con.commit()
    except Exception as e:
        print('shit happens: %s' % str(e))
        print(e)
        con.rollback()
        raise e
    finally:
        cur.close()
        con.close()


def ParseStock(filename):
    f = open(filename, 'r', encoding='gbk')
    csv = list(reader(f))[1:]  # remove header

    # csv = filter(lambda data: data[2] >= '2015-01-01', csv)
    def ParseStockLine(l):
        code = l[0]
        name = l[1]
        date = l[2]
        industry = l[3]
        concept = l[4]
        area = l[5]
        opening = FloatOrElse(l[6])
        highest = FloatOrElse(l[7])
        lowest = FloatOrElse(l[8])
        closing = FloatOrElse(l[9])
        post_recovery = FloatOrElse(l[10])
        pre_recovery = FloatOrElse(l[11])
        quote_change = FloatOrElse(l[12])
        volume = FloatOrElse(l[13])
        turnover = FloatOrElse(l[14])
        hand_turnover_rate = FloatOrElse(l[15])
        circulation_market = FloatOrElse(l[16])
        total_market = FloatOrElse(l[17])
        daily_limit = FloatOrElse(l[18])
        down_limit = FloatOrElse(l[19])
        PE_ratio = FloatOrElse(l[20])
        market_sales = FloatOrElse(l[21])
        market_rate = FloatOrElse(l[22])
        pb_ratio = FloatOrElse(l[23])
        ma5 = FloatOrElse(l[24])
        ma10 = FloatOrElse(l[25])
        ma20 = FloatOrElse(l[26])
        ma30 = FloatOrElse(l[27])
        ma60 = FloatOrElse(l[28])
        ma_cross = l[29]
        macd_diff = FloatOrElse(l[30])
        macd_dea = FloatOrElse(l[31])
        macd_macd = FloatOrElse(l[32])
        macd_cross = l[33]
        k = FloatOrElse(l[34])
        d = FloatOrElse(l[35])
        j = FloatOrElse(l[36])
        kdj_cross = l[37]
        bollinger_mid = FloatOrElse(l[38])
        bollinger_up = FloatOrElse(l[39])
        bollinger_down = FloatOrElse(l[40])
        psy = FloatOrElse(l[41])
        psyma = FloatOrElse(l[42])
        rsi1 = FloatOrElse(l[43])
        rsi2 = FloatOrElse(l[44])
        rsi3 = FloatOrElse(l[45])
        amp = FloatOrElse(l[46])
        volume_ratio = FloatOrElse(l[47])
        res = (
            code, name, date, industry, concept, area, opening, highest, lowest, closing, post_recovery, pre_recovery,
            quote_change, volume, turnover, hand_turnover_rate, circulation_market, total_market, daily_limit,
            down_limit, PE_ratio, market_sales, market_rate, pb_ratio, ma5, ma10, ma20, ma30, ma60, ma_cross,
            macd_diff, macd_dea, macd_macd, macd_cross, k, d, j, kdj_cross, bollinger_mid, bollinger_up,
            bollinger_down, psy, psyma, rsi1, rsi2, rsi3, amp, volume_ratio)
        return res

    data = [ParseStockLine(l) for l in csv]
    query = '''
                INSERT INTO raw_stock VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                 ON CONFLICT ON CONSTRAINT raw_stock_pkey DO NOTHING
                '''
    return data, query


# sh000002,2019-04-30,3197.06,3224.02,3197.06,3234.57,22209343300.0,2.2536541e+11,0.00517236541405
def ParseIndex(filename):
    f = open(filename)
    csv = list(reader(f))[1:]

    def parseline(l):
        code = l[0]
        date = l[1]
        rest = [FloatOrElse(l[i], 0) for i in range(2, len(l))]
        return tuple([code, date] + rest)

    query = '''
    INSERT INTO market_index VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    return [parseline(l) for l in csv], query
