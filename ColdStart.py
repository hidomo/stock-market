'''
start from cold data
you need overview-sh.zip and overview-sz.zip to data/cold
and unzip them
you will find
data/
    cold/
        index/
            sh/sz-indices.csv
        sh-codes.csv
        sz-codes.csv
'''
import os
from misc import BatchInsert, ParseStock, ParseIndex
from multiprocessing import Pool


def ParseStocks(filelists):
    tp = Pool(16)
    try:
        data = tp.map(ParseStock, filelists)
        tp.map(BatchInsert, data)
    except Exception as e:
        print(e)
        raise e
    finally:
        tp.terminate()


def ParseIndices(filelists):
    tp = Pool(16)
    try:
        data = tp.map(ParseIndex, filelists)
        tp.map(BatchInsert,data)
    except Exception as e:
        print(e)
        raise e
    finally:
        tp.terminate()


if __name__ == '__main__':
    for a, b, c in os.walk('data/cold'):
        filelists = map(lambda filename: 'data/cold/%s' % filename, filter(lambda x: 'zip' not in x, c))
        if len(b) == 1:  # stock
            filelists = map(lambda filename: 'data/cold/%s' % filename, filter(lambda x: 'zip' not in x, c))
            ParseStocks(filelists)
        else:
            filelists = map(lambda filename: 'data/cold/index/%s' % filename, filter(lambda x: 'zip' not in x, c))
            ParseIndices(filelists)
