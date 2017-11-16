import urllib.request
import re
import pymysql
import datetime
import logging
from bs4 import BeautifulSoup

#
# This program is created to capture key stock index from Yahoo
# Data are stored in a local mariadb/mysqldb, meant to be  used by grafana for data visualization
#
#  Key SQL required
#  create databasee stockdata;
#  create table rawdata (compCode VARCHAR(10), date Date, data mediumblob, primary key (compCode, date));
#  create table stat (compCode varchar(10), date date, field varchar(100), val float, primary key (compCode, date, field));
#  create table meta (compCode varchar(10), compName varchar(255), compIndustry varchar(255), primary key (compCode));
#  create table fielddef (name varchar(10), def varchar(100), primary key (name));
#


# Global Variables
database_server = "localhost"
database_port = 3306
database_login = 'dev'
database_password = ''
database_name = "stockdata"


def YahooDataCollector(comp):
    urlOpener = urllib.request.FancyURLopener({})

    sitecontent = ""

    try:
        urlHandle = urlOpener.open(f"https://finance.yahoo.com/quote/{comp}/key-statistics?p={comp}")
        sitecontent = urlHandle.read()
    except TypeError as e:
        logging.warning("YahooDataColector, Error fetching content from yahoo: %s" % e)

    return sitecontent

def contentParser(sitecontent, dataType):

    soup = BeautifulSoup(sitecontent, 'html.parser')


    siteRecord = re.search(' \| (.+) Stock', soup.title.string)
    if siteRecord:
        compName = siteRecord.group(1)
        if not compName:
            logging.warning("contentParser, error collecting compName : %s" % sitecontent)
            return (0, "" ())
    else:
        logging.warning("contentPraser, no content found")
        return(0, "",())


    res = {}
    for dType in dataType:
        v = soup.findAll(text=dType)
        for val in v:
            if (val):
                res[dType] = val.find_next("td").string
            else :
                res[dType] = ""

    return (1, compName, res)


def runSQL(conn, cur, sql, values):

    try:
        res = cur.execute(sql, values)
    except TypeError as e:
        logging.warning("runSQL, Error running SQL %s with error : %s" % (sql,  e))
        res = None

    return res


def getCompList(conn):
    sql_get_compList = "select compCode from meta"

    cur = conn.cursor()
    res = runSQL(conn, cur, sql_get_compList, ())
    compList = []
    for r in cur:
        compList.append("%s" % r)

    cur.close()
    return compList

def getDatamap(conn):
    sql_get_compList = "select name, def from fielddef"

    # cur = conn.cursor(pymysql.cursors.DictCursor)
    cur = conn.cursor()
    res = runSQL(conn, cur, sql_get_compList, ())

    dataDef = {}
    for row in cur:
        name = ("%s" % row[0]).lower()
        dataDef[name] = "%s" % row[1]
        print("value : %s: %s" % (name, row[1]))
    cur.close()
    return dataDef


def chkMetadata(conn, compCode, compName):
    sql_meta_check = "select compName from meta where compCode=%s and compName != null"
    sql_meta_update = "update meta set compName = %s where compCode = %s"

    cur = conn.cursor()
    res = runSQL(conn, cur, sql_meta_check, compCode)

    if (cur.rowcount > 0):
        compName = "%s" % cur.fetchone()
    else:
        res = runSQL(conn, cur, sql_meta_update, (compName, compCode))

    cur.close()
    return compName

def compStatProcessor(conn, compCode, today, sitecontent, dataMap = {}):

    sql_stat_check = "select 1 from stat where compCode=%s and date=%s"
    sql_stat_update = "insert into stat (compCode, date, field, val) values (%s, %s, %s, %s)"
    sql_stat_get = "select * from stat where compCode=%s and date=%s"


    cur = conn.cursor(pymysql.cursors.DictCursor)
    runSQL(conn, cur, sql_stat_check, (compCode, today))
    res_hash = {}
    compName = ""
    if (cur.rowcount > 0):
        # collect data from cache instead
        runSQL(conn, cur, sql_stat_get, (compCode, today))
        for row in cur:
            fname = dataMap[row["field"]]
            res_hash[fname] = row["val"]
    else:
        (res, compName, res_hash) = contentParser(sitecontent, dataMap.values())
        if res:
            for r in dataMap.keys():
               runSQL(conn, cur, sql_stat_update, (compCode, today, r, res_hash[dataMap[r]]))
        else:
            cur.close()
            return (0, "", ())


    compName = chkMetadata(conn, compCode, compName)

    cur.close()
    return (1, compName, res_hash)


def rawDataCollector(conn, compCode, date, force=0):
    sql_get = "select data from rawdata where compCode=%s and date=%s;"
    sql_set = "insert into rawdata (compCode, date, data) values (%s, %s, %s);"

    cur = conn.cursor()

    res = 0
    if not force:
        runSQL(conn, cur, sql_get, (compCode, date))
        res = cur.rowcount

    rawstr=""
    if (res > 0):
        rawstr = "%s" % cur.fetchone();
    else :
        rawstr = "%s" % YahooDataCollector(compCode)

        if not rawstr == "":
            # check content
            soup = BeautifulSoup(rawstr, 'html.parser')
            siteRecord = re.search(' \| (.+) Stock', soup.title.string)
            if siteRecord:
                runSQL(conn, cur, sql_set, (compCode, date, rawstr))
            else:
                logging.warning("rawDataCollector, Data could be malformed, company name can not be identified")
        else:
            logging.warning ("rawDataCollector, raw record not found and no data from yahoo")

    cur.close()
    return rawstr


def main():
    now = datetime.datetime.now()
    today = "%s-%s-%s" % (now.year, now.month, now.day)
    conn = pymysql.connect(host=database_server, port=database_port, user=database_login, passwd=database_password, db=database_name, autocommit=True)
    compList = getCompList(conn)

    dataMap = getDatamap(conn)

    for compCode in compList:
        rawstr = rawDataCollector(conn, compCode, today)
        if rawstr:
            (res, compName, res_arr) = compStatProcessor(conn, compCode, today, rawstr, dataMap)
            if res:
                print("completd : %s, %s %s" % (compCode, compName, res))
            else:
                logging.warning("main, error processing stat for  %s, %s", (compCode, CompName))
        else:
            logging.warning("main, error parsing result")

    conn.close


if __name__ == '__main__':
    main()



