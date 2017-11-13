import urllib.request
import re
import pymysql
import datetime


from bs4 import BeautifulSoup

database_server = "localhost"
database_port = 3306
database_login = 'dev'
database_password = ''
database_name = "stockdata"
compList = ["0005.HK", "0006.HK", "0009.hk", "0010.hk", "0011.hk", "0012.hk", "0293.hk", "1299.hk", "2318.hk",
            "0700.hk"]
dataMap = {'V1':'Price/Book', 'V2':'Forward Annual Dividend Rate'}




def YahooDataCollector(comp):
    urlOpener = urllib.request.FancyURLopener({})
    urlHandle = urlOpener.open(f"https://finance.yahoo.com/quote/{comp}/key-statistics?p={comp}")
    sitecontent = urlHandle.read()

    return sitecontent

def contentPraser(sitecontent, dataType):

    soup = BeautifulSoup("%s" % sitecontent, 'html.parser')

    siteRecord = re.search(' \| (.+) Stock', soup.title.string)
    compName = siteRecord.group(1)

    if not compName:
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

def compStatProcessor(conn, compCode, today, sitecontent, dataMap):

    sql_meta_check = "select compName from meta where compCode=%s"
    sql_meta_update = "update meta set compName = %s where compCode = %s"

    sql_stat_check = "select 1 from stat where compCode=%s and date=%s"
    sql_stat_update = "insert into stat (compCode, date, V1, V2) values (%s, %s, %s, %s)"

    cursor = conn.cursor()

    (res, compName, res_arr) = contentPraser(sitecontent, dataMap.values)
    if res:
        #update record



def rawDataCollector(conn, compCode, date):
    sql_get = "select data from rawdata where compCode=%s and date=%s;"
    sql_set = "insert into rawdata (compCode, date, data) values (%s, %s, %s);"

    cur = conn.cursor()
    cur.execute(sql_get, (compCode, date))

    rawstr=""
    if (cur.rowcount):
        rawstr = cur.fetchone();
    else :
        rawstr = YahooDataCollector(compCode)

        if not rawstr == "":
            try:
                result = cur.execute(sql_set, (compCode, date, rawstr))
            except TypeError as e:
                print ("error executing : ", e)
            print("record not found, updating %s %s" % (compCode, date))
        else:
            print ("record not found and no data from yahoo")

    cur.close()
    return rawstr


def main():
    now = datetime.datetime.now()
    today = "%s-%s-%s" % (now.year, now.month, now.day)
    conn = pymysql.connect(host=database_server, port=database_port, user=database_login, passwd=database_password, db=database_name, autocommit=True)


    for compCode in compList:
        rawstr = rawDataCollector(conn, compCode, today)
        if res:
            res = compStatProcessor(conn, compCode, today, rawstr, dataMap)
             print(res," : ", res_arr)
        else:
            print("error parsing result")

    conn.close

main()


