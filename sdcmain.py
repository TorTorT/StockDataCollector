import urllib.request
import re

from bs4 import BeautifulSoup


def main():
    compList  = ["0005.HK","0006.HK","0009.hk","0010.hk","0011.hk","0012.hk","0293.hk","1299.hk","2318.hk","0700.hk"]
    dataType = ['Price/Book', 'Forward Annual Dividend Rate']

    for comp in compList:
        urlOpener = urllib.request.FancyURLopener({})
        urlHandle = urlOpener.open(f"https://finance.yahoo.com/quote/{comp}/key-statistics?p={comp}")
        sitecontent = urlHandle.read()

        soup = BeautifulSoup(sitecontent, 'html.parser')
#        dataType = 'Forward Annual Dividend Rate'

        siteRecord = re.search(' \| (.+) Stock', soup.title.string)
        if not siteRecord:
            print (comp, " not found")
            continue

        compName = siteRecord.group(1)

        for dType in dataType:
            v = soup.findAll(text=dType)
            for val in v:
                if (val):
                     print (comp, " ", compName, " ", dType, " : ", val.find_next("td").string)
                else :
                    print (comp, " ", compName, " ", dType, " : Not found")

    print ("completed")


main()


