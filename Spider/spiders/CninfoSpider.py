import logging
from scrapy import Spider
from scrapy_splash import SplashRequest

from Spider.items import *

from py2neo import *


class ChinfoSpider(Spider):
    name = "CninfoSpider"
    allowed_domains = ["www.cninfo.com.cn"]
    start_urls = \
        [
            "http://www.cninfo.com.cn/cninfo-new/information/companylist"
            ]
    headerCount = 6

    luaShareholder = """
         function main(splash) 
            assert(splash:go(splash.args.url)) 
            splash:wait(5) 
            assert(splash:runjs( 'document.getElementById("shareholders").firstChild.click()'))
            splash:wait(1) 
            return splash:html() 
         end 
    """
    luaSubject = """
             function main(splash) 
                assert(splash:go(splash.args.url))
                splash:wait(5) 
                assert(splash:runjs('document.getElementById("brief").firstChild.click()'))
                splash:wait(1) 
                return splash:html() 
             end 
    """

    luaSenior = """
             function main(splash) 
                assert(splash:go(splash.args.url)) 
                splash:wait(5) 
                assert(splash:runjs('document.getElementById("management").firstChild.click()'))
                splash:wait(1) 
                return splash:html() 
             end 
    """

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url=url
                , callback=self.parse
                , endpoint="render.html"
                , args={'wait': 1}
                )

    def parse(self, response):
        logging.debug(msg="Start Parse!")
        categoryNodeList = response.xpath("//ul[@class='ct-menu']//a")
        index = 1
        for categoryNode in categoryNodeList:
            if index < 5:
                category = categoryNode.xpath("text()").extract()[0].strip()
                categoryItem = CategoryItem()
                categoryItem["name"] = category
                yield categoryItem
                stockNodeList = response.xpath(
                    "//div[@class='list-ct']/div[@id='con-a-" + str(index) + "']//a[@href]")

                for stockNode in stockNodeList:
                    stock = stockNode.xpath("text()").extract()[0].strip()
                    tmpStrs = stock.partition(' ')
                    stockCode = tmpStrs[0]
                    stockName = tmpStrs[2]
                    stockItem = StockItem()
                    stockItem["category"] = category
                    stockItem["name"] = stockName
                    stockItem["code"] = stockCode
                    yield stockItem

                    link = stockNode.xpath("@href").extract()[0]

                    # Subject Parser
                    meta = {
                        "data": {
                            "category": category
                            , "stock": stockName
                            }
                        , "parser": self.parseSubject
                        , "nextUrl": link
                        , "level": 0
                        }
                    yield SplashRequest(
                        url=link
                        , meta=meta
                        , args={
                            'lua_source': self.luaSubject
                            }
                        , endpoint='execute'
                        , callback=self.iFramePretreat
                        )
            else:
                pass
            index += 1

    def iFramePretreat(self, response):
        link = response.url
        index = link.rfind("/")
        link = link[0:index]
        # target = response.xpath("//div[@class='layout1']/iframe[@id='i_nr']/@src").extract()[0]
        target = response.xpath("//*[@id='i_nr']/@src").extract()[0]
        link = link + "/" + target

        level = response.meta.get('level') if not response.meta.get('level') is None else 0
        # priority = response.meta.get('priority') if not response.meta.get('priority') is None else 0

        meta = {
            "data": response.meta['data']
            , "nextUrl": response.meta['nextUrl']
            , "level": level
            , "parser": response.meta['parser']
            }

        if level > 0:
            meta['level'] = level - 1
            yield SplashRequest(
                link
                , meta=meta
                , args={'wait': 1}
                , callback=self.iFramePretreat
                # , priority=priority
                )
        else:
            yield SplashRequest(
                url=link
                , meta=meta
                , args={
                    'wait': 1
                    }
                , callback=response.meta["parser"]
                # , priority=priority
                )

    def parseSubject(self, response):
        logging.debug("Start ParseDetail")
        data = response.meta['data']
        stock = data['stock']
        subjectNodeList = response.xpath("//div[@class='zx_left']/div[@class='clear']//tr")
        subjectItem = SubjectItem()
        subjectItem["stock"] = stock
        subjectItem['name'] = subjectNodeList[0].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['address'] = subjectNodeList[2].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['legalRepresentative'] = subjectNodeList[4].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['regCapital'] = subjectNodeList[6].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['postCode'] = subjectNodeList[8].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['phoneNumber'] = subjectNodeList[9].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['webSite'] = subjectNodeList[11].xpath("td[2]/text()").extract()[0].strip()
        subjectItem['time2Market'] = subjectNodeList[12].xpath("td[2]/text()").extract()[0].strip()
        yield subjectItem

        data['subject'] = subjectItem['name']
        meta = {
            "data": data
            , "nextUrl": response.meta['nextUrl']
            , "level": 0
            , "parser": self.parseSenior
            }

        yield SplashRequest(url=meta["nextUrl"]
                            , meta=meta
                            , args={'lua_source': self.luaSenior}
                            , endpoint='execute'
                            , callback=self.iFramePretreat
                            )

        meta = {
            "data": data
            , "nextUrl": response.meta['nextUrl']
            , "level": 1
            , "parser": self.parseShareholder
            }
        yield SplashRequest(url=meta["nextUrl"]
                            , meta=meta
                            , args={'lua_source': self.luaShareholder}
                            , endpoint='execute'
                            , callback=self.iFramePretreat
                            )

    def parseSenior(self, response):
        logging.debug("Start ParseSenior")
        data = response.meta['data']
        subject = data['subject']
        seniorNodeList = response.xpath("//div[@class='zx_left']/div[@class='clear']//tr")
        index = 0
        for seniorNode in seniorNodeList:
            if index > 0:
                seniorItem = SeniorItem()
                colNode = seniorNode.xpath("td")
                seniorItem['subject'] = subject
                seniorItem['name'] = colNode[0].xpath("text()").extract()[0].strip()
                seniorItem['job'] = colNode[1].xpath("text()").extract()[0].strip()
                seniorItem['birthday'] = colNode[2].xpath("text()").extract()[0].strip()
                seniorItem['sex'] = colNode[3].xpath("text()").extract()[0].strip()
                seniorItem['education'] = colNode[4].xpath("text()").extract()[0].strip()
                yield seniorItem
            else:
                pass
            index += 1
        pass

    def parseShareholder(self, response):
        logging.debug("Start ParseDetail")
        data = response.meta['data']
        subject = data['subject']
        shareholderNodeList = response.xpath("//div[@class='zx_left']/div[@class='clear']//tr")
        index = 0
        for shareholderNode in shareholderNodeList:
            if index > 1:
                shareholderItem = ShareholderItem()
                shareholderItem['subject'] = subject
                colNode = shareholderNode.xpath("td")
                shareholderItem['shareName'] = colNode[0].xpath("text()").extract()[0].strip()
                shareholderItem['shareQuantity'] = colNode[1].xpath("text()").extract()[0].strip()
                shareholderItem['shareRatio'] = colNode[2].xpath("text()").extract()[0].strip()
                shareholderItem['shareNature'] = colNode[3].xpath("text()").extract()[0].strip()
                yield shareholderItem
            elif index == 1:
                shareholderItem = ShareholderItem()
                shareholderItem['subject'] = subject
                colNode = shareholderNode.xpath("td")
                shareholderItem['shareName'] = colNode[1].xpath("text()").extract()[0].strip()
                shareholderItem['shareQuantity'] = colNode[2].xpath("text()").extract()[0].strip()
                shareholderItem['shareRatio'] = colNode[3].xpath("text()").extract()[0].strip()
                shareholderItem['shareNature'] = colNode[4].xpath("text()").extract()[0].strip()
                yield shareholderItem
            else:
                pass
            index += 1
        pass
