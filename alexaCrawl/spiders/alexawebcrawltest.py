import re
from urlparse import urlparse
import urllib
import csv
import dns.zone
import dns.ipv4
import os.path
import sys
import math
import multiprocessing
import Queue

from scrapy.selector import Selector
try:
    from scrapy.spider import Spider
except:
    from scrapy.spider import BaseSpider as Spider
from scrapy.utils.response import get_base_url
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor as sle
import sys
from collections import defaultdict
from twisted.internet import reactor
from scrapy import signals
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from alexaCrawl.items import *
from dns.resolver import dns
import dpkt, pcap
import socket, sys
from collections import OrderedDict
from struct import *
from scrapy.exceptions import CloseSpider
from multiprocessing import Process
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from scrapy.crawler import CrawlerProcess
import fileinput

items=[]
itemList=[]
# class CrawlerWorker(multiprocessing.Process):

#     def __init__(self, spider, result_queue):
#         multiprocessing.Process.__init__(self)
#         self.result_queue = result_queue

#         self.crawler = CrawlerProcess(settings)
#         if not hasattr(project, 'crawler'):
#             self.crawler.install()
#         self.crawler.configure()

#         self.items = []
#         self.spider = spider
#         dispatcher.connect(self._item_passed, signals.item_passed)

#     def _item_passed(self, item):
#         self.items.append(item)
 
#     def run(self):
#         self.crawler.crawl(self.spider)
#         self.crawler.start()
#         self.crawler.stop()
#         self.result_queue.put(self.items)


class alexaSpider(CrawlSpider):
    name = "alexa"
    allowed_domains = ["alexa.com"]
    start_urls = [
        #"http://www.alexa.com/",
        "http://www.alexa.com/topsites/",
        #"http://www.alexa.com/topsites/global/Top/",
        
    ]

    rules = [
        Rule(sle(allow=("/topsites/global;0")), callback='parse_category_top', follow=True),
        Rule(sle(allow=("/topsites/global;[0-9]"),restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top', follow=True),
        #Rule(sle(allow=(items),restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top', follow=True),
        #Rule(sle(allow=(itemList),restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top', follow=True),
        #Rule(sle(allow=("/topsites/global;[0-9]"),restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top', follow=True),
        #Rule(sle(allow=("/topsites/countries/AF/"),restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top', follow=True),
        #Rule(LinkExtractor(allow=('/topsites/global;', ), restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top',follow= True),
    ]
    
    global items
    items=[]
    itemList=[]
    global resultFile
    resultFile = open("final.csv",'wbr+')
    # def parse(self,response):
    #     items=[]  
    #     output=[]
    #     #response.url=sle(allow=("/topsites/"))
    #     #print "len(items)++++++++++++++++++++++++++",len(items)

    #     while (len(output)<100):
    #         sel = Selector(response)
    #         sel=allow("/topsites/")
    #         sites = sel.css('.site-listing')
    #         items=self.parse_category_top(sites)
    #         output.append(items)

    #     print "output++++++++++++++++++++++++",output

    #     print "items!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",items
    #     print "check the flow************************************************"

    #     resultFile = open("output3.csv",'wb')
    #     wr = csv.writer(resultFile, dialect='excel')
    #     for item in items:
    #         wr.writerow([item,])

    #     finalItemList=[]
    #     finalItemList=self.parse_sites(items,50)
    #     print "return................................................................"

    # to extract from first page



    #function to retrieve links from alexa 
    def parse_category_top(self, response):
        
        sel = Selector(response)
        sites = sel.css('.site-listing')
        #print "site",sites

        for site in sites:
            item = alexaSiteInfoItem()
            #item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
            item['name']= site.css('a[href*=siteinfo]::text')[0].extract()
            if len(items)>500:
                raise CloseSpider('Item limit exceeded')
            else:
                if item in items:
                    continue
                else :
                    if item['name'] not in items:
                        items.append(item['name'])

        if len(items)<500:
                return
        else :
            wr = csv.writer(resultFile, dialect='excel')
            for item in items:
                wr.writerow([item,])

        #request=self.parse_details(items)
        # for url in items:
        #     if ((url.find("http://") == -1) or (url.find("https://") == -1)):
        #          url="http://"+ url
        #     request=Request(url=url,callback=self.parse_details,dont_filter=True)
        #     request.meta['item'] = item
        #     return request
        # for url in items:
        # for url in open('output22.csv','r').readlines():
        #     url = filter(lambda x: not x.isspace(), url)
        #     print "fileline-------",url
        # splitDomain=[]
        # for url in items:
        #     if ((url.find("http://") == -1) or (url.find("https://") == -1)):
        #         url="http://"+ url
        #     print "value",url
            #request=self.parse_details(url=url)
            #request=Request(url=url,callback=self.parse_details,dont_filter=True)
            #request.meta['item'] = item
            #continue
        
    #     #return request
    # except Exception as e:
    #     #log.msg("Parsing failed for URL {%s}"%format(response.request.url))
    #     raise       

    

    #return Request(items,callback=self.parse_sites)

        if len(items)<500:
            return
        else :
            resultstFile = open("output15.csv",'wb')
            wr = csv.writer(resultFile, dialect='excel')
            for item in items:
                wr.writerow([item,])
            
        for url in items:
            print "iterate"
            if ((url.find("http://") == -1) or (url.find("https://") == -1)):
                url="http://"+ url
            yield Request(url=url,callback=self.parse_details,dont_filter=True)
            #request.meta['item'] = item
        #return request
        # finalItemList=[]
        # finalItemList=self.parse_sites(items,50)

        # print "finalItemList",finalItemList
        

    def parse_details(self,response):
        print("start crawling........................................................")
        print "response",response.url
        
        #for url in response:
        allowed_domains=[]
        splitDomain=[]
        #splitDomain=response.url.split(".com/")
        #response.url=splitDomain[0]+".com"
        #print "response",url

        # url=response.url
        # splitDomain=url.split(".com/")
        # url=splitDomain[0]+".com"

        if response.url.find("http://www") == 0:
            allowed_domains=response.url.split("http://www.")
        elif response.url.find("https://www") == 0:
            allowed_domains=response.url.split("https://www.")
        elif response.url.find("http://") == 0:
            allowed_domains=response.url.split("http://")
        elif response.url.find("https://") == 0:
            allowed_domains=response.url.split("https://")


        # splitDomain=allowed_domains[0].split(".com/")
        # allowed_domains=splitDomain[0]+".com"
        
        # print "allowedDomain=",allowed_domains
        allowed_domains=[allowed_domains[1]]
        # print "allowed_domains=",allowed_domains

        #allowed_domains = [response.url]
        #print "allowed_domains",allowed_domains
        # if response.url.find ("http://") ==-1 or response.url.find ("https://")==-1:
        #      url="http://"+ response.url
        #      start_urls = [url]
        #else:
        start_urls = [response.url]
        print "allowed_domains",allowed_domains
        print "start_urls",start_urls
        #response = HtmlResponse(url=response)
        #response.replace="http://facebook.com"
        #hxs = HtmlXPathSelector(response)
        #print "hxs",hxs
        #sel = Selector(response)
        #sites = sel.css('.site-listing')
        hxs = Selector(response)
        titles = hxs.select("//a/@href").extract()
        #print "results",titles
        #titles = hxs.select("//ul/li").extract()
        #titles=response.selector.xpath("//p")
        #print "titles",titles
        #titles=hxs.select("//a")
        #print "titles",titles
        # print "url inside pasre_details",url
        # value = url.select("a/@href")
        # url = ''.join(value)
        # if url.find ("http://") ==0 or url.find ("https://")==0 or url.find ("www") ==0:
        #         item['name'] =  url
        # print "item==========",item
        # print "items***********************************",items
        # items.append(item)
        splititemp=[]
        for title in titles:
            item1 = alexaSiteInfoItem()
            #value=titles.select("a/@href").extract()
            #value=titles.select("a/@href").extract()
            url = ''.join(title)
            
            if url.find ("http://") ==0 or url.find ("https://")==0:
                item1= url
                #splititemp=item1.split(".com/")
                #splititemp=splititemp[0]+".com"
            else:
                break
            #print "item==========",item
            #print "items***********************************",items
            if item1 not in itemList:
                itemList.append(item1)

        print "items***********************************",itemList

        #resultFile = open("output18.csv",'wb')
        wr = csv.writer(resultFile, dialect='excel')
        for item in itemList:
            wr.writerow([item,])

    def parse_sites(self,urls,nprocs):
        def worker(urls, out_q):
            """ The worker function, invoked in a process. 'urls' is a
                list of numbers to parse. The results are placed in
                a multiple lists that's are finally pushed to a queue.
            """
            outdict = []
            for url in urls:
                item = alexaSiteInfoItem()
                if ((url.find("http://") == -1) or (url.find("https://") == -1)):
                    value="http://"+ url

                # crawler = CrawlerWorker(MySpider(myArgs), result_queue)   
                # crawler.start()
                request=Request(url=value,callback=self.parse_details,dont_filter=True)
                # for item in result_queue.get():
                #      yield item
                return
                #response = HtmlResponse(url=value)
                #outdict =self.parse_details(response)
                #print "outdict********************",outdict 
            #return request
                #outdict =self.parse_details(response)
            #print "outdict********************",outdict
            #out_q.put(outdict)
            

        # Each process will get 'chunksize' nums and a queue to put his out
        # dict into
        #proclist=[]
        #nprocs=50
        out_q = Queue.Queue()
        #print "urls====",urls
        #print "nprocs=",nprocs
        #print "out_q=",out_q
        chunksize = int(math.ceil(len(items) / float(nprocs)))
        procs = []

        for i in range(nprocs):
            p = multiprocessing.Process(
                    target=worker,
                    args=(urls[chunksize * i:chunksize * (i + 1)],
                          out_q))
            procs.append(p)
            p.start()

        # Collect all results into a single result dict. We know how many dicts
        # with results to expect.
        resultdict = {}
        for i in range(nprocs):
            resultdict.update(out_q.get())

        # Wait for all worker processes to finish
        for p in procs:
            p.join()

        return resultdic