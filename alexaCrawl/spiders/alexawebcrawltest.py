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
import httplib2
from multiprocessing import Lock, Process, Queue, current_process

items=[]
itemList=[]
Final_items=[]

class alexaSpider(CrawlSpider):
    name = "alexa"
    allowed_domains = ["alexa.com"]
    start_urls = [
        "http://www.alexa.com/topsites/",    
    ]

    rules = [
        Rule(sle(allow=("/topsites/global;0")), callback='parse_category_top', follow=True),
        Rule(sle(allow=("/topsites/global;[0-9]"),restrict_xpaths=('//a[@class="next"]', )), callback='parse_category_top', follow=True),
    ]
    
    global items,Final_items
    items=[]
    itemList=[]
    Final_items=[]
    global resultFile
    resultFile = open("final.csv",'wbr+')

    #function to retrieve links from alexa 
    # def parse_category_top(self, response):
        
    #     sel = Selector(response)
    #     sites = sel.css('.site-listing')
    #     #print "site",sites

    #     for site in sites:
    #         item = alexaSiteInfoItem()
    #         #item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
    #         item['name']= site.css('a[href*=siteinfo]::text')[0].extract()
    #         if len(items)>500:
    #             raise CloseSpider('Item limit exceeded')
    #         else:
    #             if item in items:
    #                 continue
    #             else :
    #                 if item['name'] not in items:
    #                     items.append(item['name'])

    #     print "return1................................................................"

    #     if len(items)<500:
    #             return
    #     else :
    #         wr = csv.writer(resultFile, dialect='excel')
    #         for item in items:
    #             wr.writerow([item,])

    #     # finalItemList=[]
    #     # print "return2................................................................"
    #     # finalItemList=self.parse_sites(items)
    #     # print "finalItemList",finalItemList
    #     # print "return................................................................"
            
    #     for url in items:
    #         print "iterate"
    #         if ((url.find("http://") == -1) or (url.find("https://") == -1)):
    #             url="http://"+ url
    #         yield Request(url=url,callback=self.parse_details,dont_filter=True)

    def parse(self,response):
        # FileToRead = open("top-1m.csv",'r+')
        # rowValues=[]
        # reader = csv.DictReader(FileToRead)
        # for row in reader:
        #     # Values=row
        #     # rowValues=row.split(',')
        #     print "rowValues",row
        rowValues=[]
        finalItemList=[]
        listOfLists=[]

        row_no=1
        code_chunk=1
        listOfLists=[[] for _ in range(4)]
        #listOfLists.append([])
        with open('top-1m.csv') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
            for row in spamreader:
                rowValue=', '.join(row)
                rowValues=rowValue.split(",")
               # if len(listOfLists)<10:
                if (code_chunk==row_no):
                    #finalItemList.append(rowValues[1])
                    
                    listOfLists[row_no-1].append(rowValues[1])             
                    if (row_no==4):
                        row_no=row_no%4
                        #row_no=row_no-1
                    row_no=row_no+1
                    code_chunk=(row_no%20)
                    # if row_no==20:
                    #     code_chunk=20
                    # else:    
                    #     code_chunk=(row_no%20)
                    #     continue
                        #print "code_chunk",code_chunk    
                else:
                    break

        
        # 
        #     print "eachList",eachList
        #     finalItemList=[]
        #     finalItemList=self.parse_sites(eachList,4)

        # print "finalItemList",finalItemLis
        for eachList in listOfLists:
            for url in eachList:
                print "iterate"
                if ((url.find("http://") == -1) or (url.find("https://") == -1)):
                    url="http://"+ url
                yield Request(url=url,callback=self.parse_details,dont_filter=True)
            
        #return status    
                #yield Request(url=url,callback=self.parse_details,dont_filter=True)

                # if (len(finalItemList)<50):
                #     #if row
                #     finalItemList.append(rowValues[1])
                #     print "rowValue",rowValues[1]
                # else:
                #     break

            
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

        splititemp=[]
        for title in titles:
            item1 = alexaSiteInfoItem()
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

    # def parse_sites(self,urls,nprocs):
    #     def worker(urls, out_q):
    #         """ The worker function, invoked in a process. 'urls' is a
    #             list of numbers to parse. The results are placed in
    #             a multiple lists that's are finally pushed to a queue.
    #         """
    #         outdict = []
    #         print "soumya"
    #         for url in urls:
    #             print "url***********8",url
    #             #item = alexaSiteInfoItem()
    #             if ((url.find("http://") == -1) or (url.find("https://") == -1)):
    #                 url="http://"+ url
                
    #             yield Request(url=url,callback=self.parse_details,dont_filter=True)

    #             # crawler = CrawlerWorker(MySpider(myArgs), result_queue)   
    #             # crawler.start()
    #             #request=Request(url=value,callback=self.parse_details,dont_filter=True)
    #             # for item in result_queue.get():
    #             #      yield item
    #             #return
    #             #response = HtmlResponse(url=value)
    #             #outdict =self.parse_details(response)
    #             #print "outdict********************",outdict 
    #         #return request
    #             #outdict =self.parse_details(response)
    #         #print "outdict********************",outdict
    #         #out_q.put(outdict)
            

    #     # Each process will get 'chunksize' nums and a queue to put his out
    #     # dict into
    #     #proclist=[]
    #     #nprocs=50
    #     out_q = multiprocessing.Queue()
    #     chunksize = int(math.ceil(len(urls) / float(nprocs)))
    #     procs = []

    #     for i in range(nprocs):
    #         p = multiprocessing.Process(
    #                 target=worker,
    #                 args=(urls,out_q))
    #         procs.append(p)
    #         p.start()

    #     # Collect all results into a single result dict. We know how many dicts
    #     # with results to expect.
    #     resultdict = {}
    #     for i in range(nprocs):
    #         resultdict.update(out_q.get())

    #     # Wait for all worker processes to finish
    #     for p in procs:
    #         p.join()

    #     return resultdic        






















    # def parse_sites(self,urls):
    #     def worker(work_queue, done_queue):
    #         try:
    #             print "soumya"
    #             for url1 in work_queue:
    #                 print "url1",url1
    #                 print "worker",work_queue.get
    #                 request=Request(url=url,callback=self.parse_details,dont_filter=True)
    #                 request.meta['item'] = item
    #                 #status_code = print_site_status(url1)
    #              #   print "status_code",status_code
    #               #  done_queue.put("%s - %s got %s." % (current_process().name, url1, status_code))
    #                 return
    #         except Exception, e:
    #             #done_queue.put("%s failed on %s with: %s" % (current_process().name, url1, e.message))
    #             return True

    #     def print_site_status(url2):
    #         http = httplib2.Http(timeout=10)
    #         headers, content = http.request(url2)
    #         return headers.get('status', 'no response')

    
    #     workers = 2
    #     work_queue = Queue()
    #     done_queue = Queue()
    #     processes = []

    #     for url in urls:
    #         if ((url.find("http://") == -1) or (url.find("https://") == -1)):
    #             url="http://"+ url
    #         work_queue.put(url)
    #         print "url",url
    #         print "work_queue",work_queue.get()

    #     for w in xrange(workers):
    #         p = multiprocessing.Process(target=worker, args=(work_queue, done_queue))
    #         p.start()
    #         processes.append(p)
    #         work_queue.put('STOP')

    #     for p in processes:
    #         p.join()

    #     done_queue.put('STOP')

    #     for status in iter(done_queue.get, 'STOP'):
    #         print status
    #     return status

