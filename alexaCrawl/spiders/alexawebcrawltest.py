import re
import json
from urlparse import urlparse
import urllib
import csv
import dns.zone
import dns.ipv4
import os.path
import sys

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

items=[]
class alexaSpider(CrawlSpider):
    name = "alexa"
    allowed_domains = ["alexa.com"]
    start_urls = [
        "http://www.alexa.com/",
         "http://www.alexa.com/topsites",
    ]

    rules = [
        Rule(sle(allow=("/topsites/")), callback='parse_category_top', follow=True),
    ]
    
    global items
    items=[]

    #function to retrieve links from alexa 
    def parse_category_top(self, response):
        print "***********************************************"
        sel = Selector(response)
        sites = sel.css('.site-listing')

        for site in sites:
            item = alexaSiteInfoItem()
            #item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
            item['name']= site.css('a[href*=siteinfo]::text')[0].extract()
            print item['name']
            print "len(items)********************************************888",len(items)
            if len(items)>10000:
                raise CloseSpider('Item limit exceeded')
            else:
                if item in items:
                    continue
                else :
                    if item['name'] not in items:
                        items.append(item['name'])
        
        resultFile = open("output2.csv",'wb')
        wr = csv.writer(resultFile, dialect='excel')
        for item in items:
            wr.writerow([item,])

        finalItemList=[]
        finalItemList=self.parse_sites(items,50)
        return

    def parse_details(url):
        print("start crawling........................................................")
        allowedDomain=[]
        if (response.url).find("http://www") == 0:
            allowedDomain=response.url.split("http://www.")
        elif (response.url).find("https://www") == 0:
            allowedDomain=response.url.split("https://www.")
        elif (response.url).find("http://") == 0:
            allowedDomain=response.url.split("http://")
        elif (response.url).find("https://") == 0:
            allowedDomain=response.url.split("https://")
        
        allowed_domains = [allowedDomain[1]]
        start_urls = [response.url]
        hxs = HtmlXPathSelector(response)
        titles = hxs.select("//ul/li")
       
        for titles in titles:
            item = alexaSiteInfoItem()
            value=titles.select("a/@href").extract()
            url = ''.join(value)
            print "url=========",url
            if url.find ("http://") ==0 or url.find ("https://")==0 or url.find ("www") ==0:
                item['name'] =  url
            print "item==========",item
            print "items***********************************",items
            items.append(item)

        return items

    def parse_sites(urls, nprocs):
        def worker(urls, out_q):
            """ The worker function, invoked in a process. 'urls' is a
                list of numbers to parse. The results are placed in
                a multiple lists that's are finally pushed to a queue.
            """
            outdict = {}
            for url in urls:
                outdict[url] = parse_details(url)
            out_q.put(outdict)

        # Each process will get 'chunksize' nums and a queue to put his out
        # dict into
        out_q = Queue()
        chunksize = int(math.ceil(len(items) / float(nprocs)))
        procs = []

        for i in range(nprocs):
            p = multiprocessing.Process(
                    target=worker,
                    args=(nums[chunksize * i:chunksize * (i + 1)],
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