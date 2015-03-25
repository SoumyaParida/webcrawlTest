import re
import json
from urlparse import urlparse
import urllib
import csv


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
import dpkt, pcap
import socket, sys
from struct import *

class CloseSpider(object):
    def __init__(self, crawler):
        self.crawler = crawler
        self.close_on = {
                            'timeout': crawler.settings.getfloat('CLOSESPIDER_TIMEOUT'),
                            'itemcount': crawler.settings.getint('CLOSESPIDER_ITEMCOUNT'),
                            'pagecount': crawler.settings.getint('CLOSESPIDER_PAGECOUNT'),
                            'errorcount': crawler.settings.getint('CLOSESPIDER_ERRORCOUNT'),
                        }
        self.counter = defaultdict(int)
        if self.close_on.get('errorcount'):
            crawler.signals.connect(self.error_count, signal=signals.spider_error)
        if self.close_on.get('pagecount'):
            crawler.signals.connect(self.page_count, signal=signals.response_received)
        if self.close_on.get('timeout'):
            crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        if self.close_on.get('itemcount'):
            crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)
            crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)
        @classmethod
        def from_crawler(cls, crawler):
            return cls(crawler)

        def error_count(self, failure, response, spider):
            self.counter['errorcount'] += 1
            if self.counter['errorcount'] == self.close_on['errorcount']:
                self.crawler.engine.close_spider(spider, 'closespider_errorcount')
        def page_count(self, response, request, spider):
            self.counter['pagecount'] += 1
            if self.counter['pagecount'] == self.close_on['pagecount']:
                self.crawler.engine.close_spider(spider, 'closespider_pagecount')
        def spider_opened(self, spider):
            self.task = reactor.callLater(self.close_on['timeout'], self.crawler.engine.close_spider, spider, reason='closespider_timeout')
        def item_scraped(self, item, spider):
            self.counter['itemcount'] += 1
            if self.counter['itemcount'] == self.close_on['itemcount']:
                self.crawler.engine.close_spider(spider, 'closespider_itemcount')
        def spider_closed(self, spider):
            task = getattr(self, 'task', False)
            if task and task.active():
                task.cancel()


class alexaSpider(CrawlSpider):
    name = "alexa"
    allowed_domains = ["alexa.com"]
    start_urls = [
        "http://www.alexa.com/",
        "http://www.alexa.com/topsites",
        #"http://www.alexa.com"    
    ]
    rules = [
        Rule(sle(allow=("/topsites;?[0-9]*/")), callback='parse_category_top', follow=True),
        #Rule(sle(allow=("/topsites;2/")), callback='parse_category_top_xxx', follow=True),
    ]

    itemsNew = []
    items= []

    def __init__(self, name=None, **kwargs): 
        super(alexaSpider, self).__init__(name, **kwargs)
        self.items_buffer = {}
        self.base_url = "http://alexa.com"
        from scrapy.conf import settings
        settings.overrides['DOWNLOAD_TIMEOUT'] = 360 ## prevent too early timeout 
        
    #function to retrieve links from alexa    
    def parse_category_top(self, response):
        global items
        items = []
        sel = Selector(response)
        sites = sel.css('.site-listing')
        #print sites
        for site in sites:
            print "inside sites loop+++++++++++++++++++++++++++++++++++++++++"
            item = alexaSiteInfoItem()
            item= site.css('a[href*=siteinfo]::text')[0].extract()
            #item['name'] = site.css('a[href]text')[0].extract()
            #item['description'] = site.css('.description::text')[0].extract()
            remainder = site.css('.remainder::text')
            #if remainder:
             #   item['description'] += remainder[0].extract()  
            # more specific
            #item['category'] = response.url.split('/')[-1]
            
            if len(items)< 10 :
                items.append(item)
                print items
            else :
                break
                #sys.exit("SHUT DOWN EVERYTHING!")
            print len(items),'items count'

        for url in items:
        #check for webistes crawled from alexa contain http or https.Else it will show schema
        #error when send it using Request.
        #some websites already having http or https included while crawling from alexa
            if url.find("http://") == -1 or url.find("https://") == -1:
                sendUrl="http://"+ url
                print "Send Url##################", sendUrl
            print "Send Url with dont filter option*****************************************************"
            return Request(url= sendUrl, callback=self.parse_details,dont_filter=True)
            #print "Go for next website*****************************************************"
            #request.meta['item'] = itemsNew
        #return request    
            #request = scrapy.Request("http://www.wikipedia.com",callback=self.parse_details)
            #request.meta['item'] = item
        #return request
            #yield Request(url= sendUrl, callback=self.parse_details )
        #raise CloseSpider(reason='API usage exceeded')d
        #return items

    def parse_details(self,response):
        #self.log("Visited %s" % response.url)
        print "Start Crawling*****************************************************"
        #response.replace="http://wikipedia.com"
        print response.url
        allowedDomain=[]
        itemsNew=[]
        global items
        items = []
        if (response.url).find("http://www") == 0:
            allowedDomain=response.url.split("http://www.")
        elif (response.url).find("http://") == 0:
            allowedDomain=response.url.split("http://")
        
        allowed_domains = [allowedDomain[1]]
        print "allowed_domains=",allowed_domains
        start_urls = [response.url]
        print "start_urls=",start_urls 

        #hxs = HtmlXPathSelector(response) 
        #l_venue = alexaSiteInfoItem()

        #pc = pcap.pcap()
        #pc.setfilter('icmp')
        #for ts, pkt in pc:
         #   print dpkt.ethernet.Ethernet(pkt)


        hxs = HtmlXPathSelector(response)
        #titles = hxs.select("//span[@class='pl']")
        titles = hxs.select("//ul/li")
        
        #print "titles=",title
        
        for titles in titles:
          item = alexaSiteInfoItem()
          item ["title"] = titles.select("a/text()").extract()
          item ["link"] = titles.select("a/@href").extract()
          items.append(item)
          print "itemsNew=",items
        #s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
            # receive a packet
        #while True:
         #   packet = s.recvfrom(65565)
             
            #packet string from tuple
          #  packet = packet[0]
             
            #take first 20 characters for the ip header
           # ip_header = packet[0:20]
             
            #now unpack them :)
            #iph = unpack('!BBHHHBBH4s4s' , ip_header)
             
            #version_ihl = iph[0]
            #version = version_ihl >> 4
            #ihl = version_ihl & 0xF
             
            #iph_length = ihl * 4
             
            #ttl = iph[5]
            #protocol = iph[6]
            #s_addr = socket.inet_ntoa(iph[8]);
            #d_addr = socket.inet_ntoa(iph[9]);
             
            #print 'Version : ' + str(version) + ' IP Header Length : ' + str(ihl) + ' TTL : ' + str(ttl) + ' Protocol : ' + str(protocol) + ' Source Address : ' + str(s_addr) + ' Destination Address : ' + str(d_addr)
             
            #tcp_header = packet[iph_length:iph_length+20]
             
            #now unpack them :)
            #tcph = unpack('!HHLLBBHHH' , tcp_header)
             
            #source_port = tcph[0]
            #dest_port = tcph[1]
            #sequence = tcph[2]
            #acknowledgement = tcph[3]
            #doff_reserved = tcph[4]
            #tcph_length = doff_reserved >> 4
             
            #print 'Source Port : ' + str(source_port) + ' Dest Port : ' + str(dest_port) + ' Sequence Number : ' + str(sequence) + ' Acknowledgement : ' + str(acknowledgement) + ' TCP header length : ' + str(tcph_length)
             
            #h_size = iph_length + tcph_length * 4
            #data_size = len(packet) - h_size
             
            #get data from the packet
            #data = packet[h_size:]
             
            #print 'Data : ' + data

        print "complete Crawling*****************************************************"
        return itemsNew



    #def read_top_websites(self,response):
     #   f=open('alexa.csv')
      #  csv_f=csv.reader(f)
       # for row in csv_f:
        #    print row