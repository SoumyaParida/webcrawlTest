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
from struct import *

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
    
    items = []

    #function to retrieve links from alexa 

    def parse_category_top(self, response):
        items = []
        sel = Selector(response)

        sites = sel.css('.site-listing')
        for site in sites:
            item = alexaSiteInfoItem()
            item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
            item['name'] = site.css('a[href*=siteinfo]::text')[0].extract()
            item['description'] = site.css('.description::text')[0].extract()
            remainder = site.css('.remainder::text')
            if remainder:
                item['description'] += remainder[0].extract()
            # more specific
            item['category'] = response.url.split('/')[-1]
            items.append(item)

            value=item['name']
            print "value=",value
            if ((value.find("http://") == -1) or (value.find("https://") == -1)):
                value="http://"+ value
                print "Send Url with dont filter option************************************",value   
            
            print "Sending request",value
            request=Request(url=value,callback=self.parse_details,dont_filter=True)
            print "Sending requesttttttttttttttttttttttt",request
            request.meta['item'] = item
        return request
        #return items   

    def parse_details(self,response):
        #self.log("Visited %s" % response.url)
        print "response=",response.url
        print "Start Crawling*****************************************************"
        #response.replace="http://wikipedia.com"
        print response.url
        allowedDomain=[]
        global items
        items=[]
        #global items
        #items = []
        if (response.url).find("http://www") == 0:
            allowedDomain=response.url.split("http://www.")
        elif (response.url).find("https://www") == 0:
            allowedDomain=response.url.split("https://www.")
        elif (response.url).find("http://") == 0:
            allowedDomain=response.url.split("http://")
        elif (response.url).find("https://") == 0:
            allowedDomain=response.url.split("https://")
        
    #     print "out of index",allowedDomain[1]
        allowed_domains = [allowedDomain[1]]
    #     print "allowed_domains=",allowed_domains
        start_urls = [response.url]
        print "start_urls=",start_urls 

        hxs = HtmlXPathSelector(response)
        #hxs = Selector(response)
        #titles = hxs.select("//span[@class='pl']")
        titles = hxs.select("//ul/li")
        #titles = hxs.css('.site-listing')
        print "titles=",titles
    #     #sel = Selector(response)
    #     #sites = sel.css('.site-listing')
        
        # print "sites=",titles
        # print "soumyaaaaaaaaaaaaaaaaaaaaaa"
        # for site in titles:
        #     item = alexaSiteInfoItem()
        #     item= site.css('a[href*=siteinfo]::text')[0].extract()
        #     remainder = site.css('.remainder::text')
        #     items.append(item)
        #     print "items=",items

        for titles in titles:
            item = alexaSiteInfoItem()
            #item ["title"] = titles.select("a/text()").extract()
            item['name'] = titles.select("a/@href").extract() 
            print "item==========",item
            items.append(item)

        return items
            #items.append(item)
            #print "items in 2nd iterator=",items

    #     return item
        # for url in items:
        #     for value in url:
        #         print "url=",value
        # #check for webistes crawled from alexa contain http or https.Else it will show schema
        # #error when send it using Request.
        # #some websites already having http or https included while crawling from alexa
        #         if ((value.find("http://") == -1) or (value.find("https://") == -1)):
        #            sendUrl="http://"+ url
        #         print "Send Url with dont filter option*****************************************************",sendUrl
        #         yield Request(url= sendUrl, callback=self.parse_details,dont_filter=True)
        #     #items.append(item)

        
    # def parse_category_top(self, response):
    #     global items
    #     items = []
    #     sel = Selector(response)

    #     sites = sel.css('.site-listing')
    #     for site in sites:
    #         item = alexaSiteInfoItem()
    #         item= site.css('a[href*=siteinfo]::text')[0].extract()
    #         remainder = site.css('.remainder::text')
    #         items.append(item)
    #     return items
        #for url in items:
        # #check for webistes crawled from alexa contain http or https.Else it will show schema
        # #error when send it using Request.
        # #some websites already having http or https included while crawling from alexa
        #     if ((url.find("http://") == -1) or (url.find("https://") == -1)):
        #        sendUrl="http://"+ url
        #     print "Send Url with dont filter option*****************************************************",sendUrl
        #     Request(url= sendUrl, callback=self.parse_details,dont_filter=True)
        #     items.append(item)

        # return items

    
            
        #for count in itemsNew:
         #   print "count=",count
          #  if ((count != "http://") or (itemsNew[count] != "http://")):
           #     itemsNew.remove(itemsNew[count])
        #return itemsNew