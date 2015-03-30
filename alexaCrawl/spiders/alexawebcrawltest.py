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
    ]
    rules = [
        Rule(sle(allow=("/topsites;?[0-9]*/")), callback='parse_category_top', follow=True),
    ]
    #function to retrieve links from alexa 
    def parse_category_top(self, response):
        global items
        items = []
        sel = Selector(response)

        sites = sel.css('.site-listing')
        for site in sites:
            item = alexaSiteInfoItem()
            #item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
            item['name'] = site.css('a[href*=siteinfo]::text')[0].extract()
            #item['description'] = site.css('.description::text')[0].extract()
            remainder = site.css('.remainder::text')
            #if remainder:
             #   item['description'] += remainder[0].extract()
            # more specific
            #item['category'] = response.url.split('/')[-1]
            items.append(item)
            value=item['name']
            if ((value.find("http://") == -1) or (value.find("https://") == -1)):
                value="http://"+ value
            print "sending value"
            request=Request(url=value,callback=self.parse_details,dont_filter=True)
            print "receiving value"
            request.meta['item'] = item
        return request

    def parse_details(self,response):
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