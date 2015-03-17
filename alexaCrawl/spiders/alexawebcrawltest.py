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


from alexaCrawl.items import *

class alexaSpider(CrawlSpider):
    name = "alexa"
    allowed_domains = ["alexa.com"]
    start_urls = [
        #"http://www.alexa.com/",
        "http://www.alexa.com/topsites",
    ]
    rules = [
        #Rule(sle(allow=("/topsites;?[0-9]*/")), callback='parse_category_top_xxx', follow=True),
        Rule(sle(allow=("/topsites;?[0-1]*/")), callback='parse_category_top_xxx', follow=True),
    ]

    def parse_category_top_xxx(self, response):
        items = []
        sel = Selector(response)
        #sites = Selector(response)
        sites = sel.css('.site-listing')
        for site in sites:
            item = alexaSiteInfoItem()
            #item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
            item['url'] = site.css('a[href*=siteinfo]::attr(href)')[0].extract()
            item['name'] = site.css('a[href*=siteinfo]::text')[0].extract()
            item['description'] = site.css('.description::text')[0].extract()
            remainder = site.css('.remainder::text')
            if remainder:
                item['description'] += remainder[0].extract()
            # more specific
            item['category'] = response.url.split('/')[-1]
            items.append(item)
        return items

    def read_top_websites(self,response):
        f=open('alexa.csv')
        csv_f=csv.reader(f)
        for row in csv_f:
            print row