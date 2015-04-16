import re
import csv
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.contrib.linkextractors import LinkExtractor
from alexaCrawl.items import Page

class alexaSpider(Spider):

    name = 'alexa'
    items=[]
    global resultFile
    resultFile = open("final_parse_output.csv",'wbr+')

    """[Author:Som ,last modified:16th April 2015]
    def __init__ :this act as constructor for python
    we pass arguments from core.py and those will be 
    stored in kw."""
    def __init__(self, **kw):
        super(alexaSpider, self).__init__(**kw)
        url = kw.get('url') or kw.get('domain') or 'http://scrapinghub.com/'
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://%s/' % url
        self.url = url
        self.allowed_domains = [re.sub(r'^www\.', '', urlparse(url).hostname)]
        self.link_extractor = LinkExtractor()
        self.cookies_seen = set()

    """[Author:Som ,last modified:15th April 2015]
    start_requests:Overriding method of scrapy.spider.Spider class. 
    This is the method called by Scrapy when the spider is opened 
    for scraping when no particular URLs are specified 
    This method must return an iterable with the first Requests to 
    crawl for this spider."""
    def start_requests(self):
        return [Request(self.url, callback=self.parse, dont_filter=True)]
   
    """[Author:Som ,last modified:16th April 2015]
    def parse:Entry point of scrapy.

    @returns items
    @scrapes url title which will stored in csv file
    """
    def parse(self, response):
        global items
        items=[]
        page = self._get_item(response)
        r = [page]
        r.extend(self._extract_requests(response))

        wr = csv.writer(resultFile, dialect='excel')
        for item in r:
            wr.writerow([item,])
        return r

    """[Author:Som ,last modified:16th April 2015]
    def _get_item:used to crawl items.
    Future requiremnets (items) will be passed here.

    @returns item
    @scrapes title which will stored in csv file
    """
    def _get_item(self, response):
        item = Page(url=response.url)
        self._set_title(item, response)
        return item

    """[Author:Som ,last modified:16th April 2015]
    def _extract_requests:used to crawl urls.

    @returns urls
    """
    def _extract_requests(self, response):
        r = []
        if isinstance(response, HtmlResponse):
            links = self.link_extractor.extract_links(response)
            r.extend(Request(x.url, callback=self.parse) for x in links)
        return r

    def _set_title(self, page, response):
        if isinstance(response, HtmlResponse):
            title = Selector(response).xpath("//title/text()").extract()
            if title:
                page['title'] = title[0]