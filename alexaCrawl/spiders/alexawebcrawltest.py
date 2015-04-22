import re
import csv
import scrapy.crawler
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.contrib.linkextractors import LinkExtractor
from urlparse import urlparse
from alexaCrawl.items import Page
import dns.resolver
import dns.name
import dns.message
import dns.query
import dns.flags
#from scrapy.stats import stats

class alexaSpider(Spider):

    name = 'alexa'
    items=[]
    global resultFile
    resultFile = open("output6.csv",'wbr+')

    """[Author:Som ,last modified:16th April 2015]
    def __init__ :this act as constructor for python
    we pass arguments from core.py and those will be 
    stored in kw."""
    def __init__(self, **kw):
        super(alexaSpider, self).__init__(**kw)
        url = kw.get('url') or kw.get('domain')
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://%s' % url
        self.url = url
        self.allowed_domains = [re.sub(r'^www\.', '', urlparse(url).hostname)]
        #print "url inside init",url
        self.link_extractor = LinkExtractor()
        self.cookies_seen = set()

    """[Author:Som ,last modified:15th April 2015]
    start_requests:Overriding method of scrapy.spider.Spider class. 
    This is the method called by Scrapy when the spider is opened 
    for scraping when no particular URLs are specified 
    This method must return an iterable with the first Requests to 
    crawl for this spider."""
    def start_requests(self):
        request=Request(self.url,callback=self.parse,dont_filter=True)
        return [request]

    def parse(self,response):
        # global items
        urlList=[]
        r=[]
        # item = Page(url=response.url)
        global resultFile

        page = self._get_item(response)
        r = [page]
        #urlList=[page]
        for pageValue in page:
            urlList.append(page[pageValue])
        r.extend(self._extract_requests(response))

        '''commenting this part to use it later for 
        recurively using links'''
        #wr = csv.writer(resultFile,dialect='excel')
        wr = csv.writer(resultFile, delimiter=',',
                            quotechar=',', quoting=csv.QUOTE_MINIMAL)
        #for item in urlList:
        #wr.writerow([item,])
        wr.writerow(urlList)
        return r

    """[Author:Som ,last modified:16th April 2015]
    def _get_item:used to crawl items.
    Future requiremnets (items) will be passed here.

    @returns item
    @scrapes title which will stored in csv file
    """
    def _get_item(self, response):
        item = Page(url=response.url,content_length=str(len(response.body)),
            response_header=response.headers,response_meta=response.meta,
            response_connection=response.request.headers.get('Connection'))

        self._set_title(item, response)
        self._set_http_header_info(item,response)
        self._set_DNS_info(item,response)
        return item

    """[Author:Som ,last modified:16th April 2015]
    def _extract_requests:used to crawl urls.

    @returns urls
    """
    def _extract_requests(self,response):
        r = []
        if isinstance(response, HtmlResponse):
            links = self.link_extractor.extract_links(response)
            r.extend(Request(x.url, callback=self.parse)for x in links if x.url != response.url)
        return r

    def _set_title(self, page, response):
        #print "title"
        if isinstance(response, HtmlResponse):
            title = Selector(response).xpath("//title/text()").extract()
            if title:
                page['title'] = title[0]

    def _set_http_header_info(self, page, response):
        #print "header"
        if isinstance(response, HtmlResponse):
            responseStatus = response.status
            #print "responseStatus",responseStatus
            if responseStatus:
                page['httpResponseStatus']=responseStatus

    def _set_new_cookies(self, page, response):
        cookies = []
        #print "cookies"
        for cookie in [x.split(';', 1)[0] for x in response.headers.getlist('Set-Cookie')]:
            if cookie not in self.cookies_seen:
                self.cookies_seen.add(cookie)
                cookies.append(cookie)
        if cookies:
            page['newcookies'] = cookies

    """[Author:Som ,last modified:22th April 2015]
        def _set_DNS_info:used to retrieve CNAME chain.
    """
    def _set_DNS_info(self, page,response):
        CNAME=[]
        domain=response.url
        #urlparse :This module defines a standard interface to break URL strings up 
        #in components (addressing scheme, network location, path etc.), to combine
        #the components back into a URL string, and to convert a relative URL to 
        #an absolute URL given a base URL.
        domain=urlparse(domain).netloc
        if domain.startswith('http://'):
            domain=domain.replace("http://","",1)
        elif domain.startswith('https://'):
            domain=domain.replace("https://","",1)

        if domain.endswith('/'):
            domain=domain.replace("/","",1)

        if not domain.startswith('www.'):
            domain = 'www.%s' % domain
        try:
            answers = dns.resolver.query(domain, 'CNAME')
            for rdata in answers:
                try:
                    CNAME.append(rdata)
                    while (rdata.target):
                        value=dns.resolver.query(rdata.target, 'CNAME')
                        for rdata in value:
                            CNAME.append(rdata)
                            #print 'next cname value',value
                except dns.resolver.NXDOMAIN:
                    continue
                except dns.resolver.Timeout:
                    continue
                except dns.exception.DNSException:
                    continue
                except dns.resolver.NoAnswer:
                    continue
            page['CNAMEChain']=CNAME
        except dns.resolver.NXDOMAIN:
            CNAME.append('NONE')
            page['CNAMEChain']=CNAME        
        except dns.resolver.Timeout:
            CNAME.append('NONE')
            page['CNAMEChain']=CNAME
        except dns.exception.DNSException:
            CNAME.append('NONE')
            page['CNAMEChain']=CNAME
        except dns.resolver.NoAnswer:
            CNAME.append('NONE')
            page['CNAMEChain']=CNAME
