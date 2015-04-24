import re
import csv
import scrapy.crawler
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.contrib.linkextractors import LinkExtractor
from sets import Set
from urlparse import urlparse
from alexaCrawl.items import Page
import dns.resolver
import dns.name
import dns.message
import dns.query
import dns.flags
import codecs
import sys
import random
#from scrapy.stats import stats

class alexaSpider(Spider):

    name = 'alexa'
    items=[]
    global resultFile
    
    #resultFile = codecs.open("output6.csv",mode='wb',encoding='utf-8')
    resultFile = codecs.open("output6.csv",'wbr+')

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
        #index=random.randint(1, 20)
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
        #nbytes = {'utf-32':4}.get(encoding, 1) 
        wr = csv.writer(resultFile, delimiter=',',
                            quotechar=' ', quoting=csv.QUOTE_MINIMAL)
        # for item in urlList:
        #     if isinstance(item, unicode):
        #         item = item
        #     elif isinstance( item, ( int, long ) ):
        #         item=item
        #     elif item is None:
        #         item=item
        #     elif isinstance(item,str):
        #         item=item
        #     else:
        #         item = item.decode('utf-8')
        # for item in urlList:
        #     if isinstance(item, str):
        #         item = item.decode('utf-8')
        #         wr.writerow([item,])
        #     elif isinstance(item, unicode):
        #         item = item
        #         wr.writerow([item,])
        #     else:
        #         item = item
        #         wr.writerow([item,])
        
        #self.current_link.text = self.current_link.text + data.strip() 
        newUrlList=[]
        for item in urlList:
            if isinstance(item, unicode):
                item=item.encode('utf-8')
                newUrlList.append(item)
            elif isinstance(item,str):
                item=item
                newUrlList.append(item)
            else:
                item=item
                newUrlList.append(item)

        wr.writerow(newUrlList)
        return r

    """[Author:Som ,last modified:16th April 2015]
    def _get_item:used to crawl items.
    Future requiremnets (items) will be passed here.

    @returns item
    @scrapes title which will stored in csv file
    """
    def _get_item(self, response):
        item = Page(url=response.url,content_length=str(len(response.body)))
            #response_header=response.headers,response_meta=response.meta,
            #response_connection=response.request.headers.get('Connection'))
        self._set_http_header_info(item,response)
        self._set_new_cookies(item,response)
        #self._set_title(item, response)
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

    """[Author:Som ,last modified:16th April 2015]
    def _set_http_header_info:used to crawl response status of
    any website like 200 : ok ,404 :not found etc.

    @returns responseStatus
    """
    def _set_http_header_info(self, page, response):
        #print "header"
        if isinstance(response, HtmlResponse):
            responseStatus = response.status
            #print "responseStatus",responseStatus
            if responseStatus:
                page['httpResponseStatus']=responseStatus
            else :
                page['httpResponseStatus']="-"
    """[Author:Som ,last modified:16th April 2015]
    def _set_new_cookies:used to crawl cookies of
    any website.
    """
    def _set_new_cookies(self, page, response):
        cookies = []
        #print "cookies"
        for cookie in [x.split(';', 1)[0] for x in response.headers.getlist('Set-Cookie')]:
            if cookie not in self.cookies_seen:
                self.cookies_seen.add(cookie)
                cookies.append(cookie)
        if cookies:
            page['newcookies'] = cookies
        else:
            page['newcookies'] = "-"

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
            if CNAME:
                page['CNAMEChain']=CNAME
            else:
                page['CNAMEChain']="-"
        except dns.resolver.NXDOMAIN:
            # CNAME.append('NONE')
            # page['CNAMEChain']=CNAME 
            page['CNAMEChain']="-"       
        except dns.resolver.Timeout:
            page['CNAMEChain']="-"
        except dns.exception.DNSException:
            page['CNAMEChain']="-"
        except dns.resolver.NoAnswer:
            page['CNAMEChain']="-"
