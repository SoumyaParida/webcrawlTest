import re
import csv
import scrapy.crawler
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.contrib.linkextractors import LinkExtractor as sle
#from scrapy.contrib.linkextractors.sgml import LinkExtractor as sgml
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
import threading
import time
#from multiprocessing import Process, Value,Lock
from multiprocessing import Process, Lock
from multiprocessing.sharedctypes import Value
import threading

class Counter(object):
    def __init__(self, initval=0):
        self.val = Value('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value


#from scrapy.stats import stats
class alexaSpider(Spider):

    name = 'alexa'
    items=[]
    global resultFile
    global counter
    global depth_counter
    global tagType
    global testFile
    counter=Counter(0)
    depth_counter=0
    tagType='A'
    
    #resultFile = codecs.open("output6.csv",mode='wb',encoding='utf-8')
    resultFile = codecs.open("output6.csv",'wbr+')
    testFile = codecs.open("output7.csv",'wbr+')

    """[Author:Som ,last modified:16th April 2015]
    def __init__ :this act as constructor for python
    we pass arguments from core.py and those will be 
    stored in kw."""
    def __init__(self, **kw ):
        super(alexaSpider, self).__init__(**kw )
        url = kw.get('url') or kw.get('domain')
        counter=kw.get('indexValue')
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://%s' % url
        self.url = url
        self.allowed_domains = [re.sub(r'^www\.', '', urlparse(url).hostname)]
        #print "url inside init",url
        self.link_extractor = sle()
        self.cookies_seen = set()

        

    """[Author:Som ,last modified:15th April 2015]
    start_requests:Overriding method of scrapy.spider.Spider class. 
    This is the method called by Scrapy when the spider is opened 
    for scraping when no particular URLs are specified 
    This method must return an iterable with the first Requests to 
    crawl for this spider."""
    def start_requests(self):
        #index=random.randint(1, 20)

        #global counter
        
        #counter = Value('i',range(20))
        #lock = Lock()
        #lock.acquire()
        #counter=counter+1
        # Page(index=counter)
        
        # with counter.get_lock():
        #     counter.value += 1
        time.sleep(0.01)
        counter.increment()
        request=Request(self.url,callback=self.parse,meta={'counter': counter.value()},dont_filter=True)
        #lock.release()
        return [request]

    def parse(self,response):
        global items
        urlList=[]
        r=[]
        #tagType='A'
        # item = Page(url=response.url)
        global resultFile
        page = self._get_item(response)
        #depth = str(page['depth_level'])
        #depth=dict.get('Age')
        #depth=dict.get('depth_level')

        # if 'depth' in depth:
        #     depth='1'
        # else:
        #     depth='0'

        depth = page['depth_level']
        depth_value=depth.get('depth')
        if depth_value:
            page['depth_level'] = depth_value
        else:
            page['depth_level']='0'

        #page['index']=response.meta['counter']
        counter=response.meta.get('counter')
        
        if counter:
            page['index']=counter
        else :
            page['index']='1'
        #if response.meta['tagType']:
        #page['tagType']=
        tagType=response.meta.get('tagType')  
        if tagType:
            page['tagType']=tagType        
        else:
            page['tagType']='A'
        #else:
        #    page['tagType']=tagType
        
        r = [page]
        #urlList=[page]

        # '''commenting this part to use it later for 
        # recurively using links
        # for pageValue in page:
        #     urlList.append(page[pageValue])
        #r.extend(self._extract_requests(response,str(response.meta['counter']))) #external site link
        r.extend(self._extract_requests(response,counter)) #external site link
        r.extend(self._extract_img_requests(response,tagType,counter)) #link to img files
        r.extend(self._extract_script_requests(response,tagType,counter)) #link to script files like java script etc
        r.extend(self._extract_external_link_requests(response,tagType,counter)) #link to css or any other external linked files
        r.extend(self._extract_embed_requests(response,tagType,counter)) #link to addresses of the external file to embed
        
        
        urlList.append(page['index'])
        urlList.append(page['depth_level'])
        urlList.append(page['httpResponseStatus'])
        urlList.append(page['content_length'])
        urlList.append(page['url'].strip())
        cookieStr=';'.join(page['newcookies'])
        urlList.append(cookieStr.strip())
        urlList.append(page['tagType'])
        cname=';'.join(page['CNAMEChain'])
        urlList.append(cname)
        
        wr = csv.writer(resultFile, skipinitialspace=True,delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
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
        item = Page(url=response.url,content_length=str(len(response.body)),depth_level=response.meta,
            httpResponseStatus=response.status)
            #response_header=response.headers,response_meta=response.meta,
            #response_connection=response.request.headers.get('Connection'))
        
        #self._set_http_header_info(item,response)
        self._set_new_cookies(item,response)
        #self._set_title(item, response)
        self._set_DNS_info(item,response)
        return item

    """[Author:Som ,last modified:16th April 2015]
    def _extract_requests:used to crawl urls.

    @returns urls
    """
    def _extract_requests(self,response,counter):
        r = []
        counterValue=counter
        if isinstance(response, HtmlResponse):
            links = self.link_extractor.extract_links(response)
            r.extend(Request(x.url, callback=self.parse,meta={'counter': counterValue})for x in links if x.url != response.url)

        return r

    def _extract_img_requests(self,response,tag,counter):
        r = []
        siteList=[]
        if isinstance(response, HtmlResponse):
            tag='I'
            counterValueImg=counter
            sites = Selector(response).xpath("//img/@src").extract()
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            wr.writerow(siteList)
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueImg})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _extract_script_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        if isinstance(response, HtmlResponse):
            tag='S'
            counterValueScript=counter
            sites = Selector(response).xpath("//script/@src").extract()
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            wr.writerow(siteList)
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueScript})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _extract_external_link_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        if isinstance(response, HtmlResponse):
            tag='L'
            counterValueLink=counter
            sites = Selector(response).xpath("//link/@href").extract()
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            sites.append(counterValueLink)
            wr.writerow(siteList)
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueLink})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _extract_embed_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        if isinstance(response, HtmlResponse):
            tag='E'
            counterValueEmded=counter
            sites = Selector(response).xpath("//embed/@src").extract()
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            wr.writerow(siteList)
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueEmded})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _set_title(self, page, response):
        if isinstance(response, HtmlResponse):
            title = Selector(response).xpath("//title/text()").extract()
            if title:
                page['title'] = title[0]

    """[Author:Som ,last modified:16th April 2015]
    def _set_http_header_info:used to crawl response status of
    any website like 200 : ok ,404 :not found etc.

    @returns responseStatus
    """
    # def _set_http_header_info(self, page, response):
    #     if isinstance(response, HtmlResponse):
    #         responseStatus = response.status
    #         #print "responseStatus",responseStatus
    #         if responseStatus:
    #             page['httpResponseStatus']=responseStatus
    #         else :
    #             page['httpResponseStatus']="-"
    """[Author:Som ,last modified:16th April 2015]
    def _set_new_cookies:used to crawl cookies of
    any website.
    """
    def _set_new_cookies(self, page, response):
        cookies = []
        for cookie in [x.split(';', 1)[0] for x in response.headers.getlist('Set-Cookie')]:
            if cookie not in self.cookies_seen:
                self.cookies_seen.add(cookie)
                cookies.append(cookie.replace(" [",""))
        # cookie=response.headers.getlist('Set-Cookie')
        # for cookieValue in cookie:
        #     cookies.append(cookieValue.strip())
        #     cookies.append('|')
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
                    CNAME.append(str(rdata))
                    while (rdata.target):
                        value=dns.resolver.query(rdata.target, 'CNAME')
                        for rdata in value:
                            CNAME.append(str(rdata))
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
