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
import ipwhois
from ipwhois import IPWhois
import gc
#from multiprocessing import Process, Value,Lock
from multiprocessing import Process, Lock
from multiprocessing.sharedctypes import Value
import threading
from datetime import datetime
from operator import itemgetter, attrgetter
#from pybloomfilter import BloomFilter
from scrapy.utils.job import job_dir
#from scrapy.dupefilter import BaseDupeFilter
import pygeoip
#from lockfile import LockFile
#import filelock
import fcntl


#from guppy import hpy
#import geoiplookup
#import logging
#import scrapy.statscol

# class BLOOMDupeFilter(BaseDupeFilter):
#     """Request Fingerprint duplicates filter"""
 
#     def __init__(self, path=None):
#         self.file = None
#         self.fingerprints = BloomFilter(2000000, 0.00001)
 
#     @classmethod
#     def from_settings(cls, settings):
#         return cls(job_dir(settings))
 
#     def request_seen(self, request):
#         fp = request.url
#         if fp in self.fingerprints:
#             return True
#         self.fingerprints.add(fp)
 
#     def close(self, reason):
#         self.fingerprints = None


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
    global logFile
    global dest_server_ip
    global _extract_object_count
    global getsecondleveldomain
    dest_server_ip=[]
    global dest_ASN
    global lock
    dest_ASN=[]
    counter=Counter(0)
    depth_counter=0
    tagType='A'
    # global distinct_asn
    # distinct_asn=[]
    # asn_counter=Counter(0)
    global logwr
    #global wr
    #global destIP
    #destIP=''
    # hp = hpy()
    # h = hp.heap()
    # print "##########################################"
    # print "size",h
    # print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
    #resultFile = codecs.open("output6.csv",mode='wb',encoding='utf-8')
    
    testFile = codecs.open("output7.csv",'wbr+')
    #logging.basicConfig(filename='example.log',level=logging.DEBUG)
    logFile = codecs.open("log.csv",'wbr+')
    fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount','UniqueExternalSites','ExternalSites']
    logwr = csv.DictWriter(logFile, fieldnames=fieldnames)
    #logwriter = csv.DictWriter(logFile, fieldnames=fieldnames)
    #lock = LockFile(/logFile)
    #writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    

    #lock = filelock.FileLock("output6.csv")
    lock = Lock()
    resultFile = codecs.open("output6.csv",'wbr+')
    #wr = csv.writer(resultFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)

    """[Author:Som ,last modified:16th April 2015]
    def __init__ :this act as constructor for python
    we pass arguments from core.py and those will be 
    stored in kw."""
    def __init__(self, **kw ):
        super(alexaSpider, self).__init__(**kw )
        #print "spider_name",kw.get(spider)
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
        #time.sleep(0.01)
        counter.increment()
        request=Request(self.url,callback=self.parse,meta={'counter': counter.value()},dont_filter=True)
        #lock.release()
        return [request]

    #@profile
    def parse(self,response):
        global items
        #global destIP
        #destIP=''
        urlList=[]
        r=[]
        #tagType='A'
        # item = Page(url=response.url)
        global resultFile
        page = self._get_item(response)
        #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
        #depth = str(page['depth_level'])
        #depth=dict.get('Age')
        #depth=dict.get('depth_level')

        # if 'depth' in depth:
        #     depth='1'
        # else:
        #     depth='0'
        lock.acquire()
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
        
        # Objectcount=response.meta.get('count')
        # logwr.writerow([Objectcount])
        # if tagType=='I':
        #     page['ImgCount']=Objectcount
        # elif tagType=='L':
        #     page['LinkCount']=Objectcount
        # elif tagType=='S':
        #     page['ScriptCount']=Objectcount
        # elif tagType=='E':
        #     page['EmbededCount']=Objectcount
        # else:
        #     page['ImgCount']='-'
        #     page['LinkCount']='-'
        #     page['ScriptCount']='-'
        #     page['EmbededCount']='-'
        #logwr.writerow([response.meta])
        # imgcount=[]
        # imgcount=response.meta.get('ImageList')
        # logwr.writerow([imgcount])
        # if imgcount:
        #     page['ImgCount']=imgcount        
        # else:
        #     page['ImgCount']='-'
        # scriptcount=[]
        # scriptcount=response.meta.get('ScriptList')
        # #logwr.writerow([scriptcount])
        # if scriptcount:
        #     page['ScriptCount']=scriptcount        
        # else:
        #     page['ScriptCount']='-'
        # linkcount=[]
        # linkcount=response.meta.get('LinkList')
        # #logwr.writerow([linkcount])
        # if linkcount:
        #     page['LinkCount']=linkcount        
        # else:
        #     page['LinkCount']='-'
        # embededcount=[]
        # embededcount=response.meta.get('EmbededList')
        # #logwr.writerow([embededcount])
        # if embededcount:
        #     page['EmbededCount']=embededcount        
        # else:
        #     page['EmbededCount']='-'

        r = [page]
        #urlList=[page]

        # '''commenting this part to use it later for 
        # recurively using links
        # for pageValue in page:
        #     urlList.append(page[pageValue])
        #r.extend(self._extract_requests(response,str(response.meta['counter']))) #external site link
        #gc.collect()
        r.extend(self._extract_requests(response,counter)) #external site link
        #gc.collect()
        r.extend(self._extract_img_requests(response,tagType,counter)) #link to img files
        r.extend(self._extract_script_requests(response,tagType,counter)) #link to script files like java script etc
        r.extend(self._extract_external_link_requests(response,tagType,counter)) #link to css or any other external linked files
        r.extend(self._extract_embed_requests(response,tagType,counter)) #link to addresses of the external file to embed
        
        
        #index=str(page['index'])
        urlList.append(page['index'])
        #urlList=page['index']
        urlList.append(page['depth_level'])
        urlList.append(page['httpResponseStatus'])
        urlList.append(page['content_length'])
        urlList.append(page['url'].strip())
        cookieStr=';'.join(page['newcookies'])
        urlList.append(cookieStr.strip())
        urlList.append(page['tagType'])
        cname=';'.join(page['CNAMEChain'])
        urlList.append(cname)
        dest_server_ip_values=';'.join(dest_server_ip)
        if dest_server_ip_values:
            urlList.append(dest_server_ip_values)
        else:
            urlList.append('-')
        asn_no=';'.join(dest_ASN)
        if asn_no:
            urlList.append(asn_no)
        else :
            urlList.append('-')
        # urlList.append(page['ImgCount'])
        # urlList.append(page['ScriptCount'])
        # urlList.append(page['LinkCount'])
        # urlList.append(page['EmbededCount'])
        
        # if page['destIP']:
        #     dest_server_ip_values=';'.join(page['destIP'])
        #     urlList.append(dest_server_ip_values)
        # else:
        #     urlList.append('-')
        # if page['ASN_Number']:
        #     urlList.append(asn_no)
        # else:
        #     urlList.append('-')

        urlList.append(page['start_time'])
        page['end_time']=datetime.now().time()
        urlList.append(page['end_time'])
        #urlList.append(page['response_header'])
        #urlList.append(page['response_meta'])
        #urlList.append (page['StartDate'])
        #urlList.append(page['ASN_Number'])
        #sorted(urlList, key=itemgetter(0))
        wr = csv.writer(resultFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
        #wr = csv.writer(resultFile, skipinitialspace=True,delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
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

        # for item in newUrlList:
        #     if item[]
        #newUrlList=sorted(newUrlList, key=attrgetter(page['index']))
        # lock.acquire()
        # try:
        #     wr.writerow(newUrlList)
        # finally:
        #     lock.release()
        # fcntl.flock(resultFile, fcntl.LOCK_EX)
        wr.writerow(newUrlList)
        # fcntl.flock(resultFile, fcntl.LOCK_UN)
        lock.release()
        # csv_f = csv.reader(resultFile)
        # index_set = set()
        # for row in csv_f:
        #     index_set.add(row[0])

        # for idx in index_set:
        #     f.seek(0)
        #     unique_asn = set ()
        #     for row in csv_f:
        #         if row[0] == idx:
        #             unique_asn.add(row[9])

        # for row in csv_f:
        #     if row[0] ==idx:
        #         wr.writerow(newUrlList+str(len(unique_asn)))
        #     else:
        #         wr.writerow(newUrlList+'-')

        return r


    # def getKey(page):
    #     return page['index']

    """[Author:Som ,last modified:16th April 2015]
    def _get_item:used to crawl items.
    Future requiremnets (items) will be passed here.

    @returns item
    @scrapes title which will stored in csv file
    """
    def _get_item(self, response):
        item = Page(url=response.url,content_length=str(len(response.body)),depth_level=response.meta,
            httpResponseStatus=response.status,start_time= datetime.now().time())
            #response_header=response.headers,response_meta=response.meta,start_time= datetime.now())
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

    def _extract_object_count(siteList):
        InternalSitesCount=0
        externalSitesCount=0
        uniqueExternalSites=set()
        externalSites=[]
        for site in siteList: 
            if site.startswith("http://") or site.startswith("https://"):
                externalSitesCount+=1
                uniqueExternalSites.add(getsecondleveldomain(site))
                externalSites.append(site)
            else:
                InternalSitesCount+=1
        return (externalSitesCount,InternalSitesCount,uniqueExternalSites,externalSites)

    def _extract_img_requests(self,response,tag,counter):
        r = []
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=[]
        if isinstance(response, HtmlResponse):
            tag='I'
            #imgcount=0
            counterValueImg=counter
            sites = Selector(response).xpath("//img/@src").extract()
            # for site in sites:
            #     imgcount=imgcount+1
            #logging.info('imgcount',imgcount)
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            #wr.writerow(siteList)
            
            externalImageCount,InternalImageCount,uniqueExternalSites,externalSites =_extract_object_count(siteList)
            Imagecount=len(siteList)
            #lock.acquire()
            
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueImg
            # ObjectList['Imagecount']=Imagecount
            # ObjectList['InternalImageCount']=InternalImageCount
            # ObjectList['ExternalImageCount']=externalImageCount

            # logwr.writeheader()
            logwr.writerow({'url': response.url, 'counter': counterValueImg,'InternalImageCount':InternalImageCount,'ExternalImageCount':externalImageCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites})
            #logwr.writerow([ObjectList])
            #lock.release()
            #wr.writerow([Imagecount])
            #logwr.writerow([imgcount])
            #Imagecount=str(len(siteList))
            #logwr.writerow([siteList])
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueImg})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _extract_script_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=[]
        if isinstance(response, HtmlResponse):
            tag='S'
            #scriptcount=0
            counterValueScript=counter
            sites = Selector(response).xpath("//script/@src").extract()
            # for site in sites:
            #     scriptcount=scriptcount+1
            #logging.info('scriptcount',scriptcount)
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            wr.writerow(siteList)
            externalscriptCount,InternalscriptCount,uniqueExternalSites,externalSites=_extract_object_count(siteList)
            scriptcount=len(siteList)
            #lock.acquire()
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueScript
            # ObjectList['scriptcount']=scriptcount
            # ObjectList['InternalscriptCount']=InternalscriptCount
            # ObjectList['ExternalscriptCount']=externalscriptCount
            # logwr.writerow([ObjectList])

            # logwr.writeheader()
            logwr.writerow({'url': response.url, 'counter': counterValueScript,'InternalscriptCount':InternalscriptCount,'ExternalscriptCount':externalscriptCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites})
            #lock.release()
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueScript})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _extract_external_link_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=[]
        if isinstance(response, HtmlResponse):
            tag='L'
            #linkcount=0
            counterValueLink=counter
            sites = Selector(response).xpath("//link/@href").extract()
            # for site in sites:
            #     linkcount=linkcount+1
            #logging.info('linkcount',linkcount)
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            sites.append(counterValueLink)
            externallinkCount,InternallinkCount,uniqueExternalSites,externalSites=_extract_object_count(siteList)
            #wr.writerow(siteList)
            linkcount=len(siteList)
            #lock.acquire()
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueLink
            # ObjectList['linkcount']=linkcount
            # ObjectList['InternallinkCount']=InternallinkCount
            # ObjectList['ExternallinkCount']=externallinkCount
            # #lock.acquire()
            # logwr.writerow([ObjectList])
            logwr.writerow({'url': response.url, 'counter': counterValueLink,'InternallinkCount':InternallinkCount,'ExternallinkCount':externallinkCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites})
            #lock.acquire()
            #lock.release()
            r.extend(Request(site, callback=self.parse,meta={'tagType': tag,'counter': counterValueLink})for site in siteList if site.startswith("http://") or site.startswith("https://"))
        return r

    def _extract_embed_requests(self,response,tag,counter):
        r=[]
        siteList=[]
        ObjectList=dict()
        externalSites=[]
        #uniqueExternalSites=set()
        if isinstance(response, HtmlResponse):
            tag='E'
            #embededcount=0
            counterValueEmded=counter
            sites = Selector(response).xpath("//embed/@src").extract()
            # for site in sites:
            #     embededcount=embededcount+1
            #logging.info('embededcount',embededcount)
            wr = csv.writer(testFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            for item in sites:
                if isinstance(item, unicode):
                    item=item.encode('utf-8')
                    siteList.append(item)
                else:
                    siteList.append(item)
            wr.writerow(siteList)
            externalembededCount,InternalembededCount,uniqueExternalSites,externalSites=_extract_object_count(siteList)
            embededcount=len(siteList)
            #lock.acquire()
            # ObjectList['url']=response.url
            # ObjectList['counter']=counterValueEmded
            # ObjectList['embededcount']=embededcount
            # ObjectList['InternalembededCount']=InternalembededCount
            # ObjectList['ExternalembededCount']=externalembededCount
            # #lock.acquire()
            # logwr.writerow([ObjectList])
            logwr.writerow({'url': response.url, 'counter': counterValueEmded,'InternalembededCount':InternalembededCount,'ExternalembededCount':externalembededCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites})
            #lock.acquire()
            #lock.release()
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
        CNAMEList=[]
        # dest_server_ip=[]
        # dest_ASN=[]
        dest_server_ip[:] = []
        dest_ASN[:]=[]
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
            destServerIPs = dns.resolver.query(domain, 'A')
            #page['destIP']='1'
            for rdata in answers:
                try:
                    CNAMEList.append(str(rdata))
                    while (rdata.target):
                        value=dns.resolver.query(rdata.target, 'CNAME')
                        for rdata in value:
                            CNAMEList.append(str(rdata))
                except dns.resolver.NXDOMAIN:
                    continue
                except dns.resolver.Timeout:
                    continue
                except dns.exception.DNSException:
                    continue
                except dns.resolver.NoAnswer:
                    continue

            for ip_address in destServerIPs:
                try:
                    if not str(ip_address) in dest_server_ip:
                         dest_server_ip.append(str(ip_address))
                    #asn_info=IPWhois(str(IPs))
                    gir = pygeoip.GeoIP('/usr/share/GeoIP/GeoIPASNum.dat',
                       flags=pygeoip.const.GEOIP_STANDARD)
                    #gi.asn_by_name(IPs)
                    # if str(ip_address):
                    #     dest_ASN.append(str(ip_address))
                    # else:
                    #     dest_ASN.append('-')
                    asNum=gir.asn_by_name(str(ip_address))
                    if asNum:
                        asNumSplit=asNum.split(' ')
                        asn=''.join(x for x in asNumSplit[0] if x.isdigit())
                        if not asn in dest_ASN:
                            dest_ASN.append(asn)
                    else:
                        dest_ASN.append('-')

                    
                    # if isinstance(asn_info, unicode):
                    #     asn_info=asn_info.encode('utf-8')
                    
                    # try:
                    #     results = asn_info.lookup()
                    #     if not results['asn'] in dest_ASN: 
                    #         dest_ASN.append(results['asn'])
                    # except:
                    #     dest_ASN.append('-')
                    #mydict.keys()[mydict.values().index(16)]
                    
                    #dest_ASN.append(results)
                except dns.resolver.NXDOMAIN:
                    continue
                except dns.resolver.Timeout:
                    continue
                except dns.exception.DNSException:
                    continue
                except dns.resolver.NoAnswer:
                    continue

            if dest_ASN:
                page['ASN_Number']=dest_ASN
            else:
                page['ASN_Number']='-'

            if CNAMEList:
                page['CNAMEChain']=CNAMEList
            else:
                page['CNAMEChain']="-"

            if dest_server_ip:
                page['destIP']=dest_server_ip
            else:
                page['destIP']='-'
        except dns.resolver.NXDOMAIN:
            # CNAME.append('NONE')
            # page['CNAMEChain']=CNAME 
            page['CNAMEChain']="-"   
            #page['destIP']='-'    
        except dns.resolver.Timeout:
            page['CNAMEChain']="-"
            #page['destIP']='-'
        except dns.exception.DNSException:
            page['CNAMEChain']="-"
            #page['destIP']='-'
        except dns.resolver.NoAnswer:
            page['CNAMEChain']="-"
            #page['destIP']='-'

    def getsecondleveldomain(url):
        # load tlds, ignore comments and empty lines:
        with open("effective_tld_names.dat") as tld_file:
            tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]

        url_elements = urlparse(url)[1].split('.')
        # url_elements = ["abcde","co","uk"]

        for i in range(-len(url_elements), 0):
            last_i_elements = url_elements[i:]
            #    i=-3: ["abcde","co","uk"]
            #    i=-2: ["co","uk"]
            #    i=-1: ["uk"] etc

            candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
            wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
            exception_candidate = "!" + candidate

            # match tlds: 
            if (exception_candidate in tlds):
                return ".".join(url_elements[i:]) 
            if (candidate in tlds or wildcard_candidate in tlds):
                return ".".join(url_elements[i-1:])
                # returns "abcde.co.uk"