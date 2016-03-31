import re
import csv
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.contrib.linkextractors import LinkExtractor as sle
from alexaCrawl.items import Page
import dns.resolver
import dns.query
import dns.flags
import codecs
import sys
# from multiprocessing import Process, Lock
# from multiprocessing import Process, Value, Lock
import time
from datetime import datetime
import pygeoip
import Queue
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.http.cookies import CookieJar
import ast
from HTMLParser import HTMLParser
from collections import defaultdict
import threading
import urllib


list_of_tags=list()
tags_d = defaultdict(int)
lock = threading.Lock()
reload(sys)
sys.setdefaultencoding('utf-8')
class MyHTMLParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        lock.acquire()
        try:
            tags_d[tag] += 1
            # cdnlistFileWriter.writerow([tag])
        finally:
            lock.release()

def getCodedList(sites,siteList):
    if len(sites) > 0 :
        for item in sites:
            if isinstance(item, unicode):
                item=item.encode('utf-8')
                siteList.append(item)
            else:
                siteList.append(item)
    return siteList
#from scrapy.stats import stats
class alexaSpider(Spider): 
    name='alexa'
    # global counter
    global resultFile
    global tagType
    global dest_server_ip
    global _extract_object_count
    global getsecondleveldomain
    global dest_ASN    
    global urllist
    global resulturldict   
    dest_ASN=[]
    dest_server_ip=[]
    urllist=[]
    urlIndexlist=dict()
    resulturldict=dict()
    resultFile = codecs.open("output6.csv",'wbr+')
    """[Author:Som ,last modified:16th April 2015]
    def __init__ :this act as constructor for python
    we pass arguments from core.py and those will be 
    stored in kw."""

    def __init__ (self,arg1,arg2):
        self.urllistfile=ast.literal_eval(arg1)
        self.urlIndexlist=ast.literal_eval(arg2)
        for url in self.urllistfile:
            if not url.startswith('http://') and not url.startswith('https://'):
                newurl = 'http://%s' % url
            resulturldict[newurl]=self.urlIndexlist.get(url)    
 
    def start_requests(self):
        self.link_extractor = sle()
        self.cookies_seen = set()
        i=0
        for url in resulturldict:
            indexUnique=resulturldict.get(url)
            yield Request(url, meta={'counter': indexUnique},method='GET',callback=self.parse,dont_filter=True)

    """[Author:Som ,last modified:15th April 2015]
    start_requests:Overriding method of scrapy.spider.Spider class. 
    This is the method called by Scrapy when the spider is opened 
    for scraping when no particular URLs are specified 
    This method must return an iterable with the first Requests to 
    crawl for this spider."""

   # @profile
    def parse(self,response):
        urlList=[]
        r=[]
        global resultFile
        counter=response.meta.get('counter')
        page = self._get_item(response,counter)

        # links = self.link_extractor.extract_links(response)
        # if link in links :
        depth = page['depth_level']
        depth_value=depth.get('depth')
        if depth_value:
            page['depth_level'] = depth_value
        else:
            page['depth_level']='0'

        if counter:
            page['index']=counter
        else :
            page['index']='1'
        tagType=response.meta.get('tagType')  
        if tagType:
            page['tagType']=tagType        
        else:
            page['tagType']='a'

        r = [page]
        r.extend(self._extract_requests(response,tagType,counter,page)) #external site link

        urlList.append(page['index'])
        urlList.append(page['depth_level'])
        urlList.append(page['httpResponseStatus'])
        urlList.append(page['content_length'])
        urlList.append(page['url'].strip())
        cookieStr=';'.join(page['newcookies'])
        cookieStr = cookieStr.replace(" ","")
        urlList.append(cookieStr.strip())
        urlList.append(page['tagType'])
        cname=';'.join(page['CNAMEChain'])
        urlList.append(cname)
        dest_server_ip_values=';'.join(dest_server_ip)
        if dest_server_ip_values:
            page['destIP']=dest_server_ip_values
        else:
            page['destIP']='-'

        urlList.append(page['destIP'])
        asn_no=';'.join(dest_ASN)
        if asn_no:
            page['ASN_Number']=asn_no
        else :
            page['ASN_Number']='-'

        urlList.append(page['ASN_Number'])
        urlList.append(page['start_time'])
        page['end_time']=datetime.now().time()
        urlList.append(page['end_time'])
        wr = csv.writer(resultFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)

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
    #@profile
    def _get_item(self, response,counter):
        item = Page(url=response.url,content_length=str(len(response.body)),depth_level=response.meta,
            httpResponseStatus=response.status,start_time= datetime.now().time())
        self._set_new_cookies(item,response)
        self._set_DNS_info(item,response)
        return item

    """[Author:Som ,last modified:16th April 2015]
    def _extract_requests:used to crawl urls.

    @returns urls
    """

    #@profile
    def _extract_requests(self,response,tagType,counter,page):
        r = []
        tags=set()
        counterValue=counter
        parser = MyHTMLParser()
        sourceCode=response.body
        parser.feed(sourceCode)
        counterValue=counter
        sites=list()
        for k,v in tags_d.iteritems():
            siteList=[]
            if isinstance(response, HtmlResponse):
                if str(k) =='a':
                    for link in self.link_extractor.extract_links(response):
                        sites.append(link.url)
                    if len(sites) > 0 :
                        for item in sites:
                            if isinstance(item, unicode):
                                item=item.encode('utf-8')
                                siteList.append(item)
                            else:
                                siteList.append(item)
                else:
                    sites=Selector(response).xpath('//'+str(k)+'/@href').extract()
                    siteSrc=Selector(response).xpath('//'+str(k)+'/@src').extract()
                    siteCodebase=Selector(response).xpath('//'+str(k)+'/@codebase').extract()
                    siteCite=Selector(response).xpath('//'+str(k)+'/@cite').extract()
                    siteBackGround=Selector(response).xpath('//'+str(k)+'/@background').extract()
                    siteActiom=Selector(response).xpath('//'+str(k)+'/@action').extract()
                    siteLongdesc=Selector(response).xpath('//'+str(k)+'/@longdesc').extract()
                    siteClassid=Selector(response).xpath('//'+str(k)+'/@classid').extract()
                    siteData=Selector(response).xpath('//'+str(k)+'/@data').extract()
                    siteUsemap=Selector(response).xpath('//'+str(k)+'/@usemap').extract()
                    siteForm=Selector(response).xpath('//'+str(k)+'/@formaction').extract()
                    siteIcon=Selector(response).xpath('//'+str(k)+'/@icon').extract()
                    sitePoster=Selector(response).xpath('//'+str(k)+'/@poster').extract()
                    siteArchieve=Selector(response).xpath('//'+str(k)+'/@archive').extract()
                    siteManifest=Selector(response).xpath('//'+str(k)+'/@manifest').extract()
                    siteList=getCodedList(sites,siteList)
                    siteList=getCodedList(siteSrc,siteList)
                    siteList=getCodedList(siteCodebase,siteList)
                    siteList=getCodedList(siteCite,siteList)
                    siteList=getCodedList(siteBackGround,siteList)
                    siteList=getCodedList(siteActiom,siteList)
                    siteList=getCodedList(siteLongdesc,siteList)
                    siteList=getCodedList(siteClassid,siteList)
                    siteList=getCodedList(siteData,siteList)
                    siteList=getCodedList(siteUsemap,siteList)
                    siteList=getCodedList(siteForm,siteList)
                    siteList=getCodedList(siteIcon,siteList)
                    siteList=getCodedList(sitePoster,siteList)
                    siteList=getCodedList(siteArchieve,siteList)
                    siteList=getCodedList(siteManifest,siteList)
            r.extend(Request(site, callback=self.parse,method='HEAD',meta={'counter': counterValue,'tagType': str(k),'download_timeout':3})for site in siteList if site.startswith("http://") or site.startswith("https://") or site.startswith("www."))            
        return r        
    #@profile
    def _extract_object_count(siteList):
        InternalSitesCount=0
        externalSitesCount=0
        uniqueExternalSites=set()
        externalSites=[]
        for site in siteList: 
            if site.startswith("http://") or site.startswith("https://") or site.startswith("http://www.") or site.startswith("https://www."):
                externalSitesCount+=1
                uniqueExternalSites.add(getsecondleveldomain(site))
                externalSites.append(site)
            else:
                InternalSitesCount+=1
        return (externalSitesCount,InternalSitesCount,len(uniqueExternalSites),externalSites,uniqueExternalSites)
    #@profile
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
    #@profile
    def _set_new_cookies(self, page, response): 
        cookies = []
        for cookie in [x.split(';', 1)[0] for x in response.headers.getlist('Set-Cookie')]:
            if cookie not in self.cookies_seen:
                self.cookies_seen.add(cookie)
                cookies.append(cookie.replace(" [",""))
        if cookies:
            page['newcookies'] = cookies
        else:
            page['newcookies'] = "-"

    """[Author:Som ,last modified:22th April 2015]
        def _set_DNS_info:used to retrieve CNAME chain.
    """
    #@profile
    def _set_DNS_info(self, page,response):
        # start_time = time.clock()
        CNAMEList=set()
        # dest_server_ip=[]
        # dest_ASN=[]
        dest_server_ip[:] = []
        dest_ASN[:]=[]
        domain=response.url
        global dns_lookup_time
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
                    CNAMEList.add(str(rdata))
                    while (rdata.target):
                        value=dns.resolver.query(rdata.target, 'CNAME')
                        for rdata in value:
                            CNAMEList.add(str(rdata))
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
                    gir = pygeoip.GeoIP('GeoIPASNum.dat',
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
        # dns_lookup_time=dns_lookup_time+(time.clock() -start_time)
        # print dns_lookup_time, "dns"
    #@profile
    def getsecondleveldomain(url):
        with open("effective_tld_names.dat") as tld_file:
            tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
        url_elements = urlparse(url)[1].split('.')
        for i in range(-len(url_elements), 0):
            last_i_elements = url_elements[i:]
            candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
            wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
            exception_candidate = "!" + candidate

            # match tlds: 
            if (exception_candidate in tlds):
                return ".".join(url_elements[i:]) 
            if (candidate in tlds or wildcard_candidate in tlds):
                return ".".join(url_elements[i-1:])