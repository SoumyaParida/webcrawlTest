import re
import csv
from urlparse import urlparse
from scrapy.http import Request, HtmlResponse
from scrapy.spider import Spider
from scrapy.selector import Selector
from alexaCrawl.items import Page
import dns.resolver
import dns.query
import dns.flags
import codecs
import sys
from datetime import datetime
import pygeoip
import ast
from HTMLParser import HTMLParser
from collections import defaultdict
import threading
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor

list_of_tags=list()

lock = threading.Lock()
reload(sys)
#sys.setdefaultencoding('utf-8')
sys.setdefaultencoding("utf-8")

resultFile = codecs.open("output6.csv",'wbr+')
lock = threading.Lock()
def write_to_file(wr,value):
    lock.acquire() # thread blocks at this line until it can obtain lock
    # in this section, only one thread can be present at a time.
    wr.writerow(value)
    lock.release()

def filesize(asset):  
    wr = csv.writer(resultFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)  
    write_to_file(wr,asset)
class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.tags_d = defaultdict(int)
        self.list_of_attributes = set()

    def handle_starttag(self, tag, attrs):
        lock.acquire()
        try:
            self.tags_d[tag] += 1
        finally:
            lock.release()

def getCodedList(sites,siteList):
    if len(sites) > 0 :
        for item in sites:
            if isinstance(item, unicode):
                item=item.decode('latin-1').encode('utf-8')
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
    #global dest_server_ip
    global _extract_object_count
    global _distinctASN
    global getsecondleveldomain
    global dest_ASN    
    global urllist
    global resulturldict   
    dest_ASN=[]
    #dest_server_ip=[]
    urllist=[]
    urlIndexlist=dict()
    resulturldict=dict()
    #resultFile = codecs.open("output6.csv",'wbr+')
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
        page['InternalObjectCount'] = 0
        page['ExternalObjectCount'] = 0
        page['NumberOfuniqueExternalSecondlevelSites'] = 0
        page['distinctASNs'] = 0
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
        dest_server_ip_values=';'.join(page['destIP'])
        urlList.append(dest_server_ip_values)
        asn_no=';'.join(page['ASN_Number'])
        urlList.append(asn_no)
        urlList.append(page['distinctASNs'])
        urlList.append(page['InternalObjectCount'])
        urlList.append(page['ExternalObjectCount'])
        urlList.append(page['NumberOfuniqueExternalSecondlevelSites'])
        urlList.append(page['start_time'])
        page['end_time']=datetime.now().time()
        urlList.append(page['end_time'])
        newUrlList=[]
        for item in urlList:
            if isinstance(item, unicode):
                item=item.decode('latin-1').encode('utf-8')
                newUrlList.append(item)
            elif isinstance(item,str):
                item=item
                newUrlList.append(item)
            else:
                item=item
                newUrlList.append(item)
        filesize(newUrlList)
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
        sourceCode=sourceCode.decode('latin-1').encode('utf-8')
        parser.feed(sourceCode)
        counterValue=counter
        internalSites=0
        externalSites=0
        uniqueSecondlevelSites=set()
        distinctAsn=set()
        Asnlist=set()
        list_of_attri=['href','src','action','data','poster','background','icon']
        for k,v in parser.tags_d.iteritems():
            if isinstance(response, HtmlResponse):
                sites=list()
                siteList=list()
                if str(k) =='img':
                    sites=Selector(response).xpath('//img/@src').extract()
                    siteList=getCodedList(sites,siteList)
                else:
                    for link in LxmlLinkExtractor(deny_extensions=['exe'],tags=str(k),attrs=list_of_attri).extract_links(response):
                        sites.append(link.url) 
                    siteList=getCodedList(sites,siteList)
                externalObjectCount, InternalObjectCount, NumberOfuniqueExternalSecondlevelSites = _extract_object_count(siteList)
                Asnlist= _distinctASN(siteList)
                distinctAsn = distinctAsn | Asnlist
                internalSites=internalSites+InternalObjectCount
                externalSites=externalSites+externalObjectCount
                uniqueSecondlevelSites=uniqueSecondlevelSites | NumberOfuniqueExternalSecondlevelSites

                r.extend(Request(site, callback=self.parse,method='HEAD',meta={'counter': counterValue,'tagType': str(k),'download_timeout':15})for site in siteList if site.startswith("http://") or site.startswith("https://") or site.startswith("www."))
        if int(externalSites) > 0:
            page['ExternalObjectCount'] = externalSites
        else:
            page['ExternalObjectCount'] = 0

        if int(internalSites) > 0:
            page['InternalObjectCount'] = internalSites
        else:
            page['InternalObjectCount'] = 0

        if len(uniqueSecondlevelSites) > 0:
            page['NumberOfuniqueExternalSecondlevelSites'] = len(uniqueSecondlevelSites)
        else:
            page['NumberOfuniqueExternalSecondlevelSites'] = 0

        if len(distinctAsn) > 0:
            page['distinctASNs'] = len(distinctAsn)
        else:
            page['distinctASNs'] = 0
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
        return (externalSitesCount,InternalSitesCount,uniqueExternalSites)

    def _distinctASN(weblinks):
        destASNValues=set()
        for site in weblinks:
            domain = urlparse(site).netloc
            if domain.startswith('http://'):
                domain = domain.replace("http://", "", 1)
            elif domain.startswith('https://'):
                domain = domain.replace("https://", "", 1)

            if domain.endswith('/'):
                domain = domain.replace("/", "", 1)

            if not domain.startswith('www.'):
                domain = 'www.%s' % domain
            try:
                destServ = dns.resolver.query(domain, 'A')
                for ip_address in destServ:
                        gir = pygeoip.GeoIP('GeoIPASNum.dat',flags=pygeoip.const.GEOIP_STANDARD)
                        asNum = gir.asn_by_name(str(ip_address))
                        if asNum:
                            asNumSplit = asNum.split(' ')
                            asn = ''.join(x for x in asNumSplit[0] if x.isdigit())
                            if not asn in destASNValues:
                                destASNValues.add(asn)
                        else:
                            destASNValues.add('-')
            except dns.resolver.NXDOMAIN:
                continue
            except dns.resolver.Timeout:
                continue
            except dns.exception.DNSException:
                continue
            except dns.resolver.NoAnswer:
                continue
        return destASNValues
    #@profile
    def _set_title(self, page, response):
        if isinstance(response, HtmlResponse):
            title = Selector(response).xpath("//title/text()").extract()
            if title:
                page['title'] = title[0]

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
        if cookies:
            page['newcookies'] = cookies
        else:
            page['newcookies'] = "-"

    """[Author:Som ,last modified:22th April 2015]
        def _set_DNS_info:used to retrieve CNAME chain.
    """
    def _set_DNS_info(self, page,response):
        CNAMEList = list()
        dest_server_ip=list()
        #dest_server_ip[:] = []
        dest_ASN[:]=[]
        domain = response.url
        global dns_lookup_time
        # urlparse :This module defines a standard interface to break URL strings up
        # in components (addressing scheme, network location, path etc.), to combine
        # the components back into a URL string, and to convert a relative URL to
        # an absolute URL given a base URL.
        domain = urlparse(domain).netloc
        if domain.startswith('http://'):
            domain = domain.replace("http://", "", 1)
        elif domain.startswith('https://'):
            domain = domain.replace("https://", "", 1)

        if domain.endswith('/'):
            domain = domain.replace("/", "", 1)
        try:
            #answer = dns.resolver.query(domain,"A")
            #print answer.response
            myResolver = dns.resolver.Resolver()
            myAnswers = myResolver.query(domain, "A")
            value = str(myAnswers.response).splitlines()
            #print value

            for item in value:
                if "IN A" in item:
                    ip=str(item.split("IN A")[1])
                    if ip:
                        domain=item.split(' ')[0].strip()
                        domain=domain.lower()  
                        if domain.startswith('www.'):
                            domain = domain.replace("www","")
                        if domain.endswith('.'):
                            domain=domain[:-1]
                        if not domain.startswith('http://'):
                            domain = 'http://%s' % domain
                        secondlevelurl=str(getsecondleveldomain(domain))
                        CNAMEList.append(secondlevelurl)
                        dest_server_ip.append(str(ip).strip())
                        gir = pygeoip.GeoIP('GeoIPASNum.dat',flags=pygeoip.const.GEOIP_STANDARD)
                        asNum = gir.asn_by_name(str(ip))
                        if asNum:
                            asNumSplit = asNum.split(' ')
                            asn = ''.join(x for x in asNumSplit[0] if x.isdigit())
                            if not asn in dest_ASN:
                                dest_ASN.append(asn)
            if len(dest_ASN) >0:
                page['ASN_Number'] = dest_ASN
            else:
                page['ASN_Number'] = '-'

            if len(CNAMEList) >0:
                page['CNAMEChain'] = CNAMEList
            else:
                page['CNAMEChain'] = "-"

            if len(dest_server_ip) >0:
                page['destIP'] = dest_server_ip
            else:
                page['destIP'] = '-'
        except dns.resolver.NXDOMAIN:
            page['CNAMEChain'] = "-"
            page['destIP']= "-"
            page['ASN_Number']= "-"
        except dns.resolver.Timeout:
            page['CNAMEChain'] = "-"
            page['destIP']= "-"
            page['ASN_Number']= "-"
        except dns.exception.DNSException:
            page['CNAMEChain'] = "-"
            page['destIP']= "-"
            page['ASN_Number']= "-"
        except dns.resolver.NoAnswer:
            page['CNAMEChain'] = "-"
            page['destIP']= "-"
            page['ASN_Number']= "-"
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
