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
import urllib
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.contrib.spidermiddleware.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

list_of_tags=list()

lock = threading.Lock()
reload(sys)
#sys.setdefaultencoding('utf-8')
sys.setdefaultencoding("utf-8")

resultFile = codecs.open("output6.csv",'wbr+')
successfulUrlsList=codecs.open("successfulUrls.csv",'wbr+')
errorLogfile=codecs.open("errorLog.csv",'wbr+')
errorwr = csv.writer(errorLogfile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
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
    global getCNameIpAsn
    global dest_ASN    
    global urllist
    global resulturldict 
    global urlParseSite
    global successfulUrlsList
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
            urlCnameDict=dict()
            urlCnameDict=getCNameIpAsn(url)
            yield Request(url, meta={'counter': indexUnique,'resultDict': urlCnameDict},method='GET',callback=self.parse,errback=self.errback_httpbin,dont_filter=True)

    def errback_httpbin(self,failure):
        error=list()
        # in case you want to do something special for some errors,
        # you may need the failure's type:
        if failure.check(HttpError):
            error.append(failure)
        elif failure.check(DNSLookupError):
            error.append(failure)
        elif failure.check(TimeoutError, TCPTimedOutError):
            error.append(failure)
        errorwr.writerow(error)

    """[Author:Som ,last modified:15th April 2015]
    start_requests:Overriding method of scrapy.spider.Spider class. 
    This is the method called by Scrapy when the spider is opened 
    for scraping when no particular URLs are specified 
    This method must return an iterable with the first Requests to 
    crawl for this spider."""

   # @profile
    def parse(self,response):
        urlList=[]
        successfulUrls=list()
        r=[]
        global successfulUrlsList
        newwr = csv.writer(successfulUrlsList, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)    
        counter=response.meta.get('counter')
        urlCnameDict=response.meta.get('resultDict') 
        page = self._get_item(response,counter,urlCnameDict)
        depth = page['depth_level']
        depth_value=depth.get('depth')
        if depth_value:
            page['depth_level'] = depth_value
        else:
            page['depth_level']='0'

        if (((response.status == 200) or (response.status == 403)) and (page['depth_level'] =='0')):
            successfulUrls.append(counter)
            successfulUrls.append(str(response.url))
            newwr.writerow(successfulUrls)
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
        page['ObjectCount'] = 0
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
        urlList.append(page['ObjectCount'])
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
    def _get_item(self, response,counter,urlCnameDict):
        item = Page(url=response.url,content_length=str(len(response.body)),depth_level=response.meta,
            httpResponseStatus=response.status,start_time= datetime.now().time())
        self._set_new_cookies(item,response)
        self._set_DNS_info(item,response,urlCnameDict)
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
        ObjectCount=0
        distinctAsn=set()
        Asnlist=set()
        distinctSecondlevelSites=set()
        uniqueSecondlevelSites=set()
        masterDict=dict()
        list_of_attri=['href','src']
        
        embededSites=set()
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
                for site in siteList:
                    embededSites.add(site)
                ObjectCount = ObjectCount+len(siteList)
                Asnlist,uniqueSecondlevelSites,masterDict= _distinctASN(siteList)
                distinctAsn = distinctAsn | Asnlist
                distinctSecondlevelSites = distinctSecondlevelSites | uniqueSecondlevelSites
                r.extend(Request(site, callback=self.parse,method='HEAD',meta={'resultDict': masterDict,'counter': counterValue,'tagType': str(k),'download_timeout':15})for site in siteList if site.startswith("http://") or site.startswith("https://") or site.startswith("www."))
        if len(embededSites) > 0:
            page['ObjectCount'] = len(embededSites)
        else:
            page['ObjectCount'] = 0

        if len(distinctSecondlevelSites) > 0:
            page['NumberOfuniqueExternalSecondlevelSites'] = len(distinctSecondlevelSites)
        else:
            page['NumberOfuniqueExternalSecondlevelSites'] = 0
        if len(distinctAsn) > 0:
            page['distinctASNs'] = len(distinctAsn)
        else:
            page['distinctASNs'] = 0
        return r   

    def _distinctASN(weblinks):
        destASNValues=set()
        SecondlevelSites=set()
        cNamelist=list()
        dest_ip=list()
        masterDict=dict()

        for site in weblinks:    
            siteinfodict=getCNameIpAsn(site)
            url=urlParseSite(site)
            if siteinfodict is not None and siteinfodict.get(url) is not None:
                sitedict=siteinfodict.get(url)
                cNamelist+=sitedict[0]
                dest_ip+=sitedict[1]
                destASNValues=destASNValues | sitedict[2]
                SecondlevelSites= SecondlevelSites| sitedict[3]
                masterDict[url]=(sitedict[0],sitedict[1],sitedict[2],sitedict[3])
        return (destASNValues,SecondlevelSites,masterDict)

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
    def _set_DNS_info(self, page,response,urlCnameDict):
        CNAMEList = list()
        dest_server_ip=list()
        dest_ASN=set()
        domain = response.url
        global dns_lookup_time
        domain=urlParseSite(domain)
        if urlCnameDict is not None and urlCnameDict.get(domain) is not None:
            resultInfo=urlCnameDict.get(domain)
            CNAMEList=resultInfo[0]
            dest_server_ip=resultInfo[1]
            dest_ASN=resultInfo[2]
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
        else :
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

    def getCNameIpAsn(site):
        CNAMEList = list()
        dest_server_ip=list()
        dest_ASN=set()
        urlCnameDict=dict()
        domain=urlParseSite(site)
        secondlevelDomain=set()
        try:
            myResolver = dns.resolver.Resolver()
            myAnswers = myResolver.query(domain, "A")
            value = str(myAnswers.response).splitlines()
            for item in value:
                if "IN A" in item:
                    ip=str(item.split("IN A")[1])
                    if ip:
                        url=item.split(' ')[0].strip()
                        if url.endswith('.'):
                            url = url[:-1]
                        if not url.startswith('http://'):
                            url="http://"+url
                        CNAMEList.append(item.split(' ')[0].strip())
                        secondlevelDomain.add(str(getsecondleveldomain(url)))
                        dest_server_ip.append(str(ip).strip())
                        gir = pygeoip.GeoIP('GeoIPASNum.dat',flags=pygeoip.const.GEOIP_STANDARD)
                        asNum = gir.asn_by_name(str(ip))
                        if asNum:
                            asNumSplit = asNum.split(' ')
                            asn = ''.join(x for x in asNumSplit[0] if x.isdigit())
                            dest_ASN.add(asn)
        except :
            CNAMEList.append('-')
            dest_server_ip.append('-')
            dest_ASN.add('-')
        urlCnameDict[domain]=(CNAMEList,dest_server_ip,dest_ASN,secondlevelDomain)
        return urlCnameDict

    def urlParseSite(site):
        domain = urlparse(site).netloc
        if domain.startswith('http://'):
            domain = domain.replace("http://", "", 1)
        elif domain.startswith('https://'):
            domain = domain.replace("https://", "", 1)

        if domain.endswith('/'):
            domain = domain[:-1]
        return domain