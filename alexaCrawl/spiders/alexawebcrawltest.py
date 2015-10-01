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
import line_profiler
import Queue
from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
import ast

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
    # global lock
    
    
    
    
    global urllist
    
    global resulturldict

    
    dest_ASN=[]
    # counter=Counter(0)
    tagType='A'
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
            page['tagType']='A'

        r = [page]
        page['InternalImageCount']=0
        page['ExternalImageCount']=0
        page['UniqueExternalSitesForImage']=0

        page['InternalscriptCount']=0
        page['ExternalscriptCount']=0
        page['UniqueExternalSitesForScript']=0

        page['InternallinkCount']=0
        page['ExternallinkCount']=0
        page['UniqueExternalSitesForLink']=0

        page['InternalembededCount']=0
        page['ExternalembededCount']=0
        page['UniqueExternalSitesForEmbeded']=0

        # urlList.append(page['InternallinkCount'])
        # urlList.append(page['ExternallinkCount'])
        # urlList.append(page['UniqueExternalSitesForLink'])

        # urlList.append(page['InternalembededCount'])
        # urlList.append(page['ExternalembededCount'])
        # urlList.append(page['UniqueExternalSitesForEmbeded'])

        r.extend(self._extract_requests(response,counter)) #external site link
        r.extend(self._extract_img_requests(response,tagType,counter,page)) #link to img files
        r.extend(self._extract_script_requests(response,tagType,counter,page)) #link to script files like java script etc
        r.extend(self._extract_external_link_requests(response,tagType,counter,page)) #link to css or any other external linked files
        r.extend(self._extract_embed_requests(response,tagType,counter,page)) #link to addresses of the external file to embed

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
        urlList.append(page['InternalImageCount'])
        urlList.append(page['ExternalImageCount'])
        urlList.append(page['UniqueExternalSitesForImage'])

        urlList.append(page['InternalscriptCount'])
        urlList.append(page['ExternalscriptCount'])
        urlList.append(page['UniqueExternalSitesForScript'])

        urlList.append(page['InternallinkCount'])
        urlList.append(page['ExternallinkCount'])
        urlList.append(page['UniqueExternalSitesForLink'])

        urlList.append(page['InternalembededCount'])
        urlList.append(page['ExternalembededCount'])
        urlList.append(page['UniqueExternalSitesForEmbeded'])

        urlList.append(page['start_time'])
        page['end_time']=datetime.now().time()
        urlList.append(page['end_time'])

        #r.extend(self._extract_script_requests(response,tagType,counter,page))

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
    def _extract_requests(self,response,counter):
        r = []
        counterValue=counter
        if isinstance(response, HtmlResponse):
            links = self.link_extractor.extract_links(response)
            r.extend(Request(x.url, callback=self.parse,method='GET',meta={'counter': counterValue})for x in links if x.url != response.url)
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
    def _extract_img_requests(self,response,tag,counter,page):
        r = []
        siteList=[]
        externalSites=[]
        if isinstance(response, HtmlResponse):
            tag='I'
            counterValueImg=counter
            sites = Selector(response).xpath("//img/@src").extract()
            if len(sites) > 0 :
                for item in sites:
                    if isinstance(item, unicode):
                        item=item.encode('utf-8')
                        siteList.append(item)
                    else:
                        siteList.append(item)
                externalImageCount,InternalImageCount,uniqueExternalSites,externalSites,secondlevelurl =_extract_object_count(siteList)
                Imagecount=len(siteList)

                if InternalImageCount > 0 :
                    page['InternalImageCount']=InternalImageCount
                else:
                    page['InternalImageCount']='0'

                if externalImageCount:
                    page['ExternalImageCount']=externalImageCount
                else:
                    page['ExternalImageCount']='0'
                if uniqueExternalSites:
                    page['UniqueExternalSitesForImage']=uniqueExternalSites
                else:
                    page['UniqueExternalSitesForImage']='0'

                r.extend(Request(site, callback=self.parse,method='HEAD',meta={'tagType': tag,'counter': counterValueImg,'download_timeout':5})for site in siteList if site.startswith("http://") or site.startswith("https://"))
            else :
                page['InternalImageCount']=0
                page['ExternalImageCount']=0
                page['UniqueExternalSitesForImage']=0

        return r
    #@profile
    def _extract_script_requests(self,response,tag,counter,page):
        r=[]
        siteList=[]
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

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            
            if len(sites) > 0:
                for item in sites:
                    if isinstance(item, unicode):
                        item=item.encode('utf-8')
                        siteList.append(item)
                    else:
                        siteList.append(item)

                externalscriptCount,InternalscriptCount,uniqueExternalSites,externalSites,secondlevelurl=_extract_object_count(siteList)
                scriptcount=len(siteList)
                if InternalscriptCount:
                    page['InternalscriptCount']=InternalscriptCount
                else:
                    page['InternalscriptCount']='0'

                if externalscriptCount:
                    page['ExternalscriptCount']=externalscriptCount
                else:
                    page['InternalscriptCount']='0'
                if uniqueExternalSites:
                    page['UniqueExternalSitesForScript']=uniqueExternalSites
                else:
                    page['UniqueExternalSitesForScript']='0'
                # page['InternalscriptCount']=InternalscriptCount
                # page['ExternalscriptCount']=externalscriptCount
                # page['UniqueExternalSitesForScript']=uniqueExternalSites

                #logwr.writerow({'url': response.url, 'counter': counterValueScript,'InternalscriptCount':InternalscriptCount,'ExternalscriptCount':externalscriptCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
                #lock.release()
                r.extend(Request(site, callback=self.parse,method='HEAD',meta={'tagType': tag,'counter': counterValueScript,'download_timeout':5})for site in siteList if site.startswith("http://") or site.startswith("https://"))
            else:
                page['InternalscriptCount']='0'
                page['InternalscriptCount']='0'
                page['UniqueExternalSitesForScript']='0'
        return r
    #@profile
    def _extract_external_link_requests(self,response,tag,counter,page):
        r=[]
        siteList=[]
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

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            if len(sites) >0:
                for item in sites:
                    if isinstance(item, unicode):
                        item=item.encode('utf-8')
                        siteList.append(item)
                    else:
                        siteList.append(item)
                sites.append(counterValueLink)
                externallinkCount,InternallinkCount,uniqueExternalSites,externalSites,secondlevelurl=_extract_object_count(siteList)
                #wr.writerow(siteList)
                linkcount=len(siteList)

                if InternallinkCount:
                    page['InternallinkCount']=InternallinkCount
                else:
                    page['InternallinkCount']='0'

                if externallinkCount:
                    page['ExternallinkCount']=externallinkCount
                else:
                    page['ExternallinkCount']='0'
                if uniqueExternalSites:
                    page['UniqueExternalSitesForLink']=uniqueExternalSites
                else:
                    page['UniqueExternalSitesForLink']='0'

                # page['InternallinkCount']=InternallinkCount
                # page['ExternallinkCount']=externallinkCount
                # page['UniqueExternalSitesForLink']=uniqueExternalSites

               #logwr.writerow({'url': response.url, 'counter': counterValueLink,'InternallinkCount':InternallinkCount,'ExternallinkCount':externallinkCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
                #lock.acquire()
                #lock.release()
                r.extend(Request(site, callback=self.parse,method='HEAD',meta={'tagType': tag,'counter': counterValueLink,'download_timeout':5})for site in siteList if site.startswith("http://") or site.startswith("https://"))
            else:
                page['InternallinkCount']='0'
                page['ExternallinkCount']='0'
                page['UniqueExternalSitesForLink']='0'
        return r
    #@profile
    def _extract_embed_requests(self,response,tag,counter,page):
        r=[]
        siteList=[]
       
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

            #logwr = csv.writer(logFile, delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
            if len(sites) > 0 :
                for item in sites:
                    if isinstance(item, unicode):
                        item=item.encode('utf-8')
                        siteList.append(item)
                    else:
                        siteList.append(item)

                externalembededCount,InternalembededCount,uniqueExternalSites,externalSites,secondlevelurl=_extract_object_count(siteList)
                embededcount=len(siteList)

                if InternalembededCount:
                    page['InternalembededCount']=InternalembededCount
                else:
                    page['InternalembededCount']='0'

                if externalembededCount:
                    page['ExternalembededCount']=externalembededCount
                else:
                    page['ExternalembededCount']='0'
                if uniqueExternalSites:
                    page['UniqueExternalSitesForEmbeded']=uniqueExternalSites
                else:
                    page['UniqueExternalSitesForEmbeded']='0'

                # page['InternalembededCount']=InternalembededCount
                # page['ExternalembededCount']=externalembededCount
                # page['UniqueExternalSitesForEmbeded']=uniqueExternalSites

                #logwr.writerow({'url': response.url, 'counter': counterValueEmded,'InternalembededCount':InternalembededCount,'ExternalembededCount':externalembededCount,'UniqueExternalSites':uniqueExternalSites,'ExternalSites':externalSites,'secondlevelurl':secondlevelurl})
                #lock.acquire()
                #lock.release()
                r.extend(Request(site, callback=self.parse,method='HEAD',meta={'tagType': tag,'counter': counterValueEmded,'download_timeout':5})for site in siteList if site.startswith("http://") or site.startswith("https://"))
            else:
                page['InternalembededCount']='0'
                page['ExternalembededCount']='0'
                page['UniqueExternalSitesForEmbeded']='0'
        return r
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
    #@profile
    def _set_DNS_info(self, page,response):
        # start_time = time.clock()
        CNAMEList=[]
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