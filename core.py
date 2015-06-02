import csv
import os
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log
import alexaCrawl.spiders
from alexaCrawl.spiders.alexawebcrawltest import alexaSpider
from scrapy.utils.project import get_project_settings
from urlparse import urlparse
#import scrapy.crawler
#from scrapy import signals
#import scrapy.statscol
#from scrapy.stats import stats
import multiprocessing as mp
#from multiprocessing import Process, Value,Lock
from multiprocessing import Process, Lock
from multiprocessing.sharedctypes import Value
#import time
import random
import Queue
import socket
import dns.resolver
import dns.name
import dns.message
import dns.query
import dns.flags
import subprocess
import shlex
from datetime import datetime, date, time
import logging
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
#from guppy import hpy


class ReactorControl:
    def __init__(self):
        self.crawlers_running = 0

    def add_crawler(self):
        self.crawlers_running += 1

    def remove_crawler(self):
        #h = hp.heap()
        # print "##########################################"
        # print "size",h
        # print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
        self.crawlers_running -= 1

        if self.crawlers_running == 0 :
            reactor.stop()

reactor_control = ReactorControl()

#hp = hpy()

rowValues=[]
finalItemList=[]
listOfLists=[]

row_no=1
code_chunk=1
listOfLists=[[] for _ in range(10)]
with open('top-1m.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
    #print "soumya"
    for row in spamreader:
        rowValue=', '.join(row)
        rowValues=rowValue.split(",")
       # if len(listOfLists)<10:
        if (code_chunk==row_no):
            #finalItemList.append(rowValues[1])
            
            listOfLists[row_no-1].append(rowValues[1])             
            if (row_no==10):
                row_no=row_no%10
                #row_no=row_no-1
            row_no=row_no+1
            code_chunk=(row_no%1000)
            # if row_no==20:
            #     code_chunk=20
            # else:    
            #     code_chunk=(row_no%20)
            #     continue
                #print "code_chunk",code_chunk    
        else:
            break
#curl http://localhost:6800/schedule.json -d project=alexaCrawl -d spider=alexa -d part=listOfLists[0]
#curl http://localhost:6800/schedule.json -d project=alexaCrawl -d spider=mySpider2 -d part=2[1]
#curl http://localhost:6800/schedule.json -d project=alexaCrawl -d spider=alexa -d setting=DOWNLOAD_DELAY=2 -d arg1=listOfLists[0]
#curl http://localhost:6800/schedule.json -d project=mySpider2 -d spider=alexa -d setting=DOWNLOAD_DELAY=2 -d arg1=listOfLists[1]
#curl http://localhost:6800/schedule.json -d project=myproject -d spider=spider1 -d part=3
#print "listOfLists",listOfLists[0]
#url=listOfLists[0]
url=",".join(listOfLists[0])
#print url

# testList=['google.com']
# urlParse=urlparse('http://www.cwi.nl/%7Eguido/Python.html')
# print "urlParse",urlParse.netloc

# domain = 'www.linkedin.com'
# answers = dns.resolver.query(domain, 'CNAME')
# print ' query qname:', answers.qname, ' num ans.', len(answers)
# for rdata in answers:
#     try:
#         print ' cname target address:', rdata.target
#         while (rdata.target):
#             value=dns.resolver.query(rdata.target, 'CNAME')
#             for rdata in value:
#                 print 'next cname value',rdata
#     except dns.resolver.NXDOMAIN:
#         continue
#     except dns.resolver.Timeout:
#         continue
#     except dns.exception.DNSException:
#         continue
#         #print "Unhandled exception"
#     except dns.resolver.NOAnswer:
#         continue


# for rdata in dns.resolver.query('www.bmw.com', 'CNAME') :
#     print rdata.target


# n = dns.name.from_text(domain)

# depth = 2
# default = dns.resolver.get_default_resolver()
# nameserver = default.nameservers[0]

# last = False
# while not last:
#     s = n.split(depth)

#     last = s[0].to_unicode() == u'@'
#     sub = s[1]

#     print('Looking up %s on %s' % (sub, nameserver))
#     query = dns.message.make_query(sub, dns.rdatatype.NS)
#     response = dns.query.udp(query, nameserver)

#     rcode = response.rcode()
#     if rcode != dns.rcode.NOERROR:
#         if rcode == dns.rcode.NXDOMAIN:
#             raise Exception('%s does not exist.' % sub)
#         else:
#             raise Exception('Error %s' % dns.rcode.to_text(rcode))

#     rrset = None
#     if len(response.authority) > 0:
#         rrset = response.authority[0]
#     else:
#         rrset = response.answer[0]

#     rr = rrset[0]
#     if rr.rdtype == dns.rdatatype.SOA:
#         print('Same server is authoritative for %s' % sub)
#     else:
#         authority = rr.target
#         print('%s is authoritative for %s' % (authority, sub))
#         nameserver = default.query(authority).rrset[0].to_text()

#     depth += 1










# answers = dns.resolver.query('www.bmw.com', 'CNAME')
# print ' query qname:', answers.qname, ' num ans.', len(answers)
# for rdata in answers:
#     print ' cname target address:', rdata.target

# answer = dns.resolver.query(domain, 'CNAME').rrset
# ttl = answer.ttl
# result = answer.items[0].to_text()
# result = result.rstrip('.')
# print ttl, result
def callback(spider, reason):
    #stats = spider.crawler.stats.get_stats()
    #print 
    # stats is a dictionary
    # write stats to the database here
    reactor.stop()

#processes = [mp.Process(target=setup_crawler, args=(5, output)) for x in range(4)]
"""[Author  : Soumya ranjan Parida]
Function : setup_crawler
This function can be used to create multiple spiders
inside single process"""




# def setup_crawler(url):
#     crawler = Crawler(settings)
#     crawler.configure()
#     crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
#     spider = alexaSpider(domain=url)
#     #spider = crawler.spiders.create(spider_name)
#     crawler.crawl(spider)
#     reactor_control.add_crawler()
#     crawler.start()

# reactor_control = ReactorControl()
# log.start()
# settings = get_project_settings()
# crawler = Crawler(settings)

# for spider in crawler.spiders.list():
#     setup_crawler(url)

# reactor.run()

# def setup_crawler(domain):
#     spider = alexaSpider(domain=domain)
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     crawler.signals.connect(callback, signal=signals.spider_closed)
#     crawler.configure()
#     crawler.crawl(spider)
#     crawler.start()
    #crawler.stats.set_value('downloader/my_errs/%s' % request.meta, ex_class)
    #crawler.stats.inc_value('pages_crawled')
    #print "crawler.stats",crawler.stats.get_stats()
 	#crawler.stats.inc_value('pages_crawled')
 	#print "crawler.stats",crawler.stats
    

# for domain in listOfLists:
#   	setup_crawler(domain)
#domain="google.co.in"
#domain="bmw.com"
# # urlParse=urlparse('http://www.cwi.nl/%7Eguido/Python.html')
# # print "urlParse",urlparse(domain).netloc
#setup_crawler(domain)
# log.start()
#reactor.run()

# for domain in listOfLists:
#     for url in domain:
# 	   setup_crawler(url)
# #log.start()
# reactor.run()

def callback(spider, reason):
    stats = spider.crawler.stats.get_stats()
    #print "stats=",stats
    reactor.stop()

def worker(urllist,out_q):
    for url in urllist:
        spider = alexaSpider(domain=url)
        #setup_crawler(url)
        settings = get_project_settings()
        crawler = Crawler(settings)
        #crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
        crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
        crawler.configure()
        crawler.crawl(spider)
        reactor_control.add_crawler()
        crawler.start()

    settings = get_project_settings()
    crawler = Crawler(settings)
    reactor.run()
    return

def multiProc_crawler(domainlist,nprocs):
    
        #out_q.put(outdict)
        #outdict = []
        # for urllist in urllistoflist:
        # 	for url in urllist:
        #     	setup_crawler(url)    
		#out_q.put(outdict)

    # Each process will get 'chunksize' nums and a queue to put his out
    # dict into
    out_q = Queue.Queue()
    #chunksize = int(math.ceil(len(nums) / float(nprocs)))
    procs = []
    # with counter.get_lock():
    #     counter.value += 1
    #counter = Counter(0)
    
    #pool = Pool(processes=nprocs)

    mp.log_to_stderr()
    logger = mp.get_logger()
    logger.setLevel(logging.INFO)

    for i in range(nprocs):
        p = mp.Process(target=worker,
                args=(domainlist[i],out_q)) 
        procs.append(p)
        p.start()
        print "procs",procs
        print "123"

    for job in procs:
        if job.is_alive():
            print 'Terminating process'
            print "jobs",job
        job.join()



    # while len(procs) > 0:
    #     try:
    #         # Join all threads using a timeout so it doesn't block
    #         # Filter out threads which have been joined or are None
    #         procs = [t.join(1) for t in procs if t is not None and t.isAlive()]
    #     except KeyboardInterrupt:
    #         print "Ctrl-c received! Sending kill to threads..."
    #         for t in procs:
    #             t.kill_received = True

    #procs[-1].terminate()

    # for job in procs:
    #     print "join"
    #     print "job",job
    #     print "procs",procs
    #     job.join()

    f = open("output6.csv",'r')
    csv_f=csv.reader(f, delimiter='\t',quotechar=' ')

    checked_list = []
    d = dict() 
    index_set = set()
    for row in csv_f:
        index_set.add(row[0])


    for idx in index_set:
        f.seek(0)
        unique_asn = set()
        for row in csv_f:
            if row[0] == idx:
                if '-' not in row[9]:
                    unique_asn.add(row[9])
        print "ASN number change for index %s is : %s %s" % (str(idx), str(len(unique_asn)),unique_asn)
        d[str(idx)]=str(len(unique_asn))

    field=[]
    with open("output6.csv",'r') as csvinput:
        with open("finalOutput.csv", 'w') as csvoutput:
            writer = csv.writer(csvoutput,delimiter='\t',quotechar=' ')
            reader=csv.reader(csvinput,delimiter='\t',quotechar=' ')
            
            for row in csvinput:    
                field = row.strip().split('\t')
                idx=row[0]
                field.insert(10,d[str(idx)])
                writer.writerow(field)
    csvinput.close()
    csvoutput.close()
    # f.close()
    logList = []
    # with open("log.csv",'r') as loginput:
    #     for key, value in mydict.iteritems(): # Iterate over items returning key, value tuples
    #         logList.append('%s: %s' % (str(key), str(value)))
    # loginput.close()
    
    # print "loglist-",logList
    #with open("log.csv",'r') as loginput:
    with open("finalOutput.csv", 'r') as outputCSV:
        with open("final.csv", 'wbr+') as finaloutput:
            readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')
            writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
            
            for row in outputCSV:   
                TotalObjectCount=TotalInternalObjects=TotalExternalObjects=0
                urlValue=counter=ImageValue=scriptcountnumber=LinkCount=embededcount=[]
                images=scripts=links=embededs=0

                intImages=intScripts=intLinks=intEmbeded=0
                extImages=extScripts=extLinks=extEmbededs=0

                externalImageList=externalembededList=externalscriptList=externallinkList=[]
                internalscriptList=internallinkList=internalImageList=internalembededList=[]

                field = row.strip().split('\t')
                loginput=open("log.csv",'r')
                reader=csv.reader(loginput,delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                
                for rowinput in reader:
                    for value in rowinput:
                        value=value.replace('{','')
                        value=value.replace('}','')
                        element=value.strip().split(',')
                        for key in element:
                            if "url" in key:
                                urlValue=key.strip().split("'url':")
                            if "counter" in key:
                                counter=key.strip().split("'counter':")
                            if "Imagecount" in key:
                                ImageValue=(key.strip().split("'Imagecount':"))                                
                            if "scriptcount" in key:
                                scriptcountnumber=(key.strip().split("'scriptcount':"))                    
                            if "linkcount" in key:
                                LinkCount=(key.strip().split("'linkcount':"))  
                            if "embededcount" in key:
                                embededcount=(key.strip().split("'embededcount':"))                              
                            
                            if "ExternalImageCount" in key:
                                externalImageList=(key.strip().split("'ExternalImageCount':"))
                            if "InternalImageCount" in key:
                                internalImageList=(key.strip().split("'InternalImageCount':"))
                            if "ExternalscriptCount" in key:
                                externalscriptList=(key.strip().split("'ExternalscriptCount':"))
                            if "InternalscriptCount" in key:
                                internalscriptList=(key.strip().split("'InternalscriptCount':"))
                            if "ExternallinkCount" in key:
                                externallinkList=(key.strip().split("'ExternallinkCount':"))
                            if "InternallinkCount" in key:
                                internallinkList=(key.strip().split("'InternallinkCount':"))
                            if "ExternalembededCount" in key:
                                externalembededList=(key.strip().split("'ExternalembededCount':"))
                            if "InternalembededCount" in key:
                                internalembededList=(key.strip().split("'InternalembededCount':"))
                        
                            if len(ImageValue)>0:
                                Imagecount=int(ImageValue[1].strip())
                            else:
                                Imagecount=0
                            if len(scriptcountnumber):
                                scriptcount=int(scriptcountnumber[1].strip())
                            else:
                                scriptcount=0
                            if len(LinkCount):
                                Linkcount=int(LinkCount[1].strip())
                            else:
                                Linkcount=0
                            if len(embededcount):
                                EmbededCount=int(embededcount[1].strip())
                            else:
                                EmbededCount=0
                            
                            if len(externalImageList)>0:
                                extImagecount=int(externalImageList[1].strip())
                            else:
                                extImagecount=0
                            if len(internalImageList):
                                intImagecount=int(internalImageList[1].strip())
                            else:
                                intImageCount=0

                            if len(externalscriptList):
                                extScriptCount=int(externalscriptList[1].strip())
                            else:
                                extScriptCount=0
                            if len(internalscriptList):
                                intScriptCount=int(internalscriptList[1].strip())
                            else:
                                intScriptCount=0

                            if len(externallinkList):
                                extLinkCount=int(externallinkList[1].strip())
                            else:
                                extLinkCount=0
                            if len(internallinkList):
                                intLinkCount=int(internallinkList[1].strip())
                            else:
                                intLinkCount=0

                            if len(externalembededList):
                                extEmbededCount=int(externalembededList[1].strip())
                            else:
                                extEmbededCount=0
                            if len(internalembededList):
                                intEmbededCount=int(internalembededList[1].strip())
                            else:
                                intEmbededCount=0

                        url=urlValue[1].replace("'","").strip()
                        
                        if (field[0] is counter[1].strip() and field[4] == url):
                            images=Imagecount
                            scripts=scriptcount
                            links=Linkcount
                            embededs=EmbededCount

                            intImages=intImagecount
                            intScripts=intScriptCount
                            intLinks=intLinkCount
                            intEmbeded=intEmbededCount

                            extImages=extImagecount
                            extScripts=extScriptCount
                            extLinks=extLinkCount
                            extEmbededs=extEmbededCount
                            
                TotalObjectCount=images+scripts+links+embededs
                TotalInternalObjects=intImages+intScripts+intLinks+intEmbeded
                TotalExternalObjects=extImages+extScripts+extLinks+extEmbededs
                d[TotalInternalObjects]=TotalInternalObjects
                d[TotalExternalObjects]=TotalExternalObjects
                field.insert(11,d[TotalInternalObjects])
                field.insert(12,d[TotalExternalObjects])
                writerOutput.writerow(field)
        f.close()

#print "ASN number change for index %s is : %s" % (str(idx), str(len(unique_asn)))

# [Som] :These lines can be used later for multiprocessing
resultlist=[]
print "listOfLists",listOfLists
multiProc_crawler(listOfLists,2)

#print "resultlist=",resultlist


#cd ../../
#os.chdir("../../")
#print os.getcwd()
#scrapy #crawl alexa -a url_list=google.com,yahoo.com

#scrapy crawl alexa 
#process_List=['alexaSpider'.'mySpider2']
#for process in process_List:



#[Author  : Soumya ranjan Parida]
#Function : setup_crawler
#This function can be used to create multiple spiders
#inside single process
# def setup_crawler(domain):
#     spider = alexaSpider(domain=domain)
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     crawler.configure()
#     crawler.crawl(spider)
#     crawler.start()

# for url_list in listOfLists:
# 	print "url_list",url_list
# 	for domain in url_list:
# 		setup_crawler(domain)
# log.start()
# reactor.run()
