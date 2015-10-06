import csv
import os
import alexaCrawl.spiders
import multiprocessing as mp
import random
import Queue
import time
import logging
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
from datetime import datetime, date,time
from multiprocessing import Process, Value, Lock
from scrapy.contrib.exporter import XmlItemExporter
from alexaCrawl.spiders.alexawebcrawltest import alexaSpider
from scrapy.utils.project import get_project_settings
from urlparse import urlparse
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import cmdline
from scrapy import log
import codecs
import sys


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

class ReactorControl:
    def __init__(self):
        self.crawlers_running = 0

    def add_crawler(self):
        self.crawlers_running += 1

    def remove_crawler(self):
        self.crawlers_running -= 1
        if self.crawlers_running == 0 :
            reactor.stop()

reactor_control = ReactorControl()
counter = Counter(0)
lock=Lock()
rowValues=[]
finalItemList=[]
listOfLists=[]
urlIndexlist=dict()

row_no=1
code_chunk=1
listrange=500
IndexInTop1mFile=list()
IndexNotInResultFile=list()

listOfLists=[[] for _ in range(listrange)]
OutputFile = codecs.open("output6.csv",'wbr+')
urllistFile=open("urllistFile.txt",'wbr+')
wr = csv.writer(OutputFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
#urllistFileWriter= csv.writer(urllistFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)

with open('top-1m.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
    for row in spamreader:
        rowValue=', '.join(row)
        rowValues=rowValue.split(",")
        urlIndexlist[rowValues[1]]=rowValues[0]
        IndexInTop1mFile.append(rowValues[0])
        if (code_chunk==row_no):
            listOfLists[row_no-1].append(rowValues[1])             
            if (row_no==listrange):
                row_no=row_no%listrange
            row_no=row_no+1
            code_chunk=(row_no%10000)
        else:
            break

"""[Author  : Soumya ranjan Parida]
Function : setup_crawler
This function can be used to create multiple spiders
inside single process"""

def worker(urllist,out_q):
    cmdline.execute([
    'scrapy', 'crawl', 'alexa',
    '-a', 'arg1='+str(urllist), '-a', 'arg2='+str(urlIndexlist)])
    return

# def worker(urllist,out_q,i):
#     outdict =    {}
#     files = {}
#     items=[]
#     newUrlList=[]
#     dircount=0
#     def add_item(item):
#         urlList=[]
#         urlList.append(item['index'])
#         urlList.append(item['depth_level'])
#         urlList.append(item['httpResponseStatus'])
#         urlList.append(item['content_length'])
#         urlList.append(item['url'].strip())
#         cookieStr=';'.join(item['newcookies'])
#         urlList.append(cookieStr.strip())
#         urlList.append(item['tagType'])
#         cname=';'.join(item['CNAMEChain'])
#         urlList.append(cname)
#         urlList.append(item['destIP'])
#         urlList.append(item['ASN_Number'])
#         urlList.append(item['start_time'])
#         urlList.append(item['end_time'])

#         urlList.append(item['InternalImageCount'])
#         urlList.append(item['ExternalImageCount'])
#         urlList.append(item['UniqueExternalSitesForImage'])

#         urlList.append(item['InternalscriptCount'])
#         urlList.append(item['ExternalscriptCount'])
#         urlList.append(item['UniqueExternalSitesForScript'])

#         urlList.append(item['InternallinkCount'])
#         urlList.append(item['ExternallinkCount'])
#         urlList.append(item['UniqueExternalSitesForLink'])

#         urlList.append(item['InternalembededCount'])
#         urlList.append(item['ExternalembededCount'])
#         urlList.append(item['UniqueExternalSitesForEmbeded'])

#         items.append(urlList)
    
#     # spider = alexaSpider(domain=urllist,counter=urlIndexlist,outputfileIndex=i,spider_queue=out_q)
#     cmdline.execute([
#     'scrapy', 'crawl', 'alexa',
#     '-a', 'arg1='+str(urllist), '-a', 'arg2='+str(urlIndexlist),'-o','item'+str(i)+'.csv'])
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     crawler.signals.connect(add_item, signals.item_passed)
#     crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
#     crawler.configure()
#     crawler.crawl(spider)
#     jobdir='alexa-1'+str(i)+str(dircount)
#     crawler.settings.set('JOBDIR',jobdir)
#     reactor_control.add_crawler()
#     crawler.start()
    
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     reactor.run()

#     for value in items:
#         outputList=[]
#         for item in value:
#             if isinstance(item, unicode):
#                 item=item.encode('utf-8')
#                 outputList.append(item)
#             elif isinstance(item,str):
#                 item=item
#                 outputList.append(item)
#             else:
#                 item=item
#                 outputList.append(item)
#         wr.writerow(outputList)
#     return

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

#@profile
def multiProc_crawler(domainlist,nprocs):
    out_q = Queue.Queue()
    finalresult=[]
    procs = []
    # for i in xrange(nprocs):           
    #     p = mp.Process(target=worker,
    #             args=(domainlist[i],out_q,i))
    #     procs.append(p)
    #     p.start()

    # for i in xrange(nprocs):
    chunks=[domainlist[x:x+nprocs] for x in xrange(0, len(domainlist), nprocs)]
    print "===================================="
    print "range=",len(chunks)
    for j in xrange(len(chunks)):
        urllistFile.write("\n"+",".join(chunks[j]))
        # urllistFile.write(chunks[j])
        p = mp.Process(target=worker,
                args=(chunks[j],out_q))
        procs.append(p)
        p.start()
    for job in procs:
        urllistFile.write("Done"+",".join(chunks[j]))
        job.join()


    maxInt = sys.maxsize
    decrement = True

    while decrement:
        # decrease the maxInt value by factor 10 
        # as long as the OverflowError occurs.

        decrement = False
        try:
            csv.field_size_limit(maxInt)
        except OverflowError:
            maxInt = int(maxInt/10)
            decrement = True
    
#[Som] :These lines can be used later for multiprocessing
resultlist=[]

for item in listOfLists:
    urllistFile.write(",".join(item))
    multiProc_crawler(item,listrange)
logFile = open("output6.csv",'r')
logwr = csv.reader(logFile,skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
wordcount=list()
UrlNotInResultFile=list()
for line in logwr:
    if line[0] not in wordcount:
        wordcount.append(line[0])
IndexNotInResultFile=list(set(IndexInTop1mFile) - set(wordcount))
def get_key_from_value(my_dict, v):
    for key,value in my_dict.items():
        if value == v:
            return key
    return None
for item in IndexNotInResultFile:
    UrlNotInResultFile.append(get_key_from_value(urlIndexlist,item))
listrangeNew=1
listOfListsNew=[]
urllistFileWriter.writerow(UrlNotInResultFile)
listOfListsNew.append(UrlNotInResultFile)
if len(UrlNotInResultFile) >10 :
    multiProc_crawler(listOfListsNew,10)
else :
    multiProc_crawler(listOfListsNew,1)
logFile.close()
urllistFile.close()
