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
from scrapy import log

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
items = []

row_no=1
code_chunk=1
listrange=200

listOfLists=[[] for _ in range(listrange)]


with open('top-1m.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
    for row in spamreader:
        rowValue=', '.join(row)
        rowValues=rowValue.split(",")
        urlIndexlist[rowValues[1]]=rowValues[0]
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

# def worker(urllist,out_q,i):
#     # outdict = {}
#     # files = {}
#     # items=[]
#     # for url in urllist:
#     spider = alexaSpider(domainlist=urllist,outputfileIndex=i)
#     out_q.put(spider)
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
#     crawler.configure()
#     crawler.crawl(spider)
#     reactor_control.add_crawler()
#     crawler.start()
    
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     reactor.run()
#     return out_q
    
def worker(urllist,out_q,i):
    outdict = {}
    files = {}
    items=[]
    for url in urllist:
        spider = alexaSpider(domain=url,counter=urlIndexlist.get(url),outputfileIndex=i,spider_queue=out_q)
        out_q.put(spider)
        settings = get_project_settings()
        crawler = Crawler(settings)
        crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
        crawler.configure()
        crawler.crawl(spider)
        reactor_control.add_crawler()
        crawler.start()
    
    settings = get_project_settings()
    crawler = Crawler(settings)
    reactor.run()
    return

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
    for i in xrange(nprocs):
        p = mp.Process(target=worker,
                args=(domainlist[i],out_q,i)) 
        procs.append(p)
        p.start()

    for job in procs:
        job.join()

        # if job.is_alive():
        #     print 'Terminating process'
        #     print "jobs",job
        
    # while job:
    #     finalresult = out_q.get()
    #     print 'Result:', finalresult
    #     num_jobs -= 1


    #response = out_q.get()
    # print "response",response
    #results_queue.put(result)
    #results_queue.put(result)
    csvinput = open("output6.csv",'r')
    csv_f=csv.reader(csvinput, delimiter='\t',quotechar=' ')
    csvoutput=open("finalOutput.csv", 'w')
    writer = csv.writer(csvoutput,delimiter='\t',quotechar=' ')

    checked_list = []
    d = dict() 
    index_set = set()
    for row in csv_f:
        index_set.add(row[0])


    for idx in index_set:
        csvinput.seek(0)
        unique_asn = set()
        for row in csv_f:
            if row[0] == idx:
                #print "row",row[4]
                if '-' not in row[9]:
                    try:
                        if ';' in row[9]:
                            rowASN=row[9].split(";")
                            for asn in rowASN:
                                unique_asn.add(asn)
                        else:
                            unique_asn.add(row[9])
                    except:
                        print "row=row"
        #print "ASN number change for index %s is : %s %s" % (str(idx), str(len(unique_asn)),unique_asn)    
        
        with open("output6.csv",'r') as inputfile:
            for row in inputfile:
                field = row.strip().split('\t')
                if field[0]==idx:
                    field.insert(10,str(len(unique_asn)))
                    writer.writerow(field)
    csvinput.close()
    csvoutput.close()

    csvfile=open('log.csv')
    fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount','UniqueExternalSites','ExternalSites']
    reader = csv.DictReader(csvfile,fieldnames=fieldnames)

    outputCSV=open("finalOutput.csv", 'r')
    readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')

    finaloutput=open("final.csv", 'wbr+')
    writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
    keyValuePairLine = { }
    keyValuePairLineNew= { }
    for row in reader:
        key=str(row['counter']+row['url'])
        keyValuePairLine.setdefault(key, []).append(row)
    #print keyValuePairLine

    for key,value in keyValuePairLine.items():
        for Valuedict in value:
            empty_keys = [k for k,v in Valuedict.iteritems() if not v or v=='[]']
            for k in empty_keys:
                del Valuedict[k]

    for key,value in keyValuePairLine.items():
        value1=merge_dicts(value[0],value[1],value[2],value[3]) 
        keyValuePairLineNew[key]=value1


    for row in outputCSV:
        TotalInternalObjects=TotalExternalObjects=0
        count=0
        field = row.strip().split('\t')
        keyValuepaircheck=field[0]+field[4]
        if keyValuepaircheck in keyValuePairLineNew:
            value=keyValuePairLineNew[keyValuepaircheck]
            TotalExternalObjects= int(value['ExternalImageCount'])+int(value['ExternalscriptCount'])+int(value['ExternallinkCount'])+int(value['ExternalembededCount'])
            TotalInternalObjects=int(value['InternalImageCount'])+int(value['InternalscriptCount'])+int(value['InternallinkCount'])+int(value['InternalembededCount'])
        else:
            TotalExternalObjects=0
            TotalInternalObjects=0

        if keyValuepaircheck in keyValuePairLine:
            value=keyValuePairLine[keyValuepaircheck]
            for item in value:
                count=count+int(item['UniqueExternalSites'])
        field.insert(11,TotalExternalObjects)
        field.insert(12,TotalInternalObjects)
        field.insert(13,count)
        writerOutput.writerow(field)

#[Som] :These lines can be used later for multiprocessing
resultlist=[]
print "listOfLists",listOfLists
multiProc_crawler(listOfLists,listrange)