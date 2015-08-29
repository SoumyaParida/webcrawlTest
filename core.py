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
listrange=50
IndexInTop1mFile=list()
IndexNotInResultFile=list()

listOfLists=[[] for _ in range(listrange)]
OutputFile = codecs.open("output6.csv",'wbr+')
wr = csv.writer(OutputFile, skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)

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
    
# def worker(urllist,out_q,i):
#     outdict = {}
#     files = {}
#     items=[]
#     for url in urllist:
#         spider = alexaSpider(domain=url,counter=urlIndexlist.get(url),outputfileIndex=i,spider_queue=out_q)
#         print spider.
#         settings = get_project_settings()
#         crawler = Crawler(settings)
#         crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
#         crawler.configure()
#         crawler.crawl(spider)
#         reactor_control.add_crawler()
#         crawler.start()
# global items
# items=[]
# def add_item(item):
#     items.append(item)
#     print items



def worker(urllist,out_q,i):
    outdict =    {}
    files = {}
    items=[]
    newUrlList=[]
    def add_item(item):
        urlList=[]
        urlList.append(item['index'])
        urlList.append(item['depth_level'])
        urlList.append(item['httpResponseStatus'])
        urlList.append(item['content_length'])
        urlList.append(item['url'].strip())
        cookieStr=';'.join(item['newcookies'])
        urlList.append(cookieStr.strip())
        urlList.append(item['tagType'])
        cname=';'.join(item['CNAMEChain'])
        urlList.append(cname)
        urlList.append(item['destIP'])
        urlList.append(item['ASN_Number'])
        urlList.append(item['start_time'])
        urlList.append(item['end_time'])
        items.append(urlList)
    # for url in urllist:
    #     spider = alexaSpider(domain=url,counter=urlIndexlist.get(url),outputfileIndex=i,spider_queue=out_q)
    #     settings = get_project_settings()
    #     crawler = Crawler(settings)
    #     crawler.signals.connect(add_item, signals.item_passed)
    #     crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
    #     crawler.configure()
    #     crawler.crawl(spider)
    #     reactor_control.add_crawler()
    #     crawler.start()
    spider = alexaSpider(domain=urllist,counter=urlIndexlist,outputfileIndex=i,spider_queue=out_q)
    settings = get_project_settings()
    crawler = Crawler(settings)
    crawler.signals.connect(add_item, signals.item_passed)
    crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
    crawler.configure()
    crawler.crawl(spider)
    reactor_control.add_crawler()
    crawler.start()

    settings = get_project_settings()
    crawler = Crawler(settings)
    reactor.run()

    for value in items:
        outputList=[]
        for item in value:
            if isinstance(item, unicode):
                item=item.encode('utf-8')
                outputList.append(item)
            elif isinstance(item,str):
                item=item
                outputList.append(item)
            else:
                item=item
                outputList.append(item)
        wr.writerow(outputList)
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

    def merge_dicts(*dict_args):
        '''
        Given any number of dicts, shallow copy and merge into a new dict,
        precedence goes to key value pairs in latter dicts.
        '''
        result = {}
        print dict_args
        for dictionary in dict_args:
            result.update(dictionary)
        return result

    reader = csv.reader(open("output6.csv", "rbU"), delimiter='\t',quotechar=' ')
    writer = csv.writer(open("output.csv", 'w'), delimiter='\t',quotechar=' ')

    for line in reader:
        fields=len(line)
        if fields == 12:
            writer.writerow(line)

    # reader.close()
    # writer.close()

    logreader = csv.reader(open("log.csv", "rbU"))
    logwriter = csv.writer(open("logoutput.csv", 'w'))

    for line in logreader:
        fields=len(line)
        if fields == 7:
            logwriter.writerow(line)

    # reader.close()
    # writer.close()

    csvinput = open("output.csv",'r')
    csv_f=csv.reader(csvinput, delimiter='\t',quotechar=' ')
    # csvinout=open("Output.csv", 'w')
    # csvinout_writer = csv.writer(csvinout,delimiter='\t',quotechar=' ')
    csvoutput=open("finalOutput.csv", 'w')
    writer = csv.writer(csvoutput,delimiter='\t',quotechar=' ')

    # with open("output.csv",'r') as inputfile:
    #   for row in inputfile:
    #       print row
    #         for row in inputfile:
    #   fieldsValue=len(value)
    #   print fieldsValue


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
        
        with open("output.csv",'r') as inputfile:
            for row in inputfile:
                field = row.strip().split('\t')
                if field[0]==idx:
                    field.insert(10,str(len(unique_asn)))
                    writer.writerow(field)
    csvinput.close()
    csvoutput.close()

    csvfile=open('logoutput.csv')
    fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount','UniqueExternalSites','ExternalSites','secondlevelurl']
    Dictreader = csv.DictReader(csvfile,fieldnames=fieldnames)

    outputCSV=open("finalOutput.csv", 'r')
    readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')

    finaloutput=open("final.csv", 'wbr+')
    writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
    keyValuePairLine = { }
    keyValuePairLineNew= { }
    for row in Dictreader:
        key=str(row['counter']+row['url'])
        keyValuePairLine.setdefault(key, []).append(row)
    #print keyValuePairLine

    for key,value in keyValuePairLine.items():
        for Valuedict in value:
            empty_keys = [k for k,v in Valuedict.iteritems() if not v or v=='[]']
            for k in empty_keys:
                del Valuedict[k]

    for key,value in keyValuePairLine.items():
        if value[0] in value and value[1] in value and value[2] in value and value[3] in value:
            value1=merge_dicts(value[0],value[1],value[2],value[3])
            keyValuePairLineNew[key]=value1
        else:
            continue

    for key,value in keyValuePairLine.items():
            value1=merge_dicts(value[0],value[1],value[2],value[3])
            print value1
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

        # if job.is_alive():
        #     print 'Terminating process'
        #     print "jobs",job
        
    # while job:
    #     finalresult = out_q.get()
    #     print 'Result:', finalresult
    #     num_jobs -= 1

    
    
    #wr.writerow(items)
    #response = out_q.get()
    # print "response",response
    #results_queue.put(result)
    #results_queue.put(result)

    # csvinput = open("output6.csv",'r')
    # csv_f=csv.reader(csvinput, delimiter='\t',quotechar=' ')
    # csvoutput=open("finalOutput.csv", 'w')
    # writer = csv.writer(csvoutput,delimiter='\t',quotechar=' ')

    # checked_list = []
    # d = dict() 
    # index_set = set()
    # for row in csv_f:
    #     index_set.add(row[0])


    # for idx in index_set:
    #     csvinput.seek(0)
    #     unique_asn = set()
    #     for row in csv_f:
    #         if row[0] == idx:
    #             #print "row",row[4]
    #             if '-' not in row[9]:
    #                 try:
    #                     if ';' in row[9]:
    #                         rowASN=row[9].split(";")
    #                         for asn in rowASN:
    #                             unique_asn.add(asn)
    #                     else:
    #                         unique_asn.add(row[9])
    #                 except:
    #                     print "row=row"
    #     #print "ASN number change for index %s is : %s %s" % (str(idx), str(len(unique_asn)),unique_asn)    
        
    #     with open("output6.csv",'r') as inputfile:
    #         for row in inputfile:
    #             field = row.strip().split('\t')
    #             if field[0]==idx:
    #                 field.insert(10,str(len(unique_asn)))
    #                 writer.writerow(field)
    # csvinput.close()
    # csvoutput.close()

    # csvfile=open('log.csv')
    # fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount','UniqueExternalSites','ExternalSites']
    # reader = csv.DictReader(csvfile,fieldnames=fieldnames)

    # outputCSV=open("finalOutput.csv", 'r')
    # readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')

    # finaloutput=open("final.csv", 'wbr+')
    # writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
    # keyValuePairLine = { }
    # keyValuePairLineNew= { }
    # for row in reader:
    #     key=str(row['counter']+row['url'])
    #     keyValuePairLine.setdefault(key, []).append(row)
    # #print keyValuePairLine

    # for key,value in keyValuePairLine.items():
    #     for Valuedict in value:
    #         empty_keys = [k for k,v in Valuedict.iteritems() if not v or v=='[]']
    #         for k in empty_keys:
    #             del Valuedict[k]

    # for key,value in keyValuePairLine.items():
    #     value1=merge_dicts(value[0],value[1],value[2],value[3]) 
    #     keyValuePairLineNew[key]=value1


    # for row in outputCSV:
    #     TotalInternalObjects=TotalExternalObjects=0
    #     count=0
    #     field = row.strip().split('\t')
    #     keyValuepaircheck=field[0]+field[4]
    #     if keyValuepaircheck in keyValuePairLineNew:
    #         value=keyValuePairLineNew[keyValuepaircheck]
    #         TotalExternalObjects= int(value['ExternalImageCount'])+int(value['ExternalscriptCount'])+int(value['ExternallinkCount'])+int(value['ExternalembededCount'])
    #         TotalInternalObjects=int(value['InternalImageCount'])+int(value['InternalscriptCount'])+int(value['InternallinkCount'])+int(value['InternalembededCount'])
    #     else:
    #         TotalExternalObjects=0
    #         TotalInternalObjects=0

    #     if keyValuepaircheck in keyValuePairLine:
    #         value=keyValuePairLine[keyValuepaircheck]
    #         for item in value:
    #             count=count+int(item['UniqueExternalSites'])
    #     field.insert(11,TotalExternalObjects)
    #     field.insert(12,TotalInternalObjects)
    #     field.insert(13,count)
    #     writerOutput.writerow(field)

#[Som] :These lines can be used later for multiprocessing
resultlist=[]
multiProc_crawler(listOfLists,listrange)
print "********************************************"
logFile = open("output6.csv",'r')
logwr = csv.reader(logFile,skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
wordcount=list()
UrlNotInResultFile=list()
for line in logwr:
    if line[0] not in wordcount:
        wordcount.append(line[0])
print len(wordcount)
IndexNotInResultFile=list(set(IndexInTop1mFile) - set(wordcount))
print "IndexNotInResultFile",IndexNotInResultFile
def get_key_from_value(my_dict, v):
    for key,value in my_dict.items():
        if value == v:
            return key
    return None
# with open('top-1m.csv') as csvfile:
#     spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
#     for row in spamreader:
#         rowValue=', '.join(row)
#         rowValues=rowValue.split(",")
#         if rowValues[0] is in IndexNotInResultFile:
#             UrlNotInResultFile.append(rowValues[1])
for item in IndexNotInResultFile:
    UrlNotInResultFile.append(get_key_from_value(urlIndexlist,item))
print UrlNotInResultFile
listrangeNew=1
listOfListsNew=[]
listOfListsNew.append(UrlNotInResultFile)
print "listOfListsNew",listOfListsNew
print "length",len(UrlNotInResultFile)
if len(UrlNotInResultFile) >0 :
    multiProc_crawler(listOfListsNew,1)
logFile.close()