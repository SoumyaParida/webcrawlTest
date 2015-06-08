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
    for row in spamreader:
        rowValue=', '.join(row)
        rowValues=rowValue.split(",")
        if (code_chunk==row_no):
            listOfLists[row_no-1].append(rowValues[1])             
            if (row_no==10):
                row_no=row_no%10
            row_no=row_no+1
            code_chunk=(row_no%1000)
        else:
            break
"""[Author  : Soumya ranjan Parida]
Function : setup_crawler
This function can be used to create multiple spiders
inside single process"""
def worker(urllist,out_q):
    outdict = {}
    for url in urllist:
        spider = alexaSpider(domain=url)
        settings = get_project_settings()
        crawler = Crawler(settings)
        #crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
        crawler.signals.connect(reactor_control.remove_crawler, signal=signals.spider_closed)
        crawler.configure()
        crawler.crawl(spider)
        reactor_control.add_crawler()
        crawler.start()
        # out_q.put(spider)
        # i=i+1

    settings = get_project_settings()
    crawler = Crawler(settings)
    reactor.run()
    return

def multiProc_crawler(domainlist,nprocs):
    out_q = Queue.Queue()
    procs = []
    for i in range(len(listOfLists)):
        p = mp.Process(target=worker,
                args=(domainlist[i],out_q)) 
        procs.append(p)
        p.start()

    for job in procs:
        if job.is_alive():
            print 'Terminating process'
            print "jobs",job
        job.join()

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
        print "ASN number change for index %s is : %s %s" % (str(idx), str(len(unique_asn)),unique_asn)    
        
        with open("output6.csv",'r') as inputfile:
            for row in inputfile:
                field = row.strip().split('\t')
                if field[0]==idx:
                    field.insert(10,str(len(unique_asn)))
                    writer.writerow(field)
    csvinput.close()
    csvoutput.close()
    logList = []
    with open("finalOutput.csv", 'r') as outputCSV:
        with open("final.csv", 'wbr+') as finaloutput:
            # loginput=open("log.csv",'r')
            readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')
            writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
            csvfile=open('log.csv')
            fieldnames = ['url', 'counter','ExternalImageCount','InternalImageCount','ExternalscriptCount','InternalscriptCount','ExternallinkCount','InternallinkCount','ExternalembededCount','InternalembededCount']
            reader = csv.DictReader(csvfile,fieldnames=fieldnames)

            TotalExObj=TotalIntObj=0
            for row in outputCSV:
                field = row.strip().split('\t')
                #print "field",field
                csvfile.seek(0)
                for rowValue in reader:     
                    if rowValue['counter'] == field[0] and rowValue['url']==field[4]:
                        extImages=rowValue['ExternalImageCount']
                        intImages=rowValue['InternalImageCount']
                        extScripts=rowValue['ExternalscriptCount']
                        intScripts=rowValue['InternalscriptCount']
                        extLinks=rowValue['ExternallinkCount']
                        intLinks=rowValue['InternallinkCount']
                        extEmbededs=rowValue['ExternalembededCount']
                        intEmbeded=rowValue['InternalembededCount']
                        #print "rowValue",rowValue
                        TotalExternalObjects= extImages+extScripts+extLinks+extEmbededs
                        TotalExObj=TotalExObj+int(TotalExternalObjects)
                        TotalInternalObjects=intImages+intScripts+intLinks+intEmbeded
                        TotalIntObj=TotalIntObj+int(TotalInternalObjects)
                field.insert(1,TotalExObj)
                field.insert(2,TotalIntObj)
                writerOutput.writerow(field)
    # with open("finalOutput.csv", 'r') as outputCSV:
    #     with open("final.csv", 'wbr+') as finaloutput:
    #         readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')
    #         writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
            
    #         for row in outputCSV:   
    #             TotalObjectCount=TotalInternalObjects=TotalExternalObjects=0
    #             urlValue=counter=ImageValue=scriptcountnumber=LinkCount=embededcount=[]
    #             images=scripts=links=embededs=0

    #             intImages=intScripts=intLinks=intEmbeded=0
    #             extImages=extScripts=extLinks=extEmbededs=0

    #             externalImageList=externalembededList=externalscriptList=externallinkList=[]
    #             internalscriptList=internallinkList=internalImageList=internalembededList=[]

    #             field = row.strip().split('\t')
                
    #             loginput=open("log.csv",'r')
    #             reader=csv.reader(loginput,delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                
    #             for rowinput in reader:
    #                 for value in rowinput:
    #                     value=value.replace('{','')
    #                     value=value.replace('}','')
    #                     element=value.strip().split(',')
    #                     for key in element:
    #                         if "url" in key:
    #                             urlValue=key.strip().split("'url':")
    #                         if "counter" in key:
    #                             counter=key.strip().split("'counter':")                            
                            
    #                         if "ExternalImageCount" in key:
    #                             externalImageList=(key.strip().split("'ExternalImageCount':"))
    #                         if "InternalImageCount" in key:
    #                             internalImageList=(key.strip().split("'InternalImageCount':"))                        
    #                         if "ExternalscriptCount" in key:
    #                             externalscriptList=(key.strip().split("'ExternalscriptCount':"))                        
    #                         if "InternalscriptCount" in key:
    #                             internalscriptList=(key.strip().split("'InternalscriptCount':"))
    #                         if "ExternallinkCount" in key:
    #                             externallinkList=(key.strip().split("'ExternallinkCount':"))
    #                         if "InternallinkCount" in key:
    #                             internallinkList=(key.strip().split("'InternallinkCount':"))
    #                         if "ExternalembededCount" in key:
    #                             externalembededList=(key.strip().split("'ExternalembededCount':"))
    #                         if "InternalembededCount" in key:
    #                             internalembededList=(key.strip().split("'InternalembededCount':"))
    #                         if len(externalImageList)>0:
    #                             extImagecount=int(externalImageList[1].strip())
    #                         else:
    #                             extImagecount=0
    #                         if len(internalImageList)>0:
    #                             intImagecount=int(internalImageList[1].strip())
    #                         else:
    #                             intImageCount=0

    #                         if len(externalscriptList)>0:
    #                             extScriptCount=int(externalscriptList[1].strip())
    #                         else:
    #                             extScriptCount=0
    #                         if len(internalscriptList)>0:
    #                             intScriptCount=int(internalscriptList[1].strip())
    #                         else:
    #                             intScriptCount=0

    #                         if len(externallinkList)>0:
    #                             extLinkCount=int(externallinkList[1].strip())
    #                         else:
    #                             extLinkCount=0
    #                         if len(internallinkList)>0:
    #                             intLinkCount=int(internallinkList[1].strip())
    #                         else:
    #                             intLinkCount=0

    #                         if len(externalembededList)>0:
    #                             extEmbededCount=int(externalembededList[1].strip())
    #                         else:
    #                             extEmbededCount=0
    #                         if len(internalembededList)>0:
    #                             intEmbededCount=int(internalembededList[1].strip())
    #                         else:
    #                             intEmbededCount=0

    #                     url=urlValue[1].replace("'","").strip()
                        
    #                     if (field[0] is counter[1].strip() and field[4] == url):
    #                         intImages=intImagecount
    #                         intScripts=intScriptCount
    #                         intLinks=intLinkCount
    #                         intEmbeded=intEmbededCount

    #                         extImages=extImagecount
    #                         extScripts=extScriptCount
    #                         extLinks=extLinkCount
    #                         extEmbededs=extEmbededCount

    #             TotalInternalObjects=intImages+intScripts+intLinks+intEmbeded
    #             TotalExternalObjects=extImages+extScripts+extLinks+extEmbededs
    #             d[TotalInternalObjects]=TotalInternalObjects
    #             d[TotalExternalObjects]=TotalExternalObjects
    #             field.insert(11,d[TotalInternalObjects])
    #             field.insert(12,d[TotalExternalObjects])
    #             writerOutput.writerow(field)
    #         f.close()

#[Som] :These lines can be used later for multiprocessing
resultlist=[]
multiProc_crawler(listOfLists,2)


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

    # f = open("output6.csv",'r')
    # csv_f=csv.reader(f, delimiter='\t',quotechar=' ')

    # checked_list = []
    # d = dict() 
    # index_set = set()
    # for row in csv_f:
    #     index_set.add(row[0])


    # for idx in index_set:
    #     f.seek(0)
    #     unique_asn = set()
    #     for row in csv_f:
    #         if row[0] == idx:
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
    #     print "ASN number change for index %s is : %s %s" % (str(idx), str(len(unique_asn)),unique_asn)
    #     d[str(idx)]=str(len(unique_asn))

    # field=[]
    # with open("output6.csv",'r') as csvinput:
    #     with open("finalOutput.csv", 'w') as csvoutput:
    #         writer = csv.writer(csvoutput,delimiter='\t',quotechar=' ')
    #         reader=csv.reader(csvinput,delimiter='\t',quotechar=' ')
            
    #         for row in csvinput:    
    #             field = row.strip().split('\t')
    #             idx=row[0]
    #             field.insert(10,d[str(idx)])
    #             writer.writerow(field)
    # csvinput.close()
    # csvoutput.close()
    # # f.close()
    # logList = []
    # # with open("log.csv",'r') as loginput:
    # #     for key, value in mydict.iteritems(): # Iterate over items returning key, value tuples
    # #         logList.append('%s: %s' % (str(key), str(value)))
    # # loginput.close()
    
    # # print "loglist-",logList
    # #with open("log.csv",'r') as loginput:
    # with open("finalOutput.csv", 'r') as outputCSV:
    #     with open("final.csv", 'wbr+') as finaloutput:
    #         readerCSV = csv.reader(outputCSV,delimiter='\t',quotechar=' ')
    #         writerOutput = csv.writer(finaloutput,delimiter='\t',quotechar=' ',quoting=csv.QUOTE_MINIMAL)
            
    #         for row in outputCSV:   
    #             TotalObjectCount=TotalInternalObjects=TotalExternalObjects=0
    #             urlValue=counter=ImageValue=scriptcountnumber=LinkCount=embededcount=[]
    #             images=scripts=links=embededs=0

    #             intImages=intScripts=intLinks=intEmbeded=0
    #             extImages=extScripts=extLinks=extEmbededs=0

    #             externalImageList=externalembededList=externalscriptList=externallinkList=[]
    #             internalscriptList=internallinkList=internalImageList=internalembededList=[]

    #             field = row.strip().split('\t')
                
    #             loginput=open("log.csv",'r')
    #             reader=csv.reader(loginput,delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                
    #             for rowinput in reader:
    #                 for value in rowinput:
    #                     value=value.replace('{','')
    #                     value=value.replace('}','')
    #                     element=value.strip().split(',')
    #                     for key in element:
    #                         if "url" in key:
    #                             urlValue=key.strip().split("'url':")
    #                         if "counter" in key:
    #                             counter=key.strip().split("'counter':")
    #                         # if "Imagecount" in key:
    #                         #     ImageValue=(key.strip().split("'Imagecount':"))                                
    #                         # if "scriptcount" in key:
    #                         #     scriptcountnumber=(key.strip().split("'scriptcount':"))                    
    #                         # if "linkcount" in key:
    #                         #     LinkCount=(key.strip().split("'linkcount':"))  
    #                         # if "embededcount" in key:
    #                         #     embededcount=(key.strip().split("'embededcount':"))                              
                            
    #                         if "ExternalImageCount" in key:
    #                             externalImageList=(key.strip().split("'ExternalImageCount':"))
    #                         print "externalImageList",externalImageList

    #                         if "InternalImageCount" in key:
    #                             internalImageList=(key.strip().split("'InternalImageCount':"))
    #                         print "internalImageList",internalImageList 
                            
    #                         if "ExternalscriptCount" in key:
    #                             externalscriptList=(key.strip().split("'ExternalscriptCount':"))
    #                         print "externalscriptList",externalscriptList
                            
    #                         if "InternalscriptCount" in key:
    #                             internalscriptList=(key.strip().split("'InternalscriptCount':"))
    #                         print "internalscriptList",internalscriptList
                            
    #                         if "ExternallinkCount" in key:
    #                             externallinkList=(key.strip().split("'ExternallinkCount':"))
    #                         print "externallinkList",externallinkList
                            
    #                         if "InternallinkCount" in key:
    #                             internallinkList=(key.strip().split("'InternallinkCount':"))
    #                         print "internallinkList",internallinkList
                            
    #                         if "ExternalembededCount" in key:
    #                             externalembededList=(key.strip().split("'ExternalembededCount':"))
    #                         print "externalembededList",externalembededList
                            
    #                         if "InternalembededCount" in key:
    #                             internalembededList=(key.strip().split("'InternalembededCount':"))
    #                         print "internalembededList",internalembededList
                        
    #                         # if len(ImageValue)>0:
    #                         #     Imagecount=int(ImageValue[1].strip())
    #                         # else:
    #                         #     Imagecount=0
    #                         # if len(scriptcountnumber)>0:
    #                         #     scriptcount=int(scriptcountnumber[1].strip())
    #                         # else:
    #                         #     scriptcount=0
    #                         # if len(LinkCount)>0:
    #                         #     Linkcount=int(LinkCount[1].strip())
    #                         # else:
    #                         #     Linkcount=0
    #                         # if len(embededcount)>0:
    #                         #     EmbededCount=int(embededcount[1].strip())
    #                         # else:
    #                         #     EmbededCount=0
                            
    #                         if len(externalImageList)>0:
    #                             extImagecount=int(externalImageList[1].strip())
    #                         else:
    #                             extImagecount=0
    #                         if len(internalImageList)>0:
    #                             intImagecount=int(internalImageList[1].strip())
    #                         else:
    #                             intImageCount=0

    #                         if len(externalscriptList)>0:
    #                             extScriptCount=int(externalscriptList[1].strip())
    #                         else:
    #                             extScriptCount=0
    #                         if len(internalscriptList)>0:
    #                             intScriptCount=int(internalscriptList[1].strip())
    #                         else:
    #                             intScriptCount=0

    #                         if len(externallinkList)>0:
    #                             extLinkCount=int(externallinkList[1].strip())
    #                         else:
    #                             extLinkCount=0
    #                         if len(internallinkList)>0:
    #                             intLinkCount=int(internallinkList[1].strip())
    #                         else:
    #                             intLinkCount=0

    #                         if len(externalembededList)>0:
    #                             extEmbededCount=int(externalembededList[1].strip())
    #                         else:
    #                             extEmbededCount=0
    #                         if len(internalembededList)>0:
    #                             intEmbededCount=int(internalembededList[1].strip())
    #                         else:
    #                             intEmbededCount=0

    #                     url=urlValue[1].replace("'","").strip()
                        
    #                     if (field[0] is counter[1].strip() and field[4] == url):
    #                         # images=Imagecount
    #                         # scripts=scriptcount
    #                         # links=Linkcount
    #                         # embededs=EmbededCount

    #                         intImages=intImagecount
    #                         intScripts=intScriptCount
    #                         intLinks=intLinkCount
    #                         intEmbeded=intEmbededCount

    #                         extImages=extImagecount
    #                         extScripts=extScriptCount
    #                         extLinks=extLinkCount
    #                         extEmbededs=extEmbededCount
                            
    #             # TotalObjectCount=images+scripts+links+embededs
    #             TotalInternalObjects=intImages+intScripts+intLinks+intEmbeded
    #             TotalExternalObjects=extImages+extScripts+extLinks+extEmbededs
    #             d[TotalInternalObjects]=TotalInternalObjects
    #             d[TotalExternalObjects]=TotalExternalObjects
    #             field.insert(11,d[TotalInternalObjects])
    #             field.insert(12,d[TotalExternalObjects])
    #             writerOutput.writerow(field)
    #     f.close()

#print "ASN number change for index %s is : %s" % (str(idx), str(len(unique_asn)))

# [Som] :These lines can be used later for multiprocessing
# resultlist=[]
# print "listOfLists",listOfLists
# multiProc_crawler(listOfLists,2)

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
