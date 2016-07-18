import csv
import multiprocessing as mp
import Queue
from alexaCrawl.spiders.alexawebcrawltest import alexaSpider
from scrapy import cmdline
import codecs
import re
import sys
import time
import os
from collections import defaultdict
from shutil import copyfile

start_time = time.time()
rowValues=[]
listOfLists=[]
urlIndexlist=dict()
urlIndexdict=dict()

listrange=20
IndexInTop1mFile=list()
IndexNotInResultFile=list()
finallist=list()

"""[Author  : Soumya ranjan Parida]
Function : makeSublist
This function can be used to create multiple sublist from the list of 100,000 websites.
Each sublist will contain equal share of websites like 1st list will contain 1st,51st,101th etc. websites,
2nd list will contain 2nd,52nd,102nd websites etc."""
def makeSublist(urllist):
    listOfLists=[[] for _ in range(listrange)]
    for rowValues in urllist:
        urlIndexlist[rowValues[1]]=rowValues[0]
        urlIndexdict[rowValues[0]]=rowValues[1]
        IndexInTop1mFile.append(rowValues[0])
        listOfLists[(int(rowValues[0]) - 1) % listrange].append(rowValues[1])
    return listOfLists

"""[Author  : Soumya ranjan Parida]
Function : setup_crawler
This function can be used to create multiple spiders
inside single process"""

def worker(urllist,out_q,i):
    cmdline.execute([
    'scrapy', 'crawl', 'alexa',
    '-a', 'arg1='+str(urllist), '-a', 'arg2='+str(urlIndexlist)])
    return


'''
Given any number of dicts, shallow copy and merge into a new dict,
precedence goes to key value pairs in latter dicts.
'''
def merge_dicts(*dict_args):
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
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.
    while decrement:
        decrement = False
        try:
            csv.field_size_limit(maxInt)
        except OverflowError:
            maxInt = int(maxInt/10)
            decrement = True

def missedUrls():
    APACHE_ACCESS_LOG_PATTERN ='(\S+)(\t)(\d{1})(\t)(\d{3})(\t)(\w+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)([0-5]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9].\d{6})(\t)([0-5]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9].\d{6})'
    wordcount=list()
    UrlNotInResultFile=list()
    #os.rename('output6.csv','final.csv')
    copyfile('output6.csv','final.csv')
    logwr = open("final.csv").readlines()
    newoutput=open("finalcheckCopy.csv", "w")
    urllistFile=open("urllistFile.txt",'wbr+')
    print "Reading files start"
    for idx, line in enumerate(logwr):
        match = re.search(APACHE_ACCESS_LOG_PATTERN, line)
        if match is None:
            logwr.pop(idx)
        else:
            newoutput.write(line)
    newoutput.close()
    print "Reading files over"      
    logFile = codecs.open("finalcheckCopy.csv",'rU')
    IndexNotInResultFile=list()
    wordcount=set()
    UrlNotInResultFile=list()
    listOfListsNew=[]
    outputReader = csv.reader(logFile,skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL,dialect=csv.excel_tab)
    for line in outputReader:
            wordcount.add(line[0])
    IndexNotInResultFile=list(set(IndexInTop1mFile) - set(wordcount))
    print "get_key_from_value start"
    def get_key_from_value(my_dict, v):
        return my_dict.get(v)
    if len(IndexNotInResultFile) > 0:
        for item in IndexNotInResultFile:
            url=get_key_from_value(urlIndexdict,item)
            UrlNotInResultFile.append((item,url))
    print "get_key_from_value end"
    for item in UrlNotInResultFile:
        urllistFile.write(item[1])
        urllistFile.write("\n")
    return UrlNotInResultFile
"""[Author  : Soumya ranjan Parida]
Function : afterCrawl
This function can be used to check whether any urls are left for crawling.
If yes it will again start crawling the websites."""

#[Som] :These lines used later multiprocessing
urllist=list()
missedUrllist=list()
execCrawl=0
with open('top-1m.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in spamreader:
        rowValue=', '.join(row)
        rowValues=rowValue.split(",")
        urllist.append(rowValues)
while execCrawl < 3:
  finallist=makeSublist(urllist)
  multiProc_crawler(finallist,listrange)
  urllist=missedUrls()
  execCrawl+=1
print("--- %s seconds ---" % (time.time() - start_time))
