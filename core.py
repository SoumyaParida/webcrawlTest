import csv
import multiprocessing as mp
import Queue
from alexaCrawl.spiders.alexawebcrawltest import alexaSpider
from alexaCrawl.spiders.alexawebcrawltest import tags_d
from scrapy import cmdline
import codecs
import re
import sys
import time
import os
from collections import defaultdict

start_time = time.time()
rowValues=[]
listOfLists=[]
urlIndexlist=dict()

listrange=10
IndexInTop1mFile=list()
IndexNotInResultFile=list()
finallist=list()
tags_d = defaultdict(int)

#listOfLists=[[] for _ in range(listrange)]



"""[Author  : Soumya ranjan Parida]
Function : makeSublist
This function can be used to create multiple sublist from the list of 100,000 websites.
Each sublist will contain equal share of websites like 1st list will contain 1st,51st,101th etc. websites,
2nd list will contain 2nd,52nd,102nd websites etc."""
def makeSublist(urllist):
    listOfLists=[[] for _ in range(listrange)]
    for rowValues in urllist:
        urlIndexlist[rowValues[1]]=rowValues[0]
        IndexInTop1mFile.append(rowValues[0])
        listOfLists[(int(rowValues[0]) - 1) % listrange].append(rowValues[1])
    return listOfLists

"""[Author  : Soumya ranjan Parida]
Function : setup_crawler
This function can be used to create multiple spiders
inside single process"""

def worker(urllist,out_q,i):
    global tags_d
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
    APACHE_ACCESS_LOG_PATTERN ='(\S+)(\t)(\d{1})(\t)(\d{3})(\t)(\w+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)([0-5]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9].\d{6})(\t)([0-5]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9].\d{6})'
    wordcount=list()
    UrlNotInResultFile=list()
    logwr = open("output6.csv").readlines()
    newoutput=open("output.csv", "w")
    urllistFile=open("urllistFile.txt",'wbr+')
    for idx, line in enumerate(logwr):
        match = re.search(APACHE_ACCESS_LOG_PATTERN, line)
        if match is None:
            logwr.pop(idx)
        else:
            newoutput.write(line)
    newoutput.close()        
    logFile = codecs.open("output.csv",'rU')
    IndexNotInResultFile=list()
    wordcount=set()
    UrlNotInResultFile=list()
    listOfListsNew=[]
    outputReader = csv.reader(logFile,skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL,dialect=csv.excel_tab)
    for line in outputReader:
            wordcount.add(line[0])
    IndexNotInResultFile=list(set(IndexInTop1mFile) - set(wordcount))
    def get_key_from_value(my_dict, v):
        for key,value in my_dict.items():
            if value == v:
                return key
        return None
    for item in IndexNotInResultFile:
        UrlNotInResultFile.append((item,get_key_from_value(urlIndexlist,item)))
    for item in UrlNotInResultFile:
        urllistFile.write(item[1])
        urllistFile.write("\n")
    return UrlNotInResultFile
"""[Author  : Soumya ranjan Parida]
Function : afterCrawl
This function can be used to check whether any urls are left for crawling.
If yes it will again start crawling the websites."""
def afterCrawl(UrlNotInResultFile):
    listOfListsNew=makeSublist(UrlNotInResultFile)
    if len(listOfListsNew) == 0:
        print "No Urls left for crawling"
    else:
        multiProc_crawler(listOfListsNew,listrange)
    return

#[Som] :These lines used later multiprocessing
urllist=list()
missedUrllist=list()
with open('top-1m.csv') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in spamreader:
        rowValue=', '.join(row)
        rowValues=rowValue.split(",")
        urllist.append(rowValues)
finallist=makeSublist(urllist)
multiProc_crawler(finallist,listrange)
missedUrllist=missedUrls()
afterCrawl(missedUrllist)
os.remove("output.csv")
os.remove("urllistFile.txt")
missedUrllist=missedUrls()
afterCrawl(missedUrllist)
missedUrllist=missedUrls()
timestr = time.strftime("%Y%m%d-%H%M%S")
filename='output_'+timestr+'.csv'
os.rename('output6.csv',filename)
print("--- %s seconds ---" % (time.time() - start_time))