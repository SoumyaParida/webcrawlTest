import csv
import os
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log
import alexaCrawl.spiders
from alexaCrawl.spiders.alexawebcrawltest import alexaSpider
from scrapy.utils.project import get_project_settings
import multiprocessing as mp
import time
import random
import Queue


rowValues=[]
finalItemList=[]
listOfLists=[]

row_no=1
code_chunk=1
listOfLists=[[] for _ in range(4)]
with open('top-1m.csv') as csvfile:
	spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|') 
	print "soumya"
	for row in spamreader:
	    rowValue=', '.join(row)
	    rowValues=rowValue.split(",")
	   # if len(listOfLists)<10:
	    if (code_chunk==row_no):
	        #finalItemList.append(rowValues[1])
	        
	        listOfLists[row_no-1].append(rowValues[1])             
	        if (row_no==4):
	            row_no=row_no%4
	            #row_no=row_no-1
	        row_no=row_no+1
	        code_chunk=(row_no%20)
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
print "listOfLists",listOfLists[0]
#url=listOfLists[0]
url=",".join(listOfLists[0])
print url

#processes = [mp.Process(target=setup_crawler, args=(5, output)) for x in range(4)]
"""[Author  : Soumya ranjan Parida]
Function : setup_crawler
This function can be used to create multiple spiders
inside single process"""
# def setup_crawler(domain):
#     spider = alexaSpider(domain=domain)
#     settings = get_project_settings()
#     crawler = Crawler(settings)
#     crawler.configure()
#     crawler.crawl(spider)
#     crawler.start()

# for domain in listOfLists[0]:
# 	setup_crawler(domain)
# #log.start()
# reactor.run()

def multiProc_crawler(domainlist,nprocs):
    def worker(urllist, out_q):
    	print "urllist",urllist
    	outdict = {}
        #for urllist in urllistoflist:
        for url in urllist:
        	#setup_crawler(url)
        	spider = alexaSpider(domain=url)
        	settings = get_project_settings()
        	crawler = Crawler(settings)
        	crawler.configure()
        	crawler.crawl(spider)
        	crawler.start()
        	print "urlllllll",url
        reactor.run()
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

    for i in range(nprocs):
        p = mp.Process(
                target=worker,
                args=(domainlist[i],
                      out_q))
        procs.append(p)
        print "procs",procs
        p.start()

    # Collect all results into a single result dict. We know how many dicts
    # with results to expect.
    resultdict = {}
    for i in range(nprocs):
        resultdict.update(out_q.get())

    # Wait for all worker processes to finish
    for p in procs:
        p.join()

    return resultdict

#for url_list in listOfLists:
#	print "url_list",url_list
# for domain in listOfLists[0]:
# 	setup_crawler(domain)
resultlist=[]
resultlist=multiProc_crawler(listOfLists,4)





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
