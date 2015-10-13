import re
import datetime
from urlparse import urlparse
from pyspark.sql import Row
import sys
import os
from pyspark import SparkContext

import numpy as np
import statsmodels.api as sm # recommended import according to the docs
import matplotlib.pyplot as plt
from math import exp
from pylab import *

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)

    if match is None:
        return (logline, 0)
    size_field = match.group(7)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(7))

    # print "unique_id=" ,match.group(1)
    # print "    depth         =" , match.group(3)
    # print "    response_code =" , int(match.group(5))
    # print "    content_size  =" , size
    # print "    url           =" , match.group(9)
    # print "    cookies       =" , match.group(11)
    # print "    objecttype    =" , match.group(13)
    # print "    host          =" , match.group(15)
    # print "    ip_address    =" , match.group(17)
    # print "    asn_number    =" , match.group(19)
    # print "    start_time    =" , parse_apache_time(match.group(21))
    # print "    end_time      =" , parse_apache_time(match.group(23))
    return (Row(
        unique_id     = int(match.group(1)),
        depth         = match.group(3),
        response_code = int(match.group(5)),
        content_size  = size,
        url           = match.group(9),
        cookies       = match.group(11),
        objecttype    = match.group(13),
        host          = match.group(15),
        ip_address    = match.group(17),
        asn_number    = match.group(19),
        start_time    = match.group(21),
        end_time      = match.group(23)
    ), 1)

# APACHE_ACCESS_LOG_PATTERN = '(\S+)  (\d{1}) (\d{3}) (\w+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)   (\S+)'

APACHE_ACCESS_LOG_PATTERN = '(\S+)(\t)(\d{1})(\t)(\d{3})(\t)(\w+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)'



# baseDir = os.path.join('data')
# print baseDir
# print dir
# inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
# print inputPath
logFile = os.path.join('/home/soumya/Documents/courses/output1m.csv')
# logFile = os.path.join('/home/soumya/Documents/thesis/webcrawlTest.git/webcrawlTest/trunk/test.csv')
print logFile


sc=SparkContext("local", "pyspark_output.py")

# Row(    unique_id     = match.group(1),
#         depth         = match.group(2),
#         response_code = int(match.group(3)),
#         content_size  = size,
#         url           = match.group(5),
#         cookies       = match.group(6),
#         objecttype    = match.group(7),
#         host          = match.group(8),
#         ip_address    = match.group(9),
#         asn_number    = match.group(10),
#         start_time    = parse_apache_time(match.group(11)),
#         end_time      = parse_apache_time(match.group(12))
#     )

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc.textFile(logFile,use_unicode=False).map(parseApacheLogLine).cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    print "failed",failed_logs_count
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()

#=================================================================
#Content Size

# import statsmodels.api as sm

# content_sizes_count=access_logs.map(lambda log: log.content_size).filter(lambda value:value != 0).cache()
# print 'Content Size Avg: %i, Min: %i, Max: %s' % (
#     content_sizes_count.reduce(lambda a, b : a + b) / content_sizes_count.count(),
#     content_sizes_count.min(),
#     content_sizes_count.max())
# content_sizes_count_list=content_sizes_count.collect()

# Content_sizes_first_100000_webiste=access_logs.map(lambda log: (log.unique_id,log.content_size)).cache()
# print Content_sizes_first_100000_webiste.take(10)
# Content_sizes_first_100000_webiste_count=Content_sizes_first_100000_webiste.filter(lambda x,y : x < 100000 )
# print Content_sizes_first_100000_webiste_count.take(10)
# print 'Content Size Avg: %i, Min: %i, Max: %s' % (
#     Content_sizes_first_100000_webiste_count.reduce(lambda a, b : a + b) / Content_sizes_first_100000_webiste_count.count(),
#     Content_sizes_first_100000_webiste_count.min(),
#     Content_sizes_first_100000_webiste_count.max())
# Content_sizes_first_100000_webiste_count_list=Content_sizes_first_100000_webiste_count.collect()

# X=sorted(content_sizes_count_list)
# Y=[]
# l=len(X)
# Y.append(float(1)/l)
# for i in range(2,l+1):
#     Y.append(float(1)/l+Y[i-2])
# plt.plot(X,Y,marker='o',label='xyz')
# plt.show()

# X=sorted(Content_sizes_first_100000_webiste_count_list)
# Y=[]
# l=len(X)
# Y.append(float(1)/l)
# for i in range(2,l+1):
#     Y.append(float(1)/l+Y[i-2])
# plt.plot(X,Y,marker='o',label='xyz')
# plt.show()

# sample = content_sizes_count_list
# ecdf = sm.distributions.ECDF(sample)

# x = np.linspace(min(sample), max(sample))
# y = ecdf(x)
# plt.yscale('log',nonposy='clip')
# plt.step(x, y)
# plt.show()

# bins=[0,100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000,1100000,1200000,1300000,1400000,1500000,1600000]
# # sorted_data = np.sort(content_sizes_count_list)
# # max_val=log10(sorted_data.max())
# # min_val=log10(sorted_data.min())
# #logspace = np.logspace(content_sizes_count.min(), content_sizes_count.max(), 50)
# plt.hist(content_sizes_count_list,bins,histtype='bar',rwidth=0.8)
# plt.yscale('log',nonposy='clip')
# plt.xlabel('X')
# plt.ylabel('Y')
# plt.title('content size')
# plt.show()

# sorted_data = np.sort(content_sizes_count_list)
# max_val=log10(sorted_data.max())
# min_val=log10(sorted_data.min())

# logspace = np.logspace(min_val, max_val, 0.5)
# plt.hist(content_sizes_count_list,bins=logspace,histtype='step')
# plt.show()


#=================================================================
#Response code
#=================================================================
# response_code=access_logs.map(lambda log: log.response_code).cache()
# labels=list()
# fracs=list()
# responseCode=access_logs.map(lambda log: log.response_code)
# responseCodeToCount = (access_logs
#                        .map(lambda log: (log.response_code, 1))
#                        .reduceByKey(lambda a, b : a + b)
#                        .cache())
# #responseCodeToCountList = responseCodeToCount.take(100)
# #print 'Found %d response codes' % len(responseCodeToCountList)
# #print 'Response Code Counts: %s' % responseCodeToCountList

# for item in responseCodeToCountList:
#     labels.append(item[0])
#     value=float(item[1])/access_logs.count()
#     fracs.append(value)
# print "labels",labels
# print "Number_Res_code",fracs

# rcParams['figure.figsize'] = 18, 7
# rcParams['font.size'] = 8

# N = len(fracs)

# ind = range(N)

# fig = plt.figure()

# ax = fig.add_subplot(111)
# ax.bar(ind,fracs,log='true',align='center')

# ax.set_ylabel('Percentage of web objects -- >',fontsize=14)
# ax.set_xlabel('Response codes -->',fontsize=14)

# ax.set_title('Response code graph for Alexa top 1 milion websites'+"\n",fontsize=18)               

# ax.set_xticks(ind)
# ax.set_xticklabels(labels)
# ax.grid()                                      

# fig.autofmt_xdate(bottom=0.2, rotation=90, ha='left')

# plt.yscale('log')
# #Uncomment the line for plotting bars in graph
# #plt.yscale('log',nonposy='clip')
# plt.show()

#End of Response code
#============================================================================================
#Start of Type of script
#============================================================================================
# urlRDD=access_logs.map(lambda log: log.url).cache()
# urlwithScriptRDD=urlRDD.filter(lambda value:'.php' in value or '.jsp' in value or '.aspx' in value
#                                 or '.perl' in value or '.css' in value or '.rb' in value
#                                    or '.py' in value)
# urlWithphplist=urlRDD.filter(lambda value:'.php' in value).collect()
# urlWithjavalist=urlRDD.filter(lambda value:'.jsp' in value).collect()
# urlWithasplist=urlRDD.filter(lambda value:'.aspx' in value).collect()
# urlWithperllist=urlRDD.filter(lambda value:'.perl' in value).collect()
# urlWithcsslist=urlRDD.filter(lambda value:'.css' in value).collect()
# urlWithrubylist=urlRDD.filter(lambda value:'.rb' in value).collect()
# urlWithpythonlist=urlRDD.filter(lambda value:'.py' in value).collect()

# labels=('PHP','JAVA','ASP','PERL','CSS','RUBY','PYTHON')
# fracs=list()
# valuephp=float(len(urlWithphplist))/urlwithScriptRDD.count()
# valuejava=float(len(urlWithjavalist))/urlwithScriptRDD.count()
# valueasp=float(len(urlWithasplist))/urlwithScriptRDD.count()
# valueperl=float(len(urlWithperllist))/urlwithScriptRDD.count()
# valuecss=float(len(urlWithcsslist))/urlwithScriptRDD.count()
# valueruby=float(len(urlWithrubylist))/urlwithScriptRDD.count()
# valuepython=float(len(urlWithpythonlist))/urlwithScriptRDD.count()
# fracs.append(valuephp)
# fracs.append(valuejava)
# fracs.append(valueasp)
# fracs.append(valueperl)
# fracs.append(valuecss)
# fracs.append(valueruby)
# fracs.append(valuepython)

# xlabel('Percentage--->',color='black',fontsize=24)
# ylabel('Server side languages--->',color='black',fontsize=24)
# title('Usage of server-side programming languages for Alexa Top 1 milion websites'+"\n",fontsize=30)
# width=0.5
# pos=arange(len(labels))+0.5
# barh(pos,fracs,align='center',color='green')
# yticks(pos,labels)
# show()

#End of Type of Script
#=============================================================================================
#start of cookies
#=============================================================================================
# cookiesRDD=access_logs.map(lambda log:(log.url,log.cookies)).cache()
# cookieswithUrlRDDList=cookiesRDD.collect()
# urls=list()
# def getsecondleveldomain(url):
#     with open("/home/soumya/Documents/thesis/scrapy_thes/webcrawlTest/trunk/effective_tld_names.dat") as tld_file:
#         tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
#     print "urlelements",url
#     url_elements = urlparse(url)[1].split('.')
#     print "url_elements",urlparse(url)[1]
#     for i in range(-len(url_elements), 0):
#         last_i_elements = url_elements[i:]
#         candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
#         wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
#         exception_candidate = "!" + candidate
#         # match tlds: 
#         if (exception_candidate in tlds):
#             print "url_elements[i:]",url_elements[i:]
#             return ".".join(url_elements[i:]) 
#         if (candidate in tlds or wildcard_candidate in tlds):
#             print "url_elements[i-1:]",url_elements[i-1:]
#             return ".".join(url_elements[i-1:])
# first_party_cookie=list()
# third_party_cookie=list()

# for item in cookieswithUrlRDDList:
#     urlValue=str(item[0])
#     url=getsecondleveldomain(urlValue)
#     value=item[1]
#     if item[1] is not None and url is not None:
#         if url in item[1]:
#             first_party_cookie.append(url)
#         else:
#             if '.com' in value and url not in value:
#                 third_party_cookie.append(url)
# print "first",len(first_party_cookie)
# print "third",len(third_party_cookie)

#End of cookies
#============================================================================================
#Type of Link
#=============================================================================================
# objectTypeRDD=access_logs.map(lambda log: log.objecttype).cache()
# objectTypeProperRDD=objectTypeRDD.filter(lambda value:'A' in value or 'S' in value or 'L' in value or 'E' in value)
# objectTypeofAnchorRDD=objectTypeProperRDD.filter(lambda value:'A' in value).collect()
# objectTypeofScriptRDD=objectTypeProperRDD.filter(lambda value:'S' in value).collect()
# objectTypeofLinkRDD=objectTypeProperRDD.filter(lambda value:'L' in value).collect()
# objectTypeofEmbededRDD=objectTypeProperRDD.filter(lambda value:'E' in value).collect()

# labels=('Anchor','Script','Links','Embeded')
# fracs=list()
# valueAnchor=float(len(objectTypeofAnchorRDD))/objectTypeProperRDD.count()
# valueScript=float(len(objectTypeofScriptRDD))/objectTypeProperRDD.count()
# valueLink=float(len(objectTypeofLinkRDD))/objectTypeProperRDD.count()
# valueEmbeded=float(len(objectTypeofEmbededRDD))/objectTypeProperRDD.count()

# fracs.append(valueAnchor)
# fracs.append(valueScript)
# fracs.append(valueLink)
# fracs.append(valueEmbeded)

# xlabel('Percentage--->',color='black',fontsize=24)
# ylabel('Object Type--->',color='black',fontsize=24)
# title('Usage of different Object type for Alexa Top 1m websites'+"\n",fontsize=30)
# width=0.1
# pos=arange(len(labels))+0.1
# barh(pos,fracs,align='center',color='green')
# yticks(pos,labels)
# show()
#End of objectType
#============================================================================================
#Type of Host
#=============================================================================================
#def changeString():
hostRDD=access_logs.map(lambda log:(log.unique_id,log.host)).cache()
hostProperRDD=hostRDD.map(lambda (id,host):(id,host.split(';')))
hostwithoutNullRDD=hostProperRDD.filter(lambda (x,y):'-' not in y)
def f(x): return x
allHostsRDD=hostwithoutNullRDD.flatMapValues(f).cache()
def getsecondleveldomain(url):
    with open("/home/soumya/Documents/thesis/scrapy_thes/webcrawlTest/trunk/effective_tld_names.dat") as tld_file:
        tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
    url_elements = urlparse(url)[1].split('.')
    for i in range(-len(url_elements), 0):
        last_i_elements = url_elements[i:]
        candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
        wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
        exception_candidate = "!" + candidate
        # match tlds: 
        if (exception_candidate in tlds):
            return url_elements[i:][0]
        if (candidate in tlds or wildcard_candidate in tlds):
            return url_elements[i-1:][0]
def getsecondleveldomainValue(value):
    domain=value[1]
    if domain.startswith('www.'):
        domain = domain.replace("www","")
    if domain.endswith('.'):
        domain=domain[:-1]

    if not domain.startswith('http://'):
        domain = 'http://%s' % domain
    secondlevelurl=str(getsecondleveldomain(domain))
    return (value[0],secondlevelurl)
allHostswithSecondlevelDomainRDD=allHostsRDD.map(getsecondleveldomainValue)
allhostwithUniquehost=allHostswithSecondlevelDomainRDD.map(lambda (x,y):(y,x))
allhostwithUniqueID=allhostwithUniquehost.groupByKey().cache()
HostCount = allhostwithUniqueID.map(lambda (x,y):(x,len(y))).cache()
HostCountSorted = HostCount.top(10, lambda s: s[1])
print 'Top five Hosts: %s' % HostCountSorted
HostCountRDD = allhostwithUniquehost.groupByKey().mapValues(lambda x: set(x))
UniqueHostCount = HostCountRDD.map(lambda (x,y):(x,len(y))).cache()
UniqueHostCountSorted = UniqueHostCount.top(10, lambda s: s[1])
print 'Top five Hosts: %s' % UniqueHostCountSorted
#End of objectType
#============================================================================================

#print allHostswithSecondlevelDomainRDD.take(3)
# # hostwithoutNullRDD=hostProperRDD.filter(lambda (id,host):id is str(816421))
# hostProperRDDstring=hossecondlevelurl !=None:tProperRDD.filter(lambda (id,host): (id ,lambda host :str(host)))
# #hostwithoutNullRDD=hostProperRDDstring.filter(lambda (id,host):(id,'-' not in host))
# hostwithoutNullRDD=hostProperRDDstring.filter(lambda x:'-' not in x)
# # hostUniqueRDD=hostwithoutNullRDD.UniqueHostCountmap(lambda (x,y) : (x,value for value in y)
# print hostProperRDDstring.take(2)
# print hostwithoutNullRDD.take(2)
# def is_error(id,host):
#     if '-' not in host:
#         unique_id=id
#         urllist=urllist.append(host)
#         for value in urllist:
#             return (unique_id,value)
# hostProperRDDfilterhypen=hostProperRDD.filter(is_error)
# print hostProperRDDfilterhypen.take(2)

# hostRDD=access_logs.map(lambda log:(log.host,log.unique_id)).cache()
# GroupedHosts = hostRDD.groupByKey().mapValues(lambda x: set(x))
# # hostProperRDD=hostRDD.filter(lambda (id,host):(id,'-' not in host))
# GroupedHostsList=GroupedHosts.takeOrdered(10,lambda s: -1 * s)
# print len(GroupedHostsList[0])



# xlabel('occurances of objects--->',color='black',fontsize=24)
# ylabel('Hosts--->',color='black',fontsize=24)
# title('Top 25 Hosts with highest object Occurances for Alexa Top 100 websites'+"\n",fontsize=30)
# width=0.5
# pos=arange(len(labels))+.5
# barh(pos,fracs,align='center',color='green')
# yticks(pos,labels)
# plt.autoscale(enable=True, axis='y', tight=None)
# grid(True)
# show()

# Any hosts that has accessed the server more than 10 times.
# hostCountPairTuple = access_logs.map(lambda log: (log.host, 1))

# hostSum = hostCountPairTuple.reduceByKey(lambda a, b : a + b)

# hostMoreThan10 = hostSum.filter(lambda s: s[1] > 10)

# hostsPick20 = (hostMoreThan10
#                .map(lambda s: s[0])
#                .take(20))

# print 'Any 20 hosts that have accessed more then 10 times: %s' % hostsPick20