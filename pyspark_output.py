import re
import datetime
from urlparse import urlparse
from datetime import datetime
from datetime import timedelta
# import time
from math import exp
from pyspark.sql import Row
from pyspark import SparkContext
import sys
import os
import csv


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

    return (Row(
        unique_id     = match.group(1),
        depth         = match.group(3),
        response_code = int(match.group(5)),
        content_size  = size,
        url           = match.group(9),
        cookies       = match.group(11),
        objecttype    = match.group(13),
        host          = match.group(15),
        ip_address    = match.group(17),
        asn_number    = match.group(19),
        InternalAnchorCount = match.group(21),
        ExternalAnchorCount = match.group(23),
        UniqueExternalSitesForAnchor = match.group(25),
        InternalImageCount=match.group(27),
        ExternalImageCount=match.group(29),
        UniqueExternalSitesForImage=match.group(31),
        InternalscriptCount=match.group(33),
        ExternalscriptCount=match.group(35),
        UniqueExternalSitesForScript=match.group(37),
        InternallinkCount=match.group(39),
        ExternallinkCount=match.group(41),
        UniqueExternalSitesForLink=match.group(43),
        InternalembededCount=match.group(45),
        ExternalembededCount=match.group(47),
        UniqueExternalSitesForEmbeded=match.group(49),
        start_time    = match.group(51),
        end_time      = match.group(53)
    ), 1)

#APACHE_ACCESS_LOG_PATTERN = '(\S+)(\t)(\d{1})(\t)(\d{3})(\t)(\w+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(^(2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9]).(\d{6})$)(\t)(^(2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9]).(\d{6})$)'
APACHE_ACCESS_LOG_PATTERN = '(\S+)(\t)(\d{1})(\t)(\d{3})(\t)(\w+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)([0-5]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9].\d{6})(\t)([0-5]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9].\d{6})'
logFile = os.path.join('/home/soumya/Documents/results/5th_nov/output6_5th_nov.csv')
# APACHE_ACCESS_LOG_PATTERN = '(\S+)(\t)(\d{1})(\t)(\d{3})(\t)(\w+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)(\t)(\S+)'
# logFile = os.path.join('/home/soumya/Documents/courses/output6.csv')
print logFile


sc=SparkContext("local", "pyspark_output.py")

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc.textFile(logFile,use_unicode=False).map(parseApacheLogLine).cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]).cache())
    failed_logs_count = failed_logs.count()
    print "failed",failed_logs_count
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    #print 'Read %d lines, successfully parsed %d lines' % (parsed_logs.count(), access_logs.count())
    return parsed_logs, access_logs,failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()
#parsed_logs, access_logs= parseLogs()

#=================================================================
#sites Crawled from top 100,000 sites
# unique_id=access_logs.map(lambda log: log.unique_id).cache()
# unique_id_values=set(unique_id.collect())
# print len(unique_id_values)

#=================================================================
#Content Size

# import statsmodels.api as sm

# content_sizes_count=access_logs.map(lambda log: log.content_size).filter(lambda value:value != 0).cache()
# print 'Content Size Avg: %i, Min: %i, Max: %s' % (
#     content_sizes_count.reduce(lambda a, b : a + b) / content_sizes_count.count(),
#     content_sizes_count.min(),
#     content_sizes_count.max())
# content_sizes_count_list=content_sizes_count.collect()

# X=sorted(content_sizes_count_list)
# Y=[]
# l=len(X)
# Y.append(float(1)/l)
# for i in range(2,l+1):
#     Y.append(float(1)/l+Y[i-2])
# plt.plot(X,Y,marker='o',label='CDF for Total Content size',color='green')
# title('CDF for total Content Size'+"\n",fontsize=18,color='blue')
# xlabel('Content (in bytes)--->',color='blue',fontsize=12)
# ylabel('CDF--->',color='blue',fontsize=12)
# plt.show()

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
response_code=access_logs.map(lambda log: log.response_code).cache()
labels=list()
fracs=list()
responseCode=access_logs.map(lambda log: log.response_code)
responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b : a + b)
                       .cache())
responseCodeToCountList = responseCodeToCount.collect()
print 'Found %d response codes' % len(responseCodeToCountList)
print 'Response Code Counts: %s' % responseCodeToCountList
labels=list()
fracs=list()
sorted_by_second = sorted(responseCodeToCountList, key=lambda tup: tup[0])
for item in sorted_by_second:
    labels.append(item[0])
    value=float(item[1]*100)/access_logs.count()
    fracs.append(value)
print "labels",labels
print "Number_Res_code",fracs

rcParams['figure.figsize'] = 18, 7
rcParams['font.size'] = 8

N = len(fracs)

ind = range(N)

pos=arange(len(labels))+0.1
fig = plt.figure()

ax = fig.add_subplot(111)
ax.bar(ind,fracs,log='true',align='center')

ax.set_ylabel('Percentage of web objects -- >',fontsize=14)
ax.set_xlabel('Response codes -->',fontsize=14)

ax.set_title('Log bar Plot for response code graph for Alexa top 100,000 websites'+"\n",fontsize=18)  

colors=list()
labels_legend=list()
for value in sorted_by_second:
    if value[0]>=200 and value[0]<300:
        colors.append('b')
        if '200-300' not in labels_legend:
            labels_legend.append('200-300')
            ax.bar(pos,fracs,log='true',align='center',color='b',label='200-299')
    elif value[0]>=300 and value[0]<400:
        colors.append('g')
        if '300-400' not in labels_legend:
            labels_legend.append('300-400')
            ax.bar(pos,fracs,log='true',align='center',color='g',label='300-399')
    elif value[0]>=400 and value[0]<500:
        if '400-500' not in labels_legend:
            labels_legend.append('400-500')
            ax.bar(pos,fracs,log='true',align='center',color='r',label='400-499')
        colors.append('r')
    elif value[0]>=500 and value[0]<600:
        if '500-600' not in labels_legend:
            labels_legend.append('500-600')
            ax.bar(pos,fracs,log='true',align='center',color='c',label='500-599')
        colors.append('c')
    elif value[0]>=600 and value[0]<700:
        if '600-700' not in labels_legend:
            labels_legend.append('600-700')
            ax.bar(pos,fracs,log='true',align='center',color='m',label='600-699')
        colors.append('m')
    elif value[0]>=700 and value[0]<800:
        if '700-800' not in labels_legend:
            labels_legend.append('700-800')
            ax.bar(pos,fracs,log='true',align='center',color='y',label='700-799')
        colors.append('y')
    else:
        if '900+' not in labels_legend:
            labels_legend.append('900+')
            ax.bar(pos,fracs,log='true',align='center',color='k',label='900+')
        colors.append('k')             

ax.bar(pos,fracs,log='true',align='center',color=colors)
ax.set_xticks(pos)
ax.set_xticklabels(labels)
ax.grid()
ax.legend()                                    

fig.autofmt_xdate(bottom=0.2, rotation=90, ha='left')
show()

#End of Response code
# #============================================================================================
# #Start of Type of script
# #============================================================================================
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

# labels=('PHP','JAVA','ASP','PERL','CSS','RUBY','PYTHON','OTHER')
# fracs=list()
# valuephp=(float(len(urlWithphplist))/urlRDD.count())*100
# valuejava=(float(len(urlWithjavalist))/urlRDD.count())*100
# valueasp=(float(len(urlWithasplist))/urlRDD.count())*100
# valueperl=(float(len(urlWithperllist))/urlRDD.count())*100
# valuecss=(float(len(urlWithcsslist))/urlRDD.count())*100
# valueruby=(float(len(urlWithrubylist))/urlRDD.count())*100
# valuepython=(float(len(urlWithpythonlist))/urlRDD.count())*100
# valueother=100-(valuephp+valuejava+valueasp+valueperl+valuecss+valueruby+valuepython)
# fracs.append(valuephp)
# fracs.append(valuejava)
# fracs.append(valueasp)
# fracs.append(valueperl)
# fracs.append(valuecss)
# fracs.append(valueruby)
# fracs.append(valuepython)
# fracs.append(valueother)

# fig, ax= plt.subplots(figsize=(9, 7))
# ax.axis([0, 100,0,7])

# xlabel('Percentage--->',color='black',fontsize=12)
# ylabel('Server side languages--->',color='black',fontsize=12)
# title('Percentages of websites from Alexa Top 1 milion websites using various server-side programming languages '+"\n",color='green',fontsize=18)
# width=0.5
# pos=arange(len(labels))+0.5
# rects=barh(pos,fracs,align='center',color='blue')
# yticks(pos,labels)
# suffixes = ['th', 'st', 'nd', 'rd', 'th', 'th', 'th', 'th', 'th', 'th']
# for rect in rects:
#     # Rectangle widths are already integer-valued but are floating
#     # type, so it helps to remove the trailing decimal point and 0 by
#     # converting width to int type
#     width = float(rect.get_width())
#     width = float("{0:.2f}".format(width))

#     # Figure out what the last digit (width modulo 10) so we can add
#     # the appropriate numerical suffix (e.g., 1st, 2nd, 3rd, etc)
#     # lastDigit = width % 10
#     # # Note that 11, 12, and 13 are special cases
#     # if (width == 11) or (width == 12) or (width == 13):
#     #     suffix = 'th'
#     # else:
#     #     suffix = suffixes[lastDigit]

#     rankStr = str(width) + '%'
#     if (width < 5):        # The bars aren't wide enough to print the ranking inside
#         xloc = width + 1   # Shift the text to the right side of the right edge
#         clr = 'black'      # Black against white background
#         align = 'left'
#     else:
#         xloc = 0.98*width  # Shift the text to the left side of the right edge
#         clr = 'white'      # White on magenta
#         align = 'right'

#     # Center the text vertically in the bar
#     yloc = rect.get_y()+rect.get_height()/2.0
#     ax.text(xloc, yloc, rankStr, horizontalalignment=align,
#             verticalalignment='center', color=clr, weight='bold')
# plt.show()

#End of Type of Script

#============================================================================================
#Start of Type of client script
#============================================================================================
# urlRDD=access_logs.map(lambda log: log.url).cache()
# urlwithScriptRDD=urlRDD.filter(lambda value:('.js' in value or '.swf' in value or '.xap' in value) and '.jsp' not in value )
# urlWithjavascriptlist=urlRDD.filter(lambda value:'.js' in value and '.jsp' not in value).collect()
# urlWithflashlist=urlRDD.filter(lambda value:'.swf' in value).collect()
# urlWithsilverlightlist=urlRDD.filter(lambda value:'.xap' in value).collect()

# labels=('Javascript','Flash','Silverlight')
# fracs=list()
# valueJavascript=(float(len(urlWithjavascriptlist))/urlwithScriptRDD.count())*100
# valueflash=(float(len(urlWithflashlist))/urlwithScriptRDD.count())*100
# valuesilverlight=(float(len(urlWithsilverlightlist))/urlwithScriptRDD.count())*100

# fracs.append(valueJavascript)
# fracs.append(valueflash)
# fracs.append(valuesilverlight)

# fig, ax= plt.subplots(figsize=(9, 7))
# ax.axis([0, 100,0,7])

# xlabel('Percentage--->',color='black',fontsize=12)
# ylabel('Server side languages--->',color='black',fontsize=12)
# title('Percentages of websites from Alexa Top 1 milion websites using various Client-side programming languages '+"\n",color='green',fontsize=18)
# width=0.5
# pos=arange(len(labels))+0.5
# rects=barh(pos,fracs,align='center',color='blue')
# yticks(pos,labels)
# suffixes = ['th', 'st', 'nd', 'rd', 'th', 'th', 'th', 'th', 'th', 'th']
# for rect in rects:
#     # Rectangle widths are already integer-valued but are floating
#     # type, so it helps to remove the trailing decimal point and 0 by
#     # converting width to int type
#     width = float(rect.get_width())
#     width = float("{0:.2f}".format(width))

#     # Figure out what the last digit (width modulo 10) so we can add
#     # the appropriate numerical suffix (e.g., 1st, 2nd, 3rd, etc)
#     # lastDigit = width % 10
#     # # Note that 11, 12, and 13 are special cases
#     # if (width == 11) or (width == 12) or (width == 13):
#     #     suffix = 'th'
#     # else:
#     #     suffix = suffixes[lastDigit]

#     rankStr = str(width) + '%'
#     if (width < 5):        # The bars aren't wide enough to print the ranking inside
#         xloc = width + 1   # Shift the text to the right side of the right edge
#         clr = 'black'      # Black against white background
#         align = 'left'
#     else:
#         xloc = 0.98*width  # Shift the text to the left side of the right edge
#         clr = 'white'      # White on magenta
#         align = 'right'

#     # Center the text vertically in the bar
#     yloc = rect.get_y()+rect.get_height()/2.0
#     ax.text(xloc, yloc, rankStr, horizontalalignment=align,
#             verticalalignment='center', color=clr, weight='bold')
# plt.show()
#=============================================================================================
#start of cookies
#=============================================================================================
# cookiesRDD=access_logs.map(lambda log:(log.url,log.cookies)).cache()
# cookieswithoutNullRDD=cookiesRDD.filter(lambda (x,y):'-' not in y).cache()
# def getsecondleveldomain(url):
#     with open("/home/soumya/Documents/thesis/scrapy_thes/webcrawlTest/trunk/effective_tld_names.dat") as tld_file:
#         tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
#     url_elements = urlparse(url)[1].split('.')
#     for i in range(-len(url_elements), 0):
#         last_i_elements = url_elements[i:]
#         candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
#         wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
#         exception_candidate = "!" + candidate
#         # match tlds: 
#         if (exception_candidate in tlds):
#             return ".".join(url_elements[i:]) 
#         if (candidate in tlds or wildcard_candidate in tlds):
#             return ".".join(url_elements[i-1:])
# def getsecondleveldomainValue(value):
#     domain=value[0]
#     secondlevelurl=str(getsecondleveldomain(domain))
#     return (secondlevelurl,value[1])
# allURLswithSecondlevelDomainRDD=cookieswithoutNullRDD.map(getsecondleveldomainValue).cache()
# allFirstPartyCookiesRDD=allURLswithSecondlevelDomainRDD.filter(lambda (x,y) :x in y)
# allThirdPartyCookiesRDD=allURLswithSecondlevelDomainRDD.filter(lambda (x,y) :'.com' in y and x not in y)
# allThirdPartyCookieswithGoogleRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'google' in y)
# allThirdPartyCookieswithGoogleAdvRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'doubleclick' in y)
# allThirdPartyCookieswithfacebookRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'facebook' in y)
# allThirdPartyCookieswithamazonRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'amazon' in y)
# allThirdPartyCookieswithebayRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'ebay' in y)
# allThirdPartyCookieswithakamaiRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'akamai' in y)
# allThirdPartyCookieswithtwitterRDD=allThirdPartyCookiesRDD.filter(lambda (x,y) : 'twitter' in y)

# print allFirstPartyCookiesRDD.take(5)
# print allThirdPartyCookiesRDD.take(10)
# print allThirdPartyCookieswithGoogleAdvRDD.take(10)
# print allThirdPartyCookieswithGoogleAdvRDD.count()

# print allThirdPartyCookieswithGoogleRDD.count()
# print allThirdPartyCookieswithfacebookRDD.count()
# print allThirdPartyCookieswithamazonRDD.count()
# print allThirdPartyCookieswithebayRDD.count()
# print allThirdPartyCookieswithakamaiRDD.count()
# print allThirdPartyCookieswithtwitterRDD.count()

# allURLswithSecondlevelDomainRDDlist=allURLswithSecondlevelDomainRDD.collect()
# first_party_cookie=list()
# third_party_cookie=list()
# for item in allURLswithSecondlevelDomainRDDlist:
#     url=item[0]
#     value=item[1]
#     if item[1] is not None and url is not None:
#         if url in item[1]:
#             first_party_cookie.append(url)
#         else:
#             if '.com' in value and url not in value:
#                 third_party_cookie.append(url)
# print "first",len(first_party_cookie)
# print "third",len(third_party_cookie)


# cookieswithUrlRDDList=cookiesRDD.collect()
# urls=list()
# def getsecondleveldomain(url):
#     with open("/home/soumya/Documents/thesis/scrapy_thes/webcrawlTest/trunk/effective_tld_names.dat") as tld_file:
#         tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
#     url_elements = urlparse(url)[1].split('.')
#     for i in range(-len(url_elements), 0):
#         last_i_elements = url_elements[i:]
#         candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
#         wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
#         exception_candidate = "!" + candidate
#         # match tlds: 
#         if (exception_candidate in tlds):
#             return url_elements[i:][0]
#         if (candidate in tlds or wildcard_candidate in tlds):
#             return url_elements[i-1:][0]
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
# hostRDD=access_logs.map(lambda log:(log.unique_id,log.host)).cache()
# hostProperRDD=hostRDD.map(lambda (id,host):(id,host.split(';')))
# hostwithoutNullRDD=hostProperRDD.filter(lambda (x,y):'-' not in y)
# def f(x): return x
# allHostsRDD=hostwithoutNullRDD.flatMapValues(f).cache()
# def getsecondleveldomain(url):
#     with open("/home/soumya/Documents/thesis/scrapy_thes/webcrawlTest/trunk/effective_tld_names.dat") as tld_file:
#         tlds = [line.strip() for line in tld_file if line[0] not in "/\n"]
#     url_elements = urlparse(url)[1].split('.')
#     for i in range(-len(url_elements), 0):
#         last_i_elements = url_elements[i:]
#         candidate = ".".join(last_i_elements) # abcde.co.uk, co.uk, uk
#         wildcard_candidate = ".".join(["*"] + last_i_elements[1:]) # *.co.uk, *.uk, *
#         exception_candidate = "!" + candidate
#         # match tlds: 
#         if (exception_candidate in tlds):
#             return url_elements[i:][0]
#         if (candidate in tlds or wildcard_candidate in tlds):
#             return url_elements[i-1:][0]
# def getsecondleveldomainValue(value):
#     domain=value[1]
#     if domain.startswith('www.'):
#         domain = domain.replace("www","")
#     if domain.endswith('.'):
#         domain=domain[:-1]

#     if not domain.startswith('http://'):
#         domain = 'http://%s' % domain
#     secondlevelurl=str(getsecondleveldomain(domain))
#     return (value[0],secondlevelurl)
# allHostswithSecondlevelDomainRDD=allHostsRDD.map(getsecondleveldomainValue)
# allhostwithUniquehost=allHostswithSecondlevelDomainRDD.map(lambda (x,y):(y,x))
# allhostwithUniqueID=allhostwithUniquehost.groupByKey().cache()
# HostCount = allhostwithUniqueID.map(lambda (x,y):(x,len(y))).cache()
# HostCountSorted = HostCount.top(100, lambda s: s[1])
# print 'Top five Hosts: %s' % HostCountSorted
# HostCountRDD = allhostwithUniquehost.groupByKey().mapValues(lambda x: set(x))
# UniqueHostCount = HostCountRDD.map(lambda (x,y):(x,len(y))).cache()
# UniqueHostCountSorted = UniqueHostCount.top(100, lambda s: s[1])
# print 'Top five Hosts: %s' % UniqueHostCountSorted
#End of objectType
#============================================================================================
#Number of hits by IP address
#=============================================================================================
# IPAddressRDD=access_logs.map(lambda log:(1,log.ip_address)).cache()
# IPAddressRDD=IPAddressRDD.map(lambda (count,ipaddress):(count,ipaddress.split(';')))
# IPAddresswithoutNullRDD=IPAddressRDD.filter(lambda (x,y):'-' not in y)
# def f(x): return x
# allIPAddressRDD=IPAddresswithoutNullRDD.flatMapValues(f).cache()
# allIPAddressCountRDD=allIPAddressRDD.map(lambda (x,y):(y,x))
# allIPAddressSum = allIPAddressCountRDD.reduceByKey(lambda x,y :x+y)
# allIPAddressTop25 = allIPAddressSum.top(100, lambda s: s[1])
# print 'Top 100 hosts that generated errors: %s' % allIPAddressTop25

#ASN NUMBER
#==========================================================================

# ASNRDD=access_logs.map(lambda log:(log.unique_id,log.asn_number)).cache()
# ASNNumbersRDD=ASNRDD.map(lambda (id,asn):(id,asn.split(';')))
# ASNwithoutNullRDD=ASNNumbersRDD.filter(lambda (x,y):'-' not in y)
# def f(x): return x
# allASNRDD=ASNwithoutNullRDD.flatMapValues(f).cache()
# allASNCountRDD=allASNRDD.map(lambda (x,y):(y,x))
# allUniqueIDwithCommonASN=allASNCountRDD.groupByKey().cache()
# ASNCount = allUniqueIDwithCommonASN.map(lambda (x,y):(x,len(y))).cache()
# ASNCountSorted = ASNCount.top(100, lambda s: s[1])
# print 'Top 100 ASNS: %s' % ASNCountSorted
# ASNCountRDD = allASNCountRDD.groupByKey().mapValues(lambda x: set(x))
# UniqueASNCount = ASNCountRDD.map(lambda (x,y):(x,len(y))).cache()
# UniqueASNCountSorted = UniqueASNCount.top(100, lambda s: s[1])
# print 'Top 100 Unique ASNs: %s' % UniqueASNCountSorted

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

#*****************************************************************************************************************


# logFile = open("/home/soumya/Documents/courses/output1m.csv",'r')
# logwr = csv.reader(logFile,skipinitialspace=True,delimiter='\t',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
 
# totaltimeForImage=0
# totaltimeForScript=0
# totaltimeForAnchor=0
# totaltimeForLink=0
# totaltimeForEmbeded=0
# imageTime=list()
# scriptlist=list()
# anchrorlist=list()
# linklist=list()
# embededlist=list()


# for line in logwr:
#   # print line[11]
#   # print line[10]
#   if line[6] is 'A':
#     time=datetime.datetime.strptime(line[11],"%H:%M:%S.%f")-datetime.datetime.strptime(line[10],"%H:%M:%S.%f")
#     if time.days < 0:
#         time = timedelta(days=0,seconds=time.seconds, microseconds=time.microseconds)
#     totaltimeForAnchor+=time.total_seconds()
#     anchrorlist.append(time.total_seconds())
#   if line[6] is 'I':
#     time=datetime.datetime.strptime(line[11],"%H:%M:%S.%f")-datetime.datetime.strptime(line[10],"%H:%M:%S.%f")
#     if time.days < 0:
#         time = timedelta(days=0,seconds=time.seconds, microseconds=time.microseconds)
#     totaltimeForImage+=time.total_seconds()
#     imageTime.append(time.total_seconds())
#   if line[6] is 'S':
#     time=datetime.datetime.strptime(line[11],"%H:%M:%S.%f")-datetime.datetime.strptime(line[10],"%H:%M:%S.%f")
#     if time.days < 0:
#         time = timedelta(days=0,seconds=time.seconds, microseconds=time.microseconds)
#     totaltimeForScript+=time.total_seconds()
#     scriptlist.append(time.total_seconds())
#   if line[6] is 'L':
#     time=datetime.datetime.strptime(line[11],"%H:%M:%S.%f")-datetime.datetime.strptime(line[10],"%H:%M:%S.%f")
#     if time.days < 0:
#         time = timedelta(days=0,seconds=time.seconds, microseconds=time.microseconds)
#     totaltimeForLink+=time.total_seconds()
#     linklist.append(time.total_seconds())
#   if line[6] is 'E':
#     time=datetime.datetime.strptime(line[11],"%H:%M:%S.%f")-datetime.datetime.strptime(line[10],"%H:%M:%S.%f")
#     if time.days < 0:
#         time = timedelta(days=0,seconds=time.seconds, microseconds=time.microseconds)
#     totaltimeForEmbeded+=time.total_seconds()
#     embededlist.append(time.total_seconds())
# print "totaltimeForImage",totaltimeForImage
# print "totaltimeForScript",totaltimeForScript
# print "totaltimeForAnchor",totaltimeForAnchor
# print "totaltimeForLink",totaltimeForLink
# print "totaltimeForEmbeded",totaltimeForEmbeded

# total=totaltimeForImage+totaltimeForScript+totaltimeForAnchor+totaltimeForLink+totaltimeForEmbeded
# print "total",total

# X=sorted(imageTime)
# Y=[]
# l=len(X)
# Y.append(float(1)/l)
# for i in range(2,l+1):
#     Y.append(float(1)/l+Y[i-2])
# plt.plot(X,Y,marker='o',label='CDF for Total time taken by Image objects',color='green')
# title('CDF for total time taken by image objects'+"\n",fontsize=18,color='blue')
# xlabel('Time (in seconds)--->',color='blue',fontsize=12)
# ylabel('CDF--->',color='blue',fontsize=12)
# plt.show()


#*********************************************************************************************8

# fig = plt.figure()
# ax = fig.add_subplot(1, 1, 1)
# ax.grid(True)

# a = 0
# nhist = 100                
# b = np.max(imageTime)
# c = b-a
# d = float(c) / float(nhist)  #size of each bin
# # tmp will contain a list of bins:  [a, a+d, a+2*d, a+3*d, ... b]
# tmp = [a]
# for i in range(nhist):
#     if i == a:
#         continue
#     else:
#         tmp.append(tmp[i-1] + d)

# #  CDF of A 
# ax.hist(imageTime, bins=tmp, cumulative=True, normed=True,
#         color='red', histtype='step', linewidth=2.0,
#         label='samples A')
# plt.show()
