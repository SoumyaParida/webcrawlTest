from urlparse import urlparse
from netaddr import *
import glob
from urlparse import urlparse
import collections

# def getsecondleveldomain(url):
#     with open("effective_tld_names.dat") as tld_file:
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
#     domain=value
#     # for domain in urls :
#     if domain.startswith('www.'):
#         domain = domain.replace("www","")
#     if domain.endswith('.'):
#         domain=domain[:-1]
#     if not domain.startswith('http://'):
#         domain = 'http://%s' % domain
#     secondlevelurl=str(getsecondleveldomain(domain))
#     return (secondlevelurl)
# filenames = ['hostlist_copy//part-00000', 'hostlist_copy//part-00001']
# with open('resultsthree', 'w') as outfile:
#     for fname in filenames:
#         with open(fname) as infile:
#             for line in infile:
#                 outfile.write(line)

# 

# read_files = glob.glob("hostlistDepthBoth30//*")

# with open("resultHostboth30.txt", "wb") as outfile:
#     for f in read_files:
#         with open(f, "rb") as infile:
#             outfile.write(infile.read())

# f=open('resultHostboth30.txt')
# resultlist=list()
# for value in f:
# 	value=value.replace('(','')
# 	value=value.replace(')','')
# 	value=value.replace('\n','')
# 	value=value.replace(' ','')
# 	value=value.replace("'",'')
# 	value=value.replace("[",'')
# 	value=value.replace("]",'')
# 	values=value.split(',')
# 	resultlist.append((values[0],values[1]))

# print resultlist[:30]
# wordcount = dict()
# resultSorted=list()
# rankinglist=list()

# d=sorted(d.items(), key=lambda x:len(x[1]),reverse=True)
# for value in resultlist:
#     if value[0] not in wordcount:
#     	wordcount[value[0]]=value[1]
#     else:
# 		wordcount[value[0]].append(value[1])

# years_dict = dict()

# for line in resultlist:
#     if line[0] in years_dict:
#         # append the new number to the existing array at this slot
#         years_dict[line[0]].append(line[1])
#     else:
#         # create a new array in this slot
#         years_dict[line[0]] = [line[1]]
# #print years_dict.items()[:10]
# from collections import defaultdict
# d = defaultdict(list)

# for k,v in years_dict.items():
# 	resultSorted.append((k,len(set(v))))
# resultSorted=sorted(resultSorted, key=lambda x: x[1],reverse=True)
# #print resultSorted[:30]
# for tup in resultSorted:
# 	d[tup[1]].append(tup[0])
# result=list()
# for k,v in d.items():
# 	result.append((k,len(v)))


# count=1
# for value in resultSorted:
# 	value = list(value)
# 	value[0]=count
# 	rankinglist.append((value[0],value[1]))
# 	count=count+1

# for value in resultSorted:
# 	rankinglist.append((value[0],len(value[1])))
#print rankinglist[:10]

# count=0
# wordcount=set()
# for line in outputReader:
#     if len(line)==16:
#         if line[7] is not '-':
#         	urllist=line[7].split(';')
#         	for url in urllist:
# 	        	domain=getsecondleveldomainValue(url)
#                 if domain not in wordcount:
#                     wordcount.add(domain)
# 	        		wordcount[domain] = 1
# 	        	else:
# 	        		wordcount[domain] += 1
# 		# if len(line)==27:
# 		# 	wordcount.add(line[0])
# print "unique servers",len(wordcount)

# resultlist=sorted(resultlist, key=lambda x: x[1],reverse=True)
# rankinglist=list()
# count=1
# for value in resultlist:
# 	value = list(value)
# 	value[0]=count
# 	rankinglist.append((value[0],value[1]))
# 	count=count+1

# a=list()
# b=list()

# for value in rankinglist:
#     a.append(value[0])
#     b.append(value[1])

# valuesum=0
# for value in result:
# 	valuesum=valuesum+value[1]
# for re in result:
# 	a.append(re[0])
# 	b.append(re[1]*100/valuesum)

# print rankinglist[:10]

# import numpy
# from matplotlib import pyplot as plt
# import math

# # coefficients=numpy.polyfit(b,a,1)
# # polynomial=numpy.poly1d(coefficients)
# # ys=polynomial(b)
# # print polynomial
# plt.loglog(a,b,'ro')
# #plt.plot(b,ys)
# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of hosts on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of hosts on CDN infrastructre for Home pages")
# plt.show()

# import matplotlib.pyplot as plt
# import numpy as np
# import matplotlib.ticker as mtick

# data = b
# perc = np.linspace(0,100,len(data))

# fig = plt.figure(1, (7,4))
# ax = fig.add_subplot(1,1,1)

# ax.plot(perc, data)

# fmt = '%.0f%%' # Format you want the ticks, e.g. '40%'
# xticks = mtick.FormatStrFormatter(fmt)
# ax.xaxis.set_major_formatter(xticks)

#plt.show()

# a=list()
# b=list()

# for result in rankinglist:
# 	a.append(result[0])
# 	b.append(result[1])

# print rankinglist[:10]

# import numpy
# from matplotlib import pyplot as plt
# import math

# coefficients=numpy.polyfit(b,a,1)
# polynomial=numpy.poly1d(coefficients)
# ys=polynomial(b)
# print polynomial
# plt.loglog(a,b,'ro')
# #plt.plot(b,ys)
# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of hosts on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of hosts on CDN infrastructre for Home pages")
# plt.show()

# x=list()
# y=list()

# for result in rankinglist:
# 	x.append(result[0])
# 	y.append(result[1])
# import numpy as np

# x = numpy.asarray(x, dtype=int)
# y = numpy.asarray(y, dtype=int)

# logx = np.log(x)
# logy = np.log(y)
# coeffs = np.polyfit(logx,logy,deg=3)
# poly = np.poly1d(coeffs)
# yfit = (lambda x: np.exp(poly(np.log(x))))
# plt.loglog(x,yfit(x))
# plt.show()



# def getsecondleveldomain(url):
#     with open("effective_tld_names.dat") as tld_file:
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
#     domain=value
#     # for domain in urls :
#     if domain.startswith('www.'):
#         domain = domain.replace("www","")
#     if domain.endswith('.'):
#         domain=domain[:-1]
#     if not domain.startswith('http://'):
#         domain = 'http://%s' % domain
#     secondlevelurl=str(getsecondleveldomain(domain))
#     return (secondlevelurl)

# f=open('resultIPtoHost.txt')
# resultlist=list()
# for value in f:
# 	value=value.replace('(','')
# 	value=value.replace(')','')
# 	value=value.replace('\n','')
# 	value=value.replace(' ','')
# 	value=value.replace("'",'')
# 	values=value.split(',')
# 	resultlist.append((values[0],values[1]))

# resultlist=sorted(resultlist, key=lambda x: x[1],reverse=True)
# #print resultlist[:10]

# d = dict()
# for k, v in resultlist:
#     d.setdefault(k, set()).add(v)
# # for k,v in resultlist[:10]:
# # 	for value in v:
# # 		d[k]=getsecondleveldomainValue(v)
# kmv_output = list(d.items())
# #print "before",kmv_output[:10]
# ipToHostmaplist=list()
# for value in kmv_output:
# 	hostlist=set()
# 	for url in value[1]:
# 		hostlist.add(getsecondleveldomainValue(url))
# 	ipToHostmaplist.append((value[0],list(hostlist)))
# #print "after",ipToHostmaplist

# server=0
# server1=0
# server2=0
# server3=0
# server4=0
# server5=0
# server6=0
# server7=0
# for ipHost in ipToHostmaplist:
# 	if len(ipHost[1]) == 1 :
# 		server1=server1+1
# 	elif len(ipHost[1]) == 2 :
# 		server2=server2+1
# 	elif len(ipHost[1]) == 3 :
# 		server3=server3+1
# 	elif len(ipHost[1]) == 4 :
# 		server4=server4+1
# 	elif len(ipHost[1]) == 5 :
# 		server5=server5+1
# 	elif len(ipHost[1]) == 6 :
# 		server6=server6+1
# 	elif len(ipHost[1]) == 7 :
# 		server7=server7+1
# 	else:
# 		server=server+1

# print server
# print server1
# print server2
# print server3
# print server4
# print server5
# print server6
# print server7

# a=list()
# b=list()

# for result in rankinglist:
# 	a.append(result[0])
# 	b.append(result[1])

# print rankinglist[:10]

# import numpy
# from matplotlib import pyplot as plt
# import math

# coefficients=numpy.polyfit(b,a,1)
# polynomial=numpy.poly1d(coefficients)
# ys=polynomial(b)
# print polynomial
# plt.loglog(a,b,'ro')
# #plt.plot(b,ys)
# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of hosts on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of hosts on CDN infrastructre")
# plt.show()

# x=list()
# y=list()

# for result in rankinglist:
# 	x.append(result[0])
# 	y.append(result[1])
# import numpy as np

# x = numpy.asarray(x, dtype=int)
# y = numpy.asarray(y, dtype=int)

# logx = np.log(x)
# logy = np.log(y)
# coeffs = np.polyfit(logx,logy,deg=3)
# poly = np.poly1d(coeffs)
# yfit = (lambda x: np.exp(poly(np.log(x))))
# plt.loglog(x,yfit(x))
# plt.show()

from netaddr import *

iplist={'192.0.2.0','192.0.2.1'}
#s1 = IPSet(iplist)
print IPSet(iplist)
#print s1
# s1.add('192.0.2.0','192.0.2.1')
# s1.add('192.0.1.0')
#print s1.output
# IPSet(['192.0.2.0/32'])
# s1.remove('192.0.2.0')
# print s1
# IPSet([])
#s1.add(IPRange("10.0.0.0", "10.0.0.255"))
#print s1
# IPSet(['10.0.0.0/24'])
# s1.remove(IPRange("10.0.0.128", "10.10.10.10"))
# print s1
# IPSet(['10.0.0.0/25'])
# rankinglist=list()
# count=1
# for value in resultlist:
# 	value = list(value)
# 	value[0]=count
# 	rankinglist.append((value[0],value[1]))
# 	count=count+1

# a=list()
# b=list()

# for result in rankinglist:
# 	a.append(result[0])
# 	b.append(result[1])

# print rankinglist[:10]

import numpy
from matplotlib import pyplot as plt
import math

# coefficients=numpy.polyfit(b,a,1)
# polynomial=numpy.poly1d(coefficients)
# ys=polynomial(b)
# print polynomial
# plt.loglog(a,b,'ro')
# #plt.plot(b,ys)
# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of hosts on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of hosts on CDN infrastructre")
# plt.show()

# content_sizes_count_list=[46125,2716,678,303,182,151,109]
# values=[1,2,3,4,5,6,7]
# X=sorted(content_sizes_count_list)
# Y=[]
# l=len(X)
# Y.append(float(1)/l)
# fig = plt.figure()
# ax = fig.add_subplot(111)
# for i in range(2,l+1):
#     Y.append(float(1)/l+Y[i-2])
# plt.plot(X,Y,marker='o',label='xyz')
# ax.set_ylabel("\n"+'Probability -- >',fontsize=10)
# ax.set_xlabel("\n"+'content size in bytes -->',fontsize=10)
# ax.set_title('Emperical CDF Plot for content size of home pages of' + "\n" +"\n"+ '        Alexa top 100,000 websites'+"\n",fontsize=14)  
# plt.show()
#fig.savefig(os.path.join(folder_name, 'content_size.png'))

# import matplotlib.pyplot as plt

# def plot_energy(xvalues, yvalues):
#     fig = plt.figure()
#     ax = fig.add_subplot(1,1,1)

#     #ax.scatter(xvalues, yvalues)
#     ax.plot(xvalues, yvalues)

#     #ax.set_xscale('log')

#     ax.set_xticks(xvalues)

#     ax.set_xlabel('RUU size')
#     ax.set_title("Energy consumption")
#     ax.set_ylabel('Energy per instruction (nJ)')
#     plt.show()
# xvalues = list()
# xvalues=a
# #webserverlist=[46125,2716,678,303,182,151,109]
# yvalues=list()
# yvalues=b
# # for value in webserverlist:
# # 	yvalues.append(value*100/sum(webserverlist))
# #print yvalues
# plot_energy(xvalues,yvalues)