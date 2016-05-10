from urlparse import urlparse
from netaddr import *
import glob
from urlparse import urlparse
import collections
import numpy
from matplotlib import pyplot as plt
import math
from netaddr import *
from operator import itemgetter
import csv
import codecs


#***********************************************************************
#rank vs cdn infr
#***********************************************************************
# read_files = glob.glob("hostlistDepthBoth30//*")
# read_files_depth0 = glob.glob("hostlistDepth030//*")
# read_files_depth1 = glob.glob("hostlistDepth130//*")

# with open("resultHostboth30.txt", "wb") as outfile:
#     for f in read_files:
#         with open(f, "rb") as infile:
#             outfile.write(infile.read())

# with open("resultHost030.txt", "wb") as outfile0:
#     for f0 in read_files_depth0:
#         with open(f0, "rb") as infile0:
#             outfile0.write(infile0.read())

# with open("resultHost130.txt", "wb") as outfile1:
#     for f1 in read_files_depth1:
#         with open(f1, "rb") as infile1:
#             outfile1.write(infile1.read())

f=open('resultHostboth30.txt')
resultlist=list()
resultlist0=list()
resultlist1=list()
result=dict()
resultlistfinal=list()
for value in f:
	value=value.replace('(','')
	value=value.replace(')','')
	value=value.replace('\n','')
	value=value.replace(' ','')
	value=value.replace("'",'')
	value=value.replace("[",'')
	value=value.replace("]",'')
	values=value.split(',')
        resultlist.append((values[0],int(values[1])))

resultlist=sorted(resultlist, key=lambda x: x[1],reverse=True)
print resultlist[:10]

# resultlistwith1=list()
# for value in resultlist:
#   if value[1] ==1:
#     resultlistwith1.append(value)

# print len(resultlistwith1)
# print float(len(resultlistwith1))/len(resultlist)

# print resultlistwith1[:5]

# print resultlist[:20]
# sumtotal=0
# for result in resultlist:
#   sumtotal=sumtotal+result[1]
# print sumtotal

# sumfirst200=0
# for value in resultlist[:200]:
#   sumfirst200=sumfirst200+value[1]
# percen=(float(sumfirst200)*100)/sumtotal
# print percen

# hostlist=list()
# percenlist=list()
# for result in resultlist[:20]:
#   hostlist.append(result[0])
#   percenlist.append(float(result[1]*100)/sumtotal)
#   #print result[1]
#   #print float(result[1]*100)/sumtotal
# import matplotlib.pyplot as plt
# plt.plot(['a','b','c','d'], [1,4,9,16], 'ro')
# #plt.axis([0, 6, 0, 20])
# plt.show()

# sumfirst20=0
# for value in resultlist[:20]:
#   sumfirst20=sumfirst20+value[1]
# percen=(float(sumfirst20)*100)/sumtotal
# print sumfirst20
# print percen

# for value in resultlist[:100]:
#   print value

# f0=open('resultHost030.txt')
# resultlist0=list()
# for value in f0:
# 	value=value.replace('(','')
# 	value=value.replace(')','')
# 	value=value.replace('\n','')
# 	value=value.replace(' ','')
# 	value=value.replace("'",'')
# 	value=value.replace("[",'')
# 	value=value.replace("]",'')
# 	values=value.split(',')
# 	resultlist0.append((values[0],int(values[1])))

# f1=open('resultHost130.txt')
# resultlist1=list()
# for value in f1:
# 	value=value.replace('(','')
# 	value=value.replace(')','')
# 	value=value.replace('\n','')
# 	value=value.replace(' ','')
# 	value=value.replace("'",'')
# 	value=value.replace("[",'')
# 	value=value.replace("]",'')
# 	values=value.split(',')
# 	resultlist1.append((values[0],int(values[1])))

# # # resultlist=sorted(resultlist, key=lambda x: x[1],reverse=True)
# # # print resultlist[:20]
# resultlist0=sorted(resultlist0, key=lambda x: x[1],reverse=True)
# resultlist1=sorted(resultlist1, key=lambda x: x[1],reverse=True)

# print len(resultlist0)

# resultlistwith10=list()
# for value in resultlist0:
#   if value[1] ==1:
#     resultlistwith10.append(value)

# print len(resultlistwith10)
# print float(len(resultlistwith10))/len(resultlist0)

# print len(resultlist1)

# resultlistwith11=list()
# for value in resultlist1:
#   if value[1] ==1:
#     resultlistwith11.append(value)

# print len(resultlistwith11)
# print float(len(resultlistwith11))/len(resultlist1)

# sumtotal=0
# for result in resultlist0:
#   sumtotal=sumtotal+result[1]
# print sumtotal

# sumfirst20=0
# for value in resultlist0[:20]:
#   sumfirst20=sumfirst20+value[1]
# percen=(float(sumfirst20)*100)/sumtotal
# print sumfirst20
# print percen

# sumtotal1=0
# for result in resultlist1:
#   sumtotal1=sumtotal1+result[1]
# print sumtotal1

# sumfirst201=0
# for value in resultlist1[:20]:
#   sumfirst201=sumfirst201+value[1]
# percen1=(float(sumfirst201)*100)/sumtotal1
# print sumfirst201
# print percen1


# # print resultlist[:20]
# rankinglist=list()
# rankinglist0=list()
# rankinglist1=list()
# count=1
# for value in resultlist:
# 	value = list(value)
# 	value[0]=count
# 	rankinglist.append((value[0],value[1]))
# 	count=count+1


# count0=1
# for value in resultlist0:
# 	value = list(value)
# 	value[0]=count0
# 	rankinglist0.append((value[0],value[1]))
# 	count0=count0+1

# count1=1
# for value in resultlist1:
# 	value = list(value)
# 	value[0]=count1
# 	rankinglist1.append((value[0],value[1]))
# 	count1=count1+1

# a=list()
# b=list()

# a0=list()
# b0=list()

# a1=list()
# b1=list()

# for value in rankinglist:
#     a.append(value[0])
#     b.append(value[1])

# for value in rankinglist0:
#     a0.append(value[0])
#     b0.append(value[1])

# for value in rankinglist1:
#     a1.append(value[0])
#     b1.append(value[1])

#plt.loglog(a,b,'ro')
# plt.loglog(a0,b0,'bo',label=r"Top links")
# plt.loglog(a1,b1,'go',label=r"Embeded links")

# #plt.legend(loc="upper right")
# plt.legend(loc="upper right")
# plt.legend(loc="upper right")

# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of website links on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of website links on CDN infrastructre")
# plt.show()

#**************************************************************************************

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

# read_files = glob.glob("IPHOSTMAPWithDepthBoth30New//*")

# with open("resultHostipboth30.txt", "wb") as outfile:
#     for f in read_files:
#         with open(f, "rb") as infile:
#             outfile.write(infile.read())

# f=open('resultHostipboth30.txt')
# resultlist=set()
# resultlist0=list()
# resultlist1=list()
# subnetlist=list()
# outfilesubnet=open("resultHostboth30Subnet.txt", "wb")
# for value in f:
# 	value=value.replace('(','')
# 	value=value.replace(')','')
# 	value=value.replace('\n','')
# 	value=value.replace(' ','')
# 	value=value.replace("'",'')
# 	value=value.replace("[",'')
# 	value=value.replace("]",'')
# 	values=value.split(',')
#         subnetlist=set()
#         for ip in values[:-1]:
#             ipvalues=ip.split('.')
#             ipNet=ipvalues[:-1]
#             newipnet=ipNet[0]+'.'+ipNet[1]+'.'+ipNet[2]+'.0'+'/24'
#             subnetlist.add(newipnet)
#         host=getsecondleveldomainValue(values[-1])
#         for item in subnetlist:
#             resultlist.add((host,item))
#             outfilesubnet.write(str((item,host)))
#             outfilesubnet.write('\n')
# outfilesubnet=open("resultHostboth30Subne.txt", "r")
# resultlist=list()
# for value in outfilesubnet:
#     value=value.replace('(','')
#     value=value.replace(')','')
#     value=value.replace('\n','')
#     value=value.replace(' ','')
#     value=value.replace("'",'')
#     value=value.replace("[",'')
#     value=value.replace("]",'')
#     print value
    #values=value.split(',')
    #resultlist.append((values[1],values[0]))
#print resultlist[:5]
# print len(resultlist)
# resultlist=set(resultlist)
# wordcount=dict()
# for value in resultlist:
#     if value[0] not in wordcount:
#         wordcount[value[0]]=1
#     else:
#         wordcount[value[0]]+=1
# sortedlist=sorted(wordcount.items(), key=lambda item: item[1],reverse=True)

# rankinglist=list()
# a=list()
# b=list()
# count=0
# for value in sortedlist:
#     value = list(value)
#     value[0]=count
#     rankinglist.append((value[0],value[1]))
#     count=count+1
# print sortedlist[:10]
# for value in rankinglist:
#     a.append(value[0])
#     b.append(value[1])
# plt.loglog(a,b,'go')

# plt.legend(loc="upper right")

# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of IPAddress subnet on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of IPAddress subnet CDN infrastructre")
# plt.show()

#****************************************************************
# from collections import defaultdict
# f=open('resultHostboth30IPSubnet.txt')
# def merge_subs(lst_of_lsts):
#   years_dict = defaultdict(list)
#   resultlist = list()
#   for line in lst_of_lsts:
#     host=line[0]
#     del line[0]
#     if host in years_dict:
#         for item in line:
#           years_dict[host].append(item)
#     else:
#       years_dict[host] = line
#   years_dict=dict(years_dict)
#   resultlist=sorted(years_dict.items(), key=lambda x: len(set(x[1])),reverse=True)
#   return resultlist

# resultlist=list()
# for value in f:
#   newvalues=list()
#   value=value.replace('(','')
#   value=value.replace(')','')
#   value=value.replace('\n','')
#   value=value.replace(' ','')
#   value=value.replace("'",'')
#   value=value.replace("[",'')
#   value=value.replace("]",'')
#   values=value.split(',')
#   for item in values:
#     if not item.startswith("set"):
#       newvalues.append(item)
#   if len(newvalues) >0:
#     resultlist.append(newvalues)
# print resultlist[:5]
# newresultlist=merge_subs(resultlist)
# print "1st phrease done"
# finallist=list()
# for result in newresultlist:
#   hostname=result[0]
#   subnetlist=set()
#   numberofIP=len(set(result[1]))
#   for ip in set(result[1]):
#     ipvalues=ip.split('.')
#     ipNet=ipvalues[:-1]
#     newipnet=ipNet[0]+'.'+ipNet[1]+'.'+ipNet[2]+'.0'+'/24'
#     subnetlist.add(newipnet)
#   finallist.append((hostname,numberofIP,len(subnetlist)))
# print "2nd phrease done"
# finallist=sorted(finallist, key=lambda x: x[1],reverse=True)
# print "finallist",finallist[:15]
# outfilesubnet=open("resultASNNumber.txt")
# asnlist=list()
# for value in outfilesubnet:
#   value=value.replace('(','')
#   value=value.replace(')','')
#   value=value.replace('\n','')
#   value=value.replace(' ','')
#   value=value.replace("'",'')
#   value=value.replace("[",'')
#   value=value.replace("]",'')
#   values=value.split(',')
#   asnlist.append((values[0],values[1]))
# print "3rd phrease done"
# print "asnlist",asnlist[:15]
# resultFile=codecs.open("kmeaninputfinal.csv", 'wbr+')
# wr = csv.writer(resultFile, skipinitialspace=True,delimiter=',',quotechar=' ', quoting=csv.QUOTE_MINIMAL)
# finallistnew=set()
# for item in finallist:
#   for value in asnlist:
#     if item[0] is not None and item[0]==value[0]:
#       finallistnew.add((item[0],item[1],item[2],int(value[1])))
# #finallistnew=[x + y[1:] for x, y in zip(finallist, asnlist) if x[0] == y[0]]
# print "4th phrease done"
# for item in finallistnew:
#   wr.writerow(item)
# years_dict = dict()
# print iplist[:2]
# for line in iplist:
#     if line[0] in years_dict:
#         # append the new number to the existing array at this slot
#         years_dict[line[0]].append(line[1])
#     else:
#         # create a new array in this slot
#         years_dict[line[0]] = [line[1]]

# wordcount=dict()
# for k,v in years_dict.items():
#     if k not in wordcount:
#         wordcount[k]=len(set(v))
#     else:
#         wordcount[k]+=len(set(v))

# sortedNumberOfIPlist=sorted(wordcount.items(), key=lambda x: x[1],reverse=True)
# print sortedNumberOfIPlist[:5]
# outfilesubnet=open("resultHostboth30Subne.txt", "r")
# resultlist=list()
# for value in outfilesubnet:
#     value=value.replace('(','')
#     value=value.replace(')','')
#     value=value.replace('\n','')
#     value=value.replace(' ','')
#     value=value.replace("'",'')
#     value=value.replace("[",'')
#     value=value.replace("]",'')
#     values=value.split(',')
#     resultlist.append((values[1],values[0]))

# resultlist=sorted(resultlist, key=lambda x: x[1],reverse=True)

#print years_dict.items()[:5]
# rankinglist=list()
# count=1
# for value in resultlist:
#   value = list(value)
#   value[0]=count
#   rankinglist.append((value[0],value[1]))
#   count=count+1


# dst1 = dict(lst1)
# dst2 = dict(lst2)

# for i in dst1:
#     if i in dst2:
#         dst1[i] = (dst1[i],dst2[i])

# print dst1
# {'a': (1, 4), 'c': (3, 6), 'b': (2, 5)}
# from shutil import copyfile
# copyfile('kmeaninputfinal2.csv','kmeaninputfinal3.csv')
# logwr = open("kmeaninputfinal3.csv")
# reader = csv.reader(logwr)
# newoutput=open("kmeaninputfinal4.csv", "w")
# writer = csv.writer(newoutput)
# for line in reader:
#     writer.writerow((line[0],result.get(line[0]),line[1],line[2],line[3]))
# #   writer.writerow((newhost,line[1],line[2],line[3]))
# newoutput.close()

# logwr = open("kmeaninputfinal2.csv")
# reader = csv.reader(logwr)
# for line in reader:
#   print line

