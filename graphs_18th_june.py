from urlparse import urlparse
from netaddr import *
import glob
from urlparse import urlparse
import collections
from collections import Counter

def rankingLists(f):
	resultlist=list()
	for value in f:
		value=value.replace('(','')
		value=value.replace(')','')
		value=value.replace('\n','')
		value=value.replace(' ','')
		value=value.replace("'",'')
		value=value.replace("[",'')
		value=value.replace("]",'')
		values=value.split(',')
		if values[0]!='':
			resultlist.append((values[0],int(values[1])))

	resultlist=sorted(resultlist, key=lambda x: x[1],reverse=True)
	print "resultlist",resultlist[:20]
	rankinglist=list()
	count=1

	for value in resultlist:
		value = list(value)
		value[0]=count
		rankinglist.append((value[0],value[1]))
		count=count+1

	print "unique hosts",len(resultlist)
	print "rankinglist",rankinglist[:20]
	a=list()
	b=list()

	for result in rankinglist:
		a.append(result[0])
		b.append(result[1])
	return (a,b)

#**************************************************************
# read_files = glob.glob("hostRankDepth18thJuneFull/*")
# with open("resultHost18thJune.txt", "wb") as outfile:
#     for f in read_files:
#         with open(f, "rb") as infile:
#             outfile.write(infile.read())

# f=open('resultHost18thJune.txt')
# (a,b)=rankingLists(f)

# read_files = glob.glob("hostRankDepth18thJuneDepth1/*")
# with open("resultHost18thJuneDepth1.txt", "wb") as outfile:
#     for f in read_files:
#         with open(f, "rb") as infile:
#             outfile.write(infile.read())

# f=open('resultHost18thJuneDepth1.txt')
# (a,b)=rankingLists(f)

# read_files1 = glob.glob("hostRankDepth18thJuneDepth0/*")
# with open("resultHost18thJuneDepth0.txt", "wb") as outfile1:
#     for f1 in read_files1:
#         with open(f1, "rb") as infile1:
#             outfile1.write(infile1.read())

# f1=open('resultHost18thJuneDepth0.txt')
# (a1,b1)=rankingLists(f1)

#**************************************************************
#CDF graph(zipf power law)
#**************************************************************
# import numpy as np
# import matplotlib.pyplot as plt
# ar = sorted(b,reverse=True)
# print "total number of times hosts used",sum(ar)
# print "first two hosts values:",ar[:20]
# for value in ar[:20]:
#     print (float(value)*100/sum(ar))
# y = np.cumsum(ar).astype("float32")
# y/=y.max()
# y*=100.
# #prepend a 0 to y as zero stores have zero items
# y = np.hstack((0,y))
# #get cumulative percentage of stores
# x = np.linspace(0,100,y.size)
# plt.plot(x,y)
# plt.grid(True)
# plt.xlabel("Percentage of CDN infrastructures (in rank order)")
# plt.ylabel("Cumulative repetitive use of CDN infrastructures ")
# plt.show()

#**************************************************************
#CDF graph(zipf power law) Numercial way
#**************************************************************

# import numpy
# from matplotlib import pyplot as plt
# import math
# from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes
# from mpl_toolkits.axes_grid1.inset_locator import mark_inset

# fig, ax = plt.subplots(figsize=[5,4])
# coefficients=numpy.polyfit(b,a,1)
# plt.loglog(a,b,'ro')

# plt.loglog(a1,b1,'bo',label=r"websites links from alexa")
# plt.loglog(a,b,'go',label=r"Embeded links")

# plt.legend(loc="upper right")

# plt.xlabel("Log (Rank of CDN infrastructures)")
# plt.ylabel("Log (Number of hosts on CDN infrastructre)")
# plt.title("Rank of CDN infrastructure vs Number of hosts on CDN infrastructre")

# axins = zoomed_inset_axes(ax, 1.5, loc=1) # zoom-factor: 2.5, location: upper-left
# axins.loglog(a,b,'ro')

# x1, x2, y1, y2 = 30000, 260000, 0, 100 # specify the limits
# axins.set_xlim(x1, x2) # apply the x-limits
# axins.set_ylim(y1, y2) # apply the y-limits

# plt.yticks(visible=False)
# plt.xticks(visible=False)

#mark_inset(ax, axins, loc1=2, loc2=4, fc="none", ec="0.5")
# plt.draw()
# plt.show()
