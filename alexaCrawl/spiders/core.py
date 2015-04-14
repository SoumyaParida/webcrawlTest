import csv

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
