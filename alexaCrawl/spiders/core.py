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
print "listOfLists",listOfLists
