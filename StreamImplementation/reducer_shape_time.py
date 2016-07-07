#!/usr/bin/python
import re
import sys

current = None
min = 0
max = 0
mean = 0
total = 0
count = 0

for line in sys.stdin:
    #print line
    line = line.strip().split('\t')
    shape = line[0].strip()
    time = int(line[1].strip())
    if current == None :
       current = shape 
       min= time
       max = time
       total += time
       count +=1 
    elif current == shape :
       count += 1 
       total += time 
       if time < min:
           min = time 
       elif max < time :
           max = time 
    else:
       #print shape ,'\t',min,'\t',max,'\t',total/float(count)
       print current ,'\t',min,'\t',max,'\t',total/float(count),'\t', total, '\t',count
       current = shape 
       min = time 
       max = time 
       total = time 
       count = 1 
    
if current != None :
     print current ,'\t',min,'\t',max,'\t',total/float(count),'\t', total, '\t',count
   
