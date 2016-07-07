#!/usr/bin/python
import re
import sys

pattern = re.compile('(\d+)\s*(min|sec|hour)', re.I)

#line = 'abcd   123 SECODS'

for line in sys.stdin:
    if len(line.strip()) > 0 :
        line = line.split('\t')
        if len(line[3]) > 0 and len(line[4]) > 0:
            matched = pattern.search(line[4])
            if matched != None:
              value = int(matched.group(1))
              unit = matched.group(2) 
              if re.match(unit, 'min', re.I)!=  None :
                  value = value * 60 
              elif re.match(unit, 'hour', re.I)!=  None :
                  value = value * 3600 
              print line[3], '\t' ,value
