#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import exc
import sys, time
import pickle
from datetime import datetime
from sets import Set
import json
reload(sys)
sys.setdefaultencoding('utf-8')

print 'sql engine start'
config = {}        
try:
    with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
        config = json.load(f)                
except Exception as e:
    logging.exception(e)
engine = create_engine(config['mysql'])
print 'sql engine end'

selectTag = """
select distinct(tag1) as tag from deal_history where tag1 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag2) as tag from deal_history where tag2 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag3) as tag from deal_history where tag3 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag4) as tag from deal_history where tag4 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag5) as tag from deal_history where tag5 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag6) as tag from deal_history where tag6 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag7) as tag from deal_history where tag7 is not null and site in ('ppomppu.co.kr', 'clien.net') union all
select distinct(tag8) as tag from deal_history where tag8 is not null and site in ('ppomppu.co.kr', 'clien.net') 
"""

userDict = {}
userDict[1] = """ update deal_history set tag1 = %s where tag1 is null and id = %s """
userDict[2] = """ update deal_history set tag2 = %s where tag2 is null and id = %s """
userDict[3] = """ update deal_history set tag3 = %s where tag3 is null and id = %s """
userDict[4] = """ update deal_history set tag4 = %s where tag4 is null and id = %s """
userDict[5] = """ update deal_history set tag5 = %s where tag5 is null and id = %s """
userDict[6] = """ update deal_history set tag6 = %s where tag6 is null and id = %s """
userDict[7] = """ update deal_history set tag7 = %s where tag7 is null and id = %s """
userDict[8] = """ update deal_history set tag8 = %s where tag8 is null and id = %s """

processed = 0
con = None
try:
    # insert into db
    
    count = 0
    print('%s - connection engine start' % str(datetime.now()))
    con = engine.connect()
    print('%s - connection engine end' % str(datetime.now()))
    result1 = con.execute(selectTag)
    print('%s - tag keyword selection execution end' % str(datetime.now()))
    tagKeyword = Set()
    print('%s - tag keyword store into Set start' % str(datetime.now()))
    for row1 in result1:
    	tagKeyword.add(row1[0])
    print('%s - tag keyword store into Set end' % str(datetime.now()))
    
    with open('tag_dictionary_kor.pkl', 'wb') as output:
    	pickle.dump(tagKeyword, output, pickle.HIGHEST_PROTOCOL)

    with open('tag_dictionary_kor.pkl', 'rb') as input:
    	tagDictLoaded = pickle.load(input)
    	print('size of set:{}'.format(len(tagDictLoaded)))

except Exception as e:
    print(e)
finally:
    if con != None:
        con.close()
