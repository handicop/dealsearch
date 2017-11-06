#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import exc
import sys, time
import pickle
from datetime import datetime
from sets import Set
import logging
import json
reload(sys)
sys.setdefaultencoding('utf-8')

class Tagging:

    selectAll = """ select lower(content), id, lower(tag1), lower(tag2), lower(tag3), lower(tag4), lower(tag5), lower(tag6), lower(tag7), lower(tag8), site from deal_history where date(reg_dtime) >= curdate() -3 order by 1 desc"""
    userDict = {}
    engine = None

    def __init__(self):
        logging.info('tagging object initialized')
        config = {}
        
        try:
            with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
                config = json.load(f)                
        except Exception as e:
            logging.exception(e)

        self.userDict[1] = """ update deal_history set tag1 = %s where tag1 is null and id = %s """
        self.userDict[2] = """ update deal_history set tag2 = %s where tag2 is null and id = %s """
        self.userDict[3] = """ update deal_history set tag3 = %s where tag3 is null and id = %s """
        self.userDict[4] = """ update deal_history set tag4 = %s where tag4 is null and id = %s """
        self.userDict[5] = """ update deal_history set tag5 = %s where tag5 is null and id = %s """
        self.userDict[6] = """ update deal_history set tag6 = %s where tag6 is null and id = %s """
        self.userDict[7] = """ update deal_history set tag7 = %s where tag7 is null and id = %s """
        self.userDict[8] = """ update deal_history set tag8 = %s where tag8 is null and id = %s """
        self.engine = create_engine(config['mysql'])

    def run(self):
        try:
            con = None
            tagKeywordEng = None
            tagKeywordKor = None
            processed = 0
            try:

                with open('/home/pi/Documents/DealProject/tag_dictionary.pkl', 'rb') as input:
                    tagKeywordEng = pickle.load(input)
                    logging.info('size of english dictionary set:{}'.format(len(tagKeywordEng)))
                    # print'size of english dictionary set:{}'.format(len(tagKeywordEng))

                with open('/home/pi/Documents/DealProject/tag_dictionary_kor.pkl', 'rb') as input:
                    tagKeywordKor = pickle.load(input)
                    logging.info('size of korean dictionary set:{}'.format(len(tagKeywordKor)))
                    # print'size of korean dictionary set:{}'.format(len(tagKeywordKor))

                # insert into db
                
                count = 0
                logging.info('%s - connection engine start' % str(datetime.now()))
                # print('connection engine start - '+str(datetime.now()))
                con = self.engine.connect()
                logging.info('%s - connection engine end' % str(datetime.now()))
                # print('connection engine end - '+str(datetime.now()))
                result = con.execute(self.selectAll)
                logging.info('%s - start tagging' % str(datetime.now()))
                # print('start tagging-'+str(datetime.now()))
                # print('result count {}'.format(result.rowcount))
                tagKeywordKor = tagKeywordKor.union(tagKeywordEng)
                # printlen(tagKeywordEng)

                # content record select
                for r in result:
                    if r[10] == 'ppomppu.co.kr' or r[10] == 'clien.net':
                        # each tag loo
                        for s in tagKeywordKor:
                            # if s tag is in content and not in any of tag column
                            tempS = s.lower()
                            tempS1 = tempS
                            if tempS == 'ebook':
                                tempS1 = ' ' + tempS
                            elif tempS == 'lg':
                                tempS1 = tempS + ' '
                            elif tempS == 'hp':
                                tempS1 = tempS + ' '
                            elif tempS == 'book':
                                tempS1 = ' ' + tempS + ' '
                            elif tempS == '4k':
                                tempS1 = ' ' + tempS + ' '
                            elif tempS == 'ssd':
                            	tempS1 = ' ' + tempS + ' '
                            elif tempS == 'aa':
                            	tempS1 = ' ' + tempS + ' '
                            elif tempS == 'aaa':
                            	tempS1 = ' ' + tempS + ' '
                            elif tempS == 'tv':
                                tempS1 = ' ' + tempS
                            elif tempS == '마이크':
                                tempS1 = tempS + ' '

                            if tempS1 in r[0] and tempS != r[2] and tempS != r[3] and tempS != r[4] and tempS != r[5] and tempS != r[6] and tempS != r[7] and tempS != r[8] and tempS != r[9]:
                                # print'temps1 [{}] content[{}] - {},{},{},{},{},{},{},{}'.format(tempS1, r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9] )
                                for index in range(2,9):
                                    if r[index] == None:
                                        logging.info('id {} - tag {}'.format(r[1], s))
                                        # print('id {} - tag {}'.format(r[1], s))
                                        rs = con.execute(self.userDict.get(index-1), s, r[1])
                                        processed += rs.rowcount
                                        if processed % 100 == 0:
                                            logging.info('current tagging count: {}'.format(processed))
                                            # print('current tagging count: {}'.format(processed))
                                        break
                                    else:
                                        continue
                            else:
                                continue
                    else:

                        # each tag loo
                        for s in tagKeywordEng:
                            # if s tag is in content and not in any of tag column
                            tempS = s.lower()
                            tempS1 = tempS
                            if tempS == 'ebook':
                                tempS1 = ' ' + tempS
                            elif tempS == 'lg':
                                tempS1 = tempS + ' '
                            elif tempS == 'hp':
                                tempS1 = tempS + ' '
                            elif tempS == 'ssd':
                            	tempS1 = ' ' + tempS + ' '
                            elif tempS == 'aa':
                            	tempS1 = ' ' + tempS + ' '
                            elif tempS == '4k':
                                tempS1 = ' ' + tempS + ' '
                            elif tempS == 'aaa':
                            	tempS1 = ' ' + tempS + ' '
                            elif tempS == 'book':
                                tempS1 = ' ' + tempS + ' '
                            elif tempS == 'tv':
                                tempS1 = ' ' + tempS
                            # print'temps1 [{}] content[{}] - {},{},{},{},{},{},{},{}'.format(tempS1, r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9] )
                            if tempS1 in r[0] and tempS != r[2] and tempS != r[3] and tempS != r[4] and tempS != r[5] and tempS != r[6] and tempS != r[7] and tempS != r[8] and tempS != r[9]:
                                # print'temps1 [{}] content[{}] - {},{},{},{},{},{},{},{}'.format(tempS1, r[0], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9] )
                                for index in range(2,9):
                                    if r[index] == None:
                                        logging.info('id {} - tag {}'.format(r[1], s))
                                        # print('id {} - tag {}'.format(r[1], s))
                                        rs = con.execute(self.userDict.get(index-1), s, r[1])
                                        processed += rs.rowcount
                                        if processed % 100 == 0:
                                            logging.info('current tagging count: {}'.format(processed))
                                            # print('current tagging count: {}'.format(processed))
                                        break
                                    else:
                                        continue
                            else:
                                continue
            except Exception as e:
                logging.exception(e)
                # printe
                processed = -1
            finally:
                if con != None:
                    con.close()

            logging.info('%s - finish tagging' % str(datetime.now()))
            logging.info('tagging update count: {}'.format(processed))
            # print('tagging update count: {}'.format(processed))
            return processed
        except Exception as e2:
            logging.exception(e2)
            # printe2
            return -1